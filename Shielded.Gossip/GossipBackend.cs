using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public class GossipBackend : IBackend, IDisposable
    {
        private readonly ShieldedDictNc<string, MessageItem> _local = new ShieldedDictNc<string, MessageItem>();
        private readonly ShieldedDictNc<string, int> _gossipBlock = new ShieldedDictNc<string, int>();
        private readonly ShieldedTreeNc<long, string> _freshIndex = new ShieldedTreeNc<long, string>();
        private readonly Shielded<ulong> _databaseHash = new Shielded<ulong>();
        private readonly ShieldedLocal<bool> _isExternalConsistent = new ShieldedLocal<bool>();
        private readonly ShieldedLocal<ulong> _localOldHash = new ShieldedLocal<ulong>();
        private readonly ShieldedLocal<ulong> _localNewHash = new ShieldedLocal<ulong>();

        private readonly Timer _gossipTimer;
        private readonly IDisposable _preCommit;

        public readonly ITransport Transport;
        public readonly GossipConfiguration Configuration;

        public GossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            Transport = transport;
            Configuration = configuration;
            Transport.MessageReceived += Transport_MessageReceived;

            _gossipTimer = new Timer(_ => SpreadRumors(), null, Configuration.GossipInterval, Configuration.GossipInterval);

            _preCommit = Shield.PreCommit(() => _local.TryGetValue("any", out MessageItem _) || true, () =>
            {
                if (IsConsistent)
                    BlockGossip();
                else
                    SyncIndexes();
            });
        }

        private bool IsConsistent => Distributed.IsConsistent || _isExternalConsistent.GetValueOrDefault();

        private void SyncIndexes()
        {
            long newFresh = GetNextFreshness();
            foreach (var key in _local.Changes)
            {
                var oldItem = Shield.ReadOldState(() => _local.TryGetValue(key, out MessageItem o) ? o : null);
                if (oldItem != null)
                {
                    _freshIndex.Remove(oldItem.Freshness, key);
                }
                var newItem = _local[key];
                newItem.Freshness = newFresh;
                _freshIndex.Add(newFresh, key);
            }
        }

        private void BlockGossip()
        {
            var changes = _local.Changes.ToArray();
            if (!changes.Any())
                return;
            var oldHash = _localOldHash.GetValueOrDefault();
            var newHash = _localNewHash.GetValueOrDefault();

            void unblock(ulong hash) => Shield.InTransaction(() =>
            {
                _databaseHash.Commute((ref ulong h) => h ^= hash);
                foreach (var key in changes)
                {
                    UnblockGossip(key);
                    if (_local.TryGetValue(key, out var local))
                        _local[key] = local;
                }
            });
            Shield.SideEffect(() => unblock(newHash), () => unblock(oldHash));

            var tcs = new TaskCompletionSource<object>();
            ThreadPool.QueueUserWorkItem(_ =>
            {
                try
                {
                    Shield.InTransaction(() =>
                    {
                        _databaseHash.Commute((ref ulong h) => h ^= oldHash);
                        foreach (var key in changes)
                            BlockGossip(key);
                    });
                    tcs.SetResult(null);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });
            tcs.Task.Wait();
        }

        private void BlockGossip(string key)
        {
            _gossipBlock.TryGetValue(key, out var i);
            _gossipBlock[key] = ++i;
        }

        private void UnblockGossip(string key)
        {
            if (!_gossipBlock.TryGetValue(key, out var i))
                return;
            if (--i <= 0)
                _gossipBlock.Remove(key);
            else
                _gossipBlock[key] = i;
        }

        private bool IsGossipBlocked(string key)
        {
            return _gossipBlock.ContainsKey(key);
        }

        private long GetNextFreshness()
        {
            return checked(GetMaxFreshness() + 1);
        }

        private long GetMaxFreshness()
        {
            return _freshIndex.Descending.FirstOrDefault().Key;
        }

        private ShieldedDictNc<string, DateTimeOffset> _lastSendTime = new ShieldedDictNc<string, DateTimeOffset>(StringComparer.InvariantCultureIgnoreCase);

        private void Send(string server, object msg, bool clearState = false)
        {
            // to keep the reply Shielded transaction read-only, so that it never conflicts and
            // gets repeated, we run the change in _lastSendTime as a side-effect too, just before
            // actually sending.
            Shield.SideEffect(() =>
            {
                Shield.InTransaction(() =>
                {
                    if (clearState)
                        _lastSendTime.Remove(server);
                    else
                        _lastSendTime[server] = DateTimeOffset.UtcNow;
                });
                Transport.Send(server, msg);
            });
        }

        private void SpreadRumors()
        {
            try
            {
                Shield.InTransaction(() =>
                {
                    var servers = Transport.Servers;
                    if (servers == null || !servers.Any())
                        return;
                    var limit = Configuration.AntiEntropyHuntingLimit;
                    var rand = new Random();
                    string server;
                    do
                    {
                        server = servers.Skip(rand.Next(servers.Count)).First();
                    }
                    while (
                        _lastSendTime.TryGetValue(server, out var lastTime) &&
                        (DateTimeOffset.UtcNow - lastTime).TotalMilliseconds < Configuration.AntiEntropyIdleTimeout &&
                        --limit >= 0);
                    if (limit < 0)
                        return;

                    // here, we want a conflict.
                    _lastSendTime[server] = DateTimeOffset.UtcNow;
                    Shield.SideEffect(() => Transport.Send(server, new GossipStart
                    {
                        From = Transport.OwnId,
                        DatabaseHash = _databaseHash
                    }));
                });
            }
            catch { } // TODO
        }

        private void Transport_MessageReceived(object sender, object msg)
        {
            switch (msg)
            {
                case DirectMail trans:
                    Shield.InTransaction(() => ApplyItems(trans.Items));
                    break;

                case GossipStart start:
                    Shield.InTransaction(() =>
                        SendReply(start.From, start.DatabaseHash, start.Time));
                    break;

                case GossipReply reply:
                    Shield.InTransaction(() => ApplyItems(reply.Items));
                    Shield.InTransaction(() =>
                        SendReply(reply.From, reply.DatabaseHash, reply.Time, reply));
                    break;

                case GossipEnd end:
                    Shield.InTransaction(() => _lastSendTime.Remove(end.From));
                    break;
            }
        }

        private static readonly MethodInfo _itemMsgMethod = typeof(GossipBackend)
            .GetMethod("SetInternalWoEnlist", BindingFlags.Instance | BindingFlags.NonPublic);

        private delegate VectorRelationship ApplyDelegate(string key, object obj);

        private readonly ConcurrentDictionary<Type, ApplyDelegate> _applyDelegates = new ConcurrentDictionary<Type, ApplyDelegate>();

        private ApplyDelegate CreateSetter(Type t)
        {
            var methodInfo = _itemMsgMethod.MakeGenericMethod(t);
            var genericMethod = typeof(GossipBackend).GetMethod("CreateSetterGeneric", BindingFlags.NonPublic | BindingFlags.Instance);
            MethodInfo genericHelper = genericMethod.MakeGenericMethod(t);
            return (ApplyDelegate)genericHelper.Invoke(this, new object[] { methodInfo });
        }

        private ApplyDelegate CreateSetterGeneric<TItem>(MethodInfo setter)
            where TItem : IMergeable<TItem, TItem>
        {
            var setterTypedDelegate = (Func<string, TItem, VectorRelationship>)
                Delegate.CreateDelegate(typeof(Func<string, TItem, VectorRelationship>), this, setter);
            ApplyDelegate setterDelegate = ((key, obj) => setterTypedDelegate(key, (TItem)obj));
            return setterDelegate;
        }

        private void ApplyItems(MessageItem[] items)
        {
            if (items == null)
                return;
            var isConsistent = IsConsistent;
            foreach (var item in items)
            {
                if (!isConsistent && IsGossipBlocked(item.Key))
                    continue;
                var obj = Serializer.Deserialize(item.Data);
                var method = _applyDelegates.GetOrAdd(obj.GetType(), CreateSetter);
                method(item.Key, obj);
            }
        }

        private void SendEnd(string server, bool success)
        {
            Send(server, new GossipEnd { From = Transport.OwnId, Success = success }, true);
        }

        private void SendReply(string server, ulong hisHash, DateTimeOffset hisTime, GossipReply reply = null)
        {
            var ourLast = reply?.LastTime;
            if (ourLast != null && (DateTimeOffset.UtcNow - ourLast.Value).TotalMilliseconds >= Configuration.AntiEntropyIdleTimeout)
                return;

            var ownHash = _databaseHash.Value;
            if (ownHash == hisHash)
            {
                SendEnd(server, true);
                return;
            }

            bool allNewIncluded = false;
            int cutoff = Configuration.AntiEntropyPackageCutoff;
            int packageSize = Configuration.AntiEntropyPackageSize;
            long? prevFreshness = null; 
            var toSend = YieldReplyItems(reply?.LastWindowStart, reply?.LastWindowEnd)
                .TakeWhile(item =>
                {
                    if (item == null)
                    {
                        allNewIncluded = true;
                        return true;
                    }
                    if (prevFreshness == null || prevFreshness.Value != item.Freshness)
                    {
                        if (--cutoff < 0 || (allNewIncluded && --packageSize < 0))
                            return false;
                        prevFreshness = item.Freshness;
                    }
                    return true;
                })
                .Where(i => i != null)
                .ToArray();
            if (toSend.Length == 0)
            {
                // we reached our end of time, but maybe the other side has more.
                if (reply != null && (reply.Items == null || reply.Items.Length == 0))
                    SendEnd(server, false);
                else
                    Send(server, new GossipReply
                    {
                        From = Transport.OwnId,
                        DatabaseHash = ownHash,
                        Items = null,
                        WindowStart = reply?.LastWindowStart ?? 0,
                        WindowEnd = GetMaxFreshness(),
                        LastWindowStart = reply?.WindowStart,
                        LastWindowEnd = reply?.WindowEnd,
                        LastTime = hisTime,
                    });
                return;
            }

            var windowStart = toSend[toSend.Length - 1].Freshness;
            if (allNewIncluded && reply?.LastWindowStart != null && reply.LastWindowStart < windowStart)
                windowStart = reply.LastWindowStart.Value;

            var windowEnd = GetMaxFreshness();

            Send(server, new GossipReply
            {
                From = Transport.OwnId,
                DatabaseHash = ownHash,
                Items = toSend,
                WindowStart = windowStart,
                WindowEnd = windowEnd,
                LastWindowStart = reply?.WindowStart,
                LastWindowEnd = reply?.WindowEnd,
                LastTime = hisTime,
            });
        }

        private IEnumerable<MessageItem> YieldReplyItems(long? prevWindowStart, long? prevWindowEnd)
        {
            var startFrom = long.MaxValue;
            var result = new HashSet<string>(_gossipBlock.Keys);
            if (prevWindowEnd != null)
            {
                foreach (var kvp in _freshIndex.RangeDescending(long.MaxValue, prevWindowEnd.Value + 1))
                    if (result.Add(kvp.Value))
                        yield return _local[kvp.Value];
                startFrom = prevWindowStart.Value - 1;
            }
            // to signal that the new result connects with the previous window.
            yield return null;
            foreach (var kvp in _freshIndex.RangeDescending(startFrom, long.MinValue))
                if (result.Add(kvp.Value))
                    yield return _local[kvp.Value];
        }

        private readonly ShieldedDictNc<string, CommitContinuation> _continuations = new ShieldedDictNc<string, CommitContinuation>();

        private readonly ShieldedLocal<string> _transactionId = new ShieldedLocal<string>();
        private readonly ShieldedLocal<TaskCompletionSource<bool>> _prepareCompleter = new ShieldedLocal<TaskCompletionSource<bool>>();

        Task<bool> IBackend.Prepare(CommitContinuation cont)
        {
            var transaction = new TransactionInfo();
            TaskCompletionSource<bool> tcs = null;
            var id = WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString());
            cont.InContext(() =>
            {
                transaction.Items = _local.Changes.Select(key =>
                {
                    var local = _local[key];
                    return new TransactionItem
                    {
                        Key = key,
                        Data = local.Data,
                        Expected = local.LastSetResult,
                    };
                }).ToArray();
                if (transaction.Items.Any())
                {
                    _transactionId.Value = id;
                    _prepareCompleter.Value = tcs = new TaskCompletionSource<bool>();
                }
            });
            if (!transaction.Items.Any())
                return Task.FromResult(true);

            Distributed.Run(() =>
            {
                transaction.State = new TransactionVector(
                    new[] { new VectorItem<TransactionState>(Transport.OwnId, TransactionState.None) }
                    .Concat(Transport.Servers.Select(s => new VectorItem<TransactionState>(s, TransactionState.None)))
                    .ToArray());
                _continuations[id] = cont;
                SetInternal(id, transaction);
            }).Wait();
            return tcs.Task;
        }

        Task IBackend.Commit(CommitContinuation cont)
        {
            string id = null;
            cont.InContext(() =>
            {
                if (_transactionId.HasValue)
                    id = _transactionId.Value;
            });
            if (id != null)
            {
                // consistent transaction
                return Distributed.Run(() =>
                {
                    _continuations.Remove(id);
                    if (!TryGetInternal(id, out TransactionInfo current))
                        return;
                    SetInternal(id, new TransactionInfo
                    {
                        Items = current.Items,
                        State = current.State.Modify(Transport.OwnId, TransactionState.Success),
                    });
                });
            }

            // eventual transaction
            if (!Configuration.DirectMail)
                return Task.FromResult<object>(null);
            var transaction = new DirectMail();
            cont.InContext(() => transaction.Items = _local.Changes.Select(key => _local[key]).ToArray());
            if (transaction.Items.Any())
                Transport.Broadcast(transaction);
            return Task.FromResult<object>(null);
        }

        void IBackend.Rollback() { }

        private string WrapPublicKey(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException();
            return PublicPfx + key;
        }

        private string WrapInternalKey(string prefix, string key)
        {
            return prefix + key;
        }

        private const string PublicPfx = "|";
        private const string TransactionPfx = "transaction|";

        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            key = WrapPublicKey(key);
            item = default;
            if (!_local.TryGetValue(key, out MessageItem i))
                return false;
            item = (TItem)Serializer.Deserialize(i.Data);
            return true;
        }

        private bool TryGetInternal<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            item = default;
            if (!_local.TryGetValue(key, out MessageItem i))
                return false;
            item = (TItem)Serializer.Deserialize(i.Data);
            return true;
        }

        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            key = WrapPublicKey(key);
            Distributed.EnlistBackend(this);
            return SetInternal(key, item);
        }

        public VectorRelationship SetVersion<TItem>(string key, TItem item) where TItem : IHasVectorClock
        {
            return Set(key, (Multiple<TItem>)item);
        }

        private ulong GetHash<TItem>(string key, TItem i) where TItem : IHasVersionHash
        {
            return FNV1a64.Hash(
                Encoding.UTF8.GetBytes(key),
                BitConverter.GetBytes(i.GetVersionHash()));
        }

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem, TItem>
        {
            Distributed.EnlistBackend(this);
            return SetInternalWoEnlist(key, val);
        }

        private VectorRelationship SetInternalWoEnlist<TItem>(string key, TItem val) where TItem : IMergeable<TItem, TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException();
            if (_local.TryGetValue(key, out MessageItem oldItem))
            {
                var oldVal = (TItem)Serializer.Deserialize(oldItem.Data);
                var cmp = oldVal.VectorCompare(val);
                if (cmp == VectorRelationship.Greater || cmp == VectorRelationship.Equal)
                    return cmp;

                val = oldVal.MergeWith(val);
                _local[key] = new MessageItem { Key = key, Data = Serializer.Serialize(val), LastSetResult = cmp };
                if (IsConsistent)
                {
                    _localOldHash.Value = _localOldHash.GetValueOrDefault() ^ GetHash(key, oldVal);
                    _localNewHash.Value = _localNewHash.GetValueOrDefault() ^ GetHash(key, val);
                }
                else
                {
                    var hash = GetHash(key, oldVal) ^ GetHash(key, val);
                    _databaseHash.Commute((ref ulong h) => h ^= hash);
                }
                OnChanging(key, oldVal, val);
                return cmp;
            }
            else
            {
                _local[key] = new MessageItem { Key = key, Data = Serializer.Serialize(val), LastSetResult = VectorRelationship.Less };
                var hash = GetHash(key, val);
                if (IsConsistent)
                    _localNewHash.Value = _localNewHash.GetValueOrDefault() ^ hash;
                else
                    _databaseHash.Commute((ref ulong h) => h ^= hash);
                OnChanging(key, null, val);
                return VectorRelationship.Less;
            }
        }

        private void OnChanging(string key, object oldVal, object newVal)
        {
            if (key.StartsWith(TransactionPfx))
                OnTransactionChanging(key, oldVal as TransactionInfo, newVal as TransactionInfo);
        }

        private void OnTransactionChanging(string id, TransactionInfo oldVal, TransactionInfo newVal)
        {
            if (oldVal == null)
            {
                if (newVal == null)
                    throw new ApplicationException("Both old and new transaction info are null.");
                if (_continuations.ContainsKey(id))
                    return;
                Shield.SideEffect(async () =>
                {
                    bool ok = true;
                    CommitContinuation cont = null;
                    try
                    {
                        cont = Shield.RunToCommit(Timeout.Infinite, () =>
                        {
                            _isExternalConsistent.Value = true;
                            if (!TryGetInternal(id, out TransactionInfo current))
                            {
                                ok = false;
                                return;
                            }
                            ApplyItems(current.Items);
                            var items = _local.Changes.ToDictionary(k => k, k => _local[k]);
                            ok = current.Items.All(input => IsOkApplied(input, items.TryGetValue(input.Key, out var local) ? local : null));
                        });
                        if (!ok)
                        {
                            await Distributed.Run(() =>
                            {
                                if (!TryGetInternal(id, out TransactionInfo current))
                                    return;
                                SetInternal(id, new TransactionInfo
                                {
                                    Items = newVal.Items,
                                    State = current.State.Modify(Transport.OwnId, TransactionState.Fail),
                                });
                            });
                            return;
                        }
                        ok = await Distributed.Run(() =>
                        {
                            if (!TryGetInternal(id, out TransactionInfo current))
                                return false;
                            if (current.State.GetOwn(Transport.OwnId) != TransactionState.None)
                                return false;
                            var newState = current.State.Modify(Transport.OwnId, TransactionState.Prepared);
                            _continuations[id] = cont;
                            // this is kinda recursive!
                            SetInternal(id, new TransactionInfo
                            {
                                Items = current.Items,
                                State = newState,
                            });
                            return true;
                        });
                        if (!ok)
                            cont.TryRollback();
                    }
                    catch
                    {
                        if (cont != null)
                            cont.TryRollback();
                        throw;
                    }
                });
            }
            else if (newVal == null)
            {
                // TODO: currently we never remove a transaction.
            }
            else // both != null
            {
                Shield.SideEffect(async () =>
                {
                    await Distributed.Run(() =>
                    {
                        if (!_continuations.TryGetValue(id, out var cont))
                            return;
                        if (!TryGetInternal(id, out TransactionInfo current) || current.State.Items.Any(s => s.Value == TransactionState.Fail))
                        {
                            _continuations.Remove(id);
                            TaskCompletionSource<bool> tcs = null;
                            cont.InContext(() => tcs = _prepareCompleter.Value);
                            Shield.SideEffect(() =>
                            {
                                if (tcs != null)
                                    tcs.SetResult(false);
                                else
                                    cont.TryRollback();
                            });
                        }
                        else if (current.State.WithoutMe(Transport.OwnId).All(s => s.Value == TransactionState.Prepared) &&
                            current.State.GetOwn(Transport.OwnId) == TransactionState.None)
                        {
                            TaskCompletionSource<bool> tcs = null;
                            cont.InContext(() => tcs = _prepareCompleter.Value);
                            Shield.SideEffect(() => tcs.TrySetResult(true));
                        }
                        else if (current.State.Items.Any(s => s.Value == TransactionState.Success) &&
                            current.State.GetOwn(Transport.OwnId) != TransactionState.Success)
                        {
                            _continuations.Remove(id);
                            Shield.SideEffect(() =>
                            {
                                cont.TryCommit();
                                Distributed.Run(() =>
                                    SetInternal(id, new TransactionInfo
                                    {
                                        Items = current.Items,
                                        State = current.State.Modify(Transport.OwnId, TransactionState.Success),
                                    })).Wait();
                            });
                        }
                    });
                });
            }
        }

        private static bool IsOkApplied(TransactionItem incoming, MessageItem applied)
        {
            return incoming.Expected == applied?.LastSetResult;
        }

        public void Dispose()
        {
            Transport.Dispose();
            _gossipTimer.Dispose();
            _preCommit.Dispose();
        }
    }
}
