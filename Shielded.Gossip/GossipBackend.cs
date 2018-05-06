using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    /// <summary>
    /// A backend supporting a key/value store which is distributed using a simple gossip protocol
    /// implementation. Can be used in <see cref="Distributed.Consistent"/> calls, but is always
    /// only eventually consistent.
    /// Values should be CRDTs, implementing <see cref="IMergeable{TIn, TOut}"/>, or you can use the
    /// <see cref="Multiple{T}"/> and <see cref="Vc{T}"/> wrappers to make them a CRDT. If a type
    /// implements <see cref="IDeletable"/>, it can be deleted from the storage.
    /// </summary>
    public class GossipBackend : IBackend, IDisposable
    {
        private readonly ShieldedDictNc<string, MessageItem> _local = new ShieldedDictNc<string, MessageItem>();
        private readonly ShieldedTreeNc<long, string> _freshIndex = new ShieldedTreeNc<long, string>();
        private readonly Shielded<ulong> _databaseHash = new Shielded<ulong>();
        private readonly ShieldedLocal<bool> _changeLock = new ShieldedLocal<bool>();

        private readonly Timer _gossipTimer;
        private readonly Timer _deletableTimer;
        private readonly IDisposable _preCommit;

        private readonly IBackend _owner;

        public readonly ITransport Transport;
        public readonly GossipConfiguration Configuration;

        /// <summary>
        /// If set inside a distributed transaction, restricts the direct mail message sending
        /// to only this server. Affects only the current transaction. Does not affect gossip.
        /// </summary>
        public readonly ShieldedLocal<string> DirectMailRestriction = new ShieldedLocal<string>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transport">The message transport to use.</param>
        /// <param name="configuration">The configuration.</param>
        /// <param name="owner">If not null, this backend will enlist the owner instead of itself.
        /// The owner then controls when exactly our IBackend methods get called.</param>
        public GossipBackend(ITransport transport, GossipConfiguration configuration, IBackend owner = null)
        {
            Transport = transport;
            Configuration = configuration;
            _owner = owner ?? this;
            Transport.MessageReceived += Transport_MessageReceived;

            _gossipTimer = new Timer(_ => SpreadRumors(), null, Configuration.GossipInterval, Configuration.GossipInterval);
            _deletableTimer = new Timer(GetDeletableTimerMethod(), null, Configuration.DeletableCleanUpInterval, Configuration.DeletableCleanUpInterval);

            _preCommit = Shield.PreCommit(() => _local.TryGetValue("any", out MessageItem _) || true, () =>
            {
                // so nobody sneaks in a PreCommit after this one, and screws up the fresh index.
                _changeLock.Value = true;
                SyncIndexes();
            });
        }

        private TimerCallback GetDeletableTimerMethod()
        {
            var lastFreshness = new Shielded<long>();
            var lockObj = new object();
            return _ =>
            {
                bool lockTaken = false;
                try
                {
                    Monitor.TryEnter(lockObj, ref lockTaken);
                    if (!lockTaken)
                        return;
                    Shield.InTransaction(() =>
                    {
                        var toRemove = _freshIndex.Range(0, lastFreshness)
                            .Where(kvp => _local[kvp.Value].Deletable)
                            .Select(kvp => kvp.Value)
                            .ToArray();
                        lastFreshness.Value = GetMaxFreshness();
                        foreach (var key in toRemove)
                            _local.Remove(key);
                    });
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(lockObj);
                }
            };
        }

        private void SyncIndexes()
        {
            long newFresh = GetNextFreshness();
            foreach (var key in _local.Changes)
            {
                var oldItem = Shield.ReadOldState(() => _local.TryGetValue(key, out MessageItem o) ? o : null);
                if (oldItem != null)
                    _freshIndex.Remove(oldItem.Freshness, key);

                if (_local.TryGetValue(key, out var newItem))
                {
                    newItem.Freshness = newFresh;
                    _freshIndex.Add(newFresh, key);
                }
            }
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
                var time =
                    clearState ? (DateTimeOffset?)null :
                    msg is GossipStart start ? start.Time :
                    msg is GossipReply reply ? reply.Time :
                    DateTimeOffset.UtcNow;
                Shield.InTransaction(() =>
                {
                    if (clearState)
                        _lastSendTime.Remove(server);
                    else
                        _lastSendTime[server] = time.Value;
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
                    ApplyItems(trans.Items);
                    break;

                case GossipStart start:
                    SendReply(start.From, start.DatabaseHash, start.Time);
                    break;

                case GossipReply reply:
                    ApplyItems(reply.Items);
                    SendReply(reply.From, reply.DatabaseHash, reply.Time, reply);
                    break;

                case GossipEnd end:
                    Shield.InTransaction(() => _lastSendTime.Remove(end.From));
                    break;
            }
        }

        private readonly ApplyMethods _applyMethods = new ApplyMethods(typeof(GossipBackend)
            .GetMethod("SetInternalWoEnlist", BindingFlags.Instance | BindingFlags.NonPublic));

        /// <summary>
        /// Applies the given items internally, does not enlist the backend in any distributed
        /// transaction.
        /// </summary>
        public void ApplyItems(IEnumerable<MessageItem> items) => Shield.InTransaction(() =>
        {
            if (items == null)
                return;
            foreach (var item in items)
            {
                if (_local.TryGetValue(item.Key, out var curr) && IsByteEqual(curr.Data, item.Data))
                    continue;
                var obj = Serializer.Deserialize(item.Data);
                var method = _applyMethods.Get(this, obj.GetType());
                method(item.Key, obj);
            }
        });

        private static bool IsByteEqual(byte[] one, byte[] two)
        {
            if (one == null && two == null)
                return true;
            if (one == null || two == null || one.Length != two.Length)
                return false;
            var len = one.Length;
            for (int i = 0; i < len; i++)
                if (one[i] != two[i])
                    return false;
            return true;
        }

        private void SendEnd(string server, bool success)
        {
            Send(server, new GossipEnd { From = Transport.OwnId, Success = success }, true);
        }

        private bool ShouldKillChain(string server, DateTimeOffset ourLast)
        {
            return
                (DateTimeOffset.UtcNow - ourLast).TotalMilliseconds >= Configuration.AntiEntropyIdleTimeout
                ||
                StringComparer.InvariantCultureIgnoreCase.Compare(server, Transport.OwnId) < 0 &&
                _lastSendTime.TryGetValue(server, out var ourLastAny) &&
                ourLastAny > ourLast;
        }

        private void SendReply(string server, ulong hisHash, DateTimeOffset hisTime, GossipReply reply = null) => Shield.InTransaction(() =>
        {
            var ourLast = reply?.LastTime;
            if (ourLast != null && ShouldKillChain(server, ourLast.Value))
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
                    --cutoff;
                    if (prevFreshness == null || prevFreshness.Value != item.Freshness)
                    {
                        if (cutoff < 0 || (allNewIncluded && --packageSize < 0))
                            return false;
                        prevFreshness = item.Freshness;
                    }
                    return true;
                })
                .Where(i => i != null)
                .ToArray();
            // if crossed the cutoff, remove the last package, except if it is the only one.
            // this forces any package > cutoff to be sent alone.
            if (cutoff < 0 && toSend[0].Freshness != toSend[toSend.Length - 1].Freshness)
            {
                var removeFreshness = toSend[toSend.Length - 1].Freshness;
                toSend = toSend.TakeWhile(item => item.Freshness > removeFreshness).ToArray();
            }

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
        });

        private IEnumerable<MessageItem> YieldReplyItems(long? prevWindowStart, long? prevWindowEnd)
        {
            var startFrom = long.MaxValue;
            if (prevWindowEnd != null)
            {
                foreach (var kvp in _freshIndex.RangeDescending(long.MaxValue, prevWindowEnd.Value + 1))
                    yield return _local[kvp.Value];
                startFrom = prevWindowStart.Value - 1;
            }
            // to signal that the new result connects with the previous window.
            yield return null;
            foreach (var kvp in _freshIndex.RangeDescending(startFrom, long.MinValue))
                yield return _local[kvp.Value];
        }

        Task<PrepareResult> IBackend.Prepare(CommitContinuation cont) => Task.FromResult(new PrepareResult(true));

        Task IBackend.Commit(CommitContinuation cont)
        {
            if (!Configuration.DirectMail)
                return Task.FromResult<object>(null);
            var package = new DirectMail();
            bool hasRestriction = false;
            string restriction = null;
            cont.InContext(() =>
            {
                hasRestriction = DirectMailRestriction.HasValue;
                if (hasRestriction)
                    restriction = DirectMailRestriction.Value;
                package.Items = _local.Changes
                    .Select(key => _local.TryGetValue(key, out var mi) ? mi : new MessageItem { Key = key })
                    .ToArray();
            });
            if (package.Items.Any())
            {
                if (!hasRestriction)
                    Transport.Broadcast(package);
                else if (!string.IsNullOrWhiteSpace(restriction))
                    Transport.Send(restriction, package);
            }
            return Task.FromResult<object>(null);
        }

        void IBackend.Rollback() { }

        /// <summary>
        /// Tries to read the value under the given key. The type of the value must be a CRDT.
        /// </summary>
        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            item = default;
            if (!_local.TryGetValue(key, out MessageItem i))
                return false;
            item = (TItem)Serializer.Deserialize(i.Data);
            return true;
        }

        /// <summary>
        /// Tries to read the value(s) under the given key. Used with types that implement
        /// <see cref="IHasVectorClock"/>. In case of conflict we return all conflicting versions.
        /// If the key is not found, returns an empty Multiple.
        /// </summary>
        public Multiple<T> TryGetMultiple<T>(string key) where T : IHasVectorClock
        {
            return TryGet(key, out Multiple<T> multi) ? multi : default;
        }

        /// <summary>
        /// Tries to read the value(s) under the given key. Can be used with any type - it will get wrapped
        /// in a <see cref="Vc{T}"/> for vector clock versioning. In case of conflict we return all
        /// conflicting versions.
        /// If the key is not found, returns an empty Multiple.
        /// </summary>
        public Multiple<Vc<T>> TryGetClocked<T>(string key)
        {
            return TryGet(key, out Multiple<Vc<T>> multi) ? multi : default;
        }

        /// <summary>
        /// Sets the given value under the given key, merging it with any already existing value
        /// there. Returns the result of comparison between the old and new value, or
        /// <see cref="VectorRelationship.Less"/> if there is no old value. The storage gets affected
        /// only if the result of comparison is Less or Conflict.
        /// </summary>
        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            return SetInternal(key, item);
        }

        /// <summary>
        /// Helper for types which implement <see cref="IHasVectorClock"/>, or are wrapped in a <see cref="Vc{T}"/>
        /// wrapper.
        /// </summary>
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
            Distributed.EnlistBackend(_owner);
            return SetInternalWoEnlist(key, val);
        }

        private VectorRelationship SetInternalWoEnlist<TItem>(string key, TItem val) where TItem : IMergeable<TItem, TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException();
            if (_changeLock.GetValueOrDefault())
                throw new InvalidOperationException("Changes are blocked at this time.");
            if (_local.TryGetValue(key, out MessageItem oldItem))
            {
                var oldVal = (TItem)Serializer.Deserialize(oldItem.Data);
                var cmp = oldVal.VectorCompare(val);
                if (cmp == VectorRelationship.Greater || cmp == VectorRelationship.Equal)
                    return cmp;
                val = oldVal.MergeWith(val);

                if (OnChanging(key, oldVal, val))
                    return VectorRelationship.Greater;
                // we support this only for safety - a CanDelete should never accept any changes, nor switch to !CanDelete.
                var oldDeletable = oldVal is IDeletable oldDel && oldDel.CanDelete;
                var deletable = val is IDeletable del && del.CanDelete;
                _local[key] = new MessageItem
                {
                    Key = key,
                    Data = Serializer.Serialize(val),
                    Deletable = deletable,
                };
                var hash = (oldDeletable ? 0 : GetHash(key, oldVal)) ^ (deletable ? 0 : GetHash(key, val));
                _databaseHash.Commute((ref ulong h) => h ^= hash);
                return cmp;
            }
            else
            {
                if (OnChanging(key, default(TItem), val))
                    return VectorRelationship.Greater;

                var deletable = val is IDeletable del && del.CanDelete;
                _local[key] = new MessageItem
                {
                    Key = key,
                    Data = Serializer.Serialize(val),
                    Deletable = deletable
                };
                if (!deletable)
                {
                    var hash = GetHash(key, val);
                    _databaseHash.Commute((ref ulong h) => h ^= hash);
                }
                return VectorRelationship.Less;
            }
        }

        private bool OnChanging(string key, object oldVal, object newVal)
        {
            var ch = Changing;
            if (ch == null)
                return false;
            var ev = new ChangingEventArgs(key, oldVal, newVal);
            ch.Invoke(this, ev);
            return ev.Cancel;
        }

        /// <summary>
        /// Fired during any change, allowing the change to be cancelled.
        /// </summary>
        public event EventHandler<ChangingEventArgs> Changing;

        public void Dispose()
        {
            Transport.Dispose();
            _gossipTimer.Dispose();
            _deletableTimer.Dispose();
            _preCommit.Dispose();
        }
    }
}
