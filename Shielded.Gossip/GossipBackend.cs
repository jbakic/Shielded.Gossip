using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace Shielded.Gossip
{
    /// <summary>
    /// A backend supporting a key/value store which is distributed using a simple gossip protocol
    /// implementation. Use it in ordinary <see cref="Shield"/> transactions.
    /// Values should be CRDTs, implementing <see cref="IMergeable{T}"/>, or you can use the
    /// <see cref="Multiple{T}"/> and <see cref="Vc{T}"/> wrappers to make them a CRDT. If a type
    /// implements <see cref="IDeletable"/>, it can be deleted from the storage.
    /// </summary>
    public class GossipBackend : IGossipBackend, IDisposable
    {
        private readonly ShieldedDictNc<string, MessageItem> _local = new ShieldedDictNc<string, MessageItem>();
        private readonly ShieldedTreeNc<long, string> _freshIndex = new ShieldedTreeNc<long, string>();
        private readonly Shielded<VersionHash> _databaseHash = new Shielded<VersionHash>();
        private readonly ShieldedLocal<bool> _changeLock = new ShieldedLocal<bool>();
        private readonly ShieldedLocal<HashSet<string>> _keysToMail = new ShieldedLocal<HashSet<string>>();

        private readonly Timer _gossipTimer;
        private readonly Timer _deletableTimer;
        private readonly IDisposable _preCommit;

        public readonly ITransport Transport;
        public readonly GossipConfiguration Configuration;

        /// <summary>
        /// If set inside a transaction, restricts the direct mail message sending
        /// to only this server. Affects only the current transaction. If set to null, no
        /// direct mail will be sent.
        /// </summary>
        public readonly ShieldedLocal<string> DirectMailRestriction = new ShieldedLocal<string>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transport">The message transport to use. The backend will dispose it when it gets disposed.</param>
        /// <param name="configuration">The configuration.</param>
        public GossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            Transport = transport;
            Configuration = configuration;
            Transport.MessageReceived += Transport_MessageReceived;

            _gossipTimer = new Timer(_ => SpreadRumors(), null, Configuration.GossipInterval, Configuration.GossipInterval);
            _deletableTimer = new Timer(GetDeletableTimerMethod(), null, Configuration.DeletableCleanUpInterval, Configuration.DeletableCleanUpInterval);

            _preCommit = Shield.PreCommit(() => _local.TryGetValue("any", out MessageItem _) || true, () =>
            {
                // so nobody sneaks in a PreCommit after this one, and screws up the fresh index.
                _changeLock.Value = true;
                SyncIndexes();
                if (_keysToMail.HasValue && _keysToMail.Value.Count > 0)
                {
                    var hasRestriction = DirectMailRestriction.HasValue;
                    var restriction = hasRestriction ? DirectMailRestriction.Value : null;
                    var items = _keysToMail.Value
                        .Select(key => _local.TryGetValue(key, out var mi) ? mi : null)
                        .Where(mi => mi != null)
                        .ToArray();
                    Shield.SideEffect(() => DoDirectMail(items, hasRestriction, restriction));
                }
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
                    var (toRemove, freshness) = Shield.InTransaction(() =>
                        (_freshIndex.Range(0, lastFreshness)
                            .Where(kvp => _local[kvp.Value].Deletable)
                            .Select(kvp => kvp.Value)
                            .ToArray(),
                        GetMaxFreshness()));
                    Shield.InTransaction(() =>
                    {
                        foreach (var key in toRemove)
                            if (_local.TryGetValue(key, out var mi) && mi.Freshness <= lastFreshness)
                                _local.Remove(key);
                        lastFreshness.Value = freshness;
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
            var newFresh = GetNextFreshness();
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

        private void DoDirectMail(MessageItem[] items, bool hasRestriction, string restriction)
        {
            if (Configuration.DirectMail == DirectMailType.Off || items.Length == 0)
                return;
            var package = new DirectMail { Items = items };
            if (hasRestriction)
            {
                if (!string.IsNullOrWhiteSpace(restriction))
                    SendMail(restriction, package);
            }
            else if (Configuration.DirectMail == DirectMailType.Always)
            {
                Transport.Broadcast(package);
            }
            else
            {
                foreach (var server in Transport.Servers)
                    SendMail(server, package);
            }
        }

        private void SendMail(string server, DirectMail package)
        {
            if (Configuration.DirectMail == DirectMailType.Always ||
                Configuration.DirectMail == DirectMailType.GossipSupressed && !IsGossipActive(server))
                Transport.Send(server, package);
            else if (Configuration.DirectMail == DirectMailType.StartGossip)
                StartGossip(server);
        }

        private ShieldedDictNc<string, DateTimeOffset> _lastSendTime = new ShieldedDictNc<string, DateTimeOffset>(StringComparer.InvariantCultureIgnoreCase);

        private bool IsGossipActive(string server) =>
            _lastSendTime.TryGetValue(server, out var last) &&
            (DateTimeOffset.UtcNow - last).TotalMilliseconds < Configuration.AntiEntropyIdleTimeout;

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
                    while (!StartGossip(server) && --limit >= 0);
                });
            }
            catch { } // TODO
        }

        private bool StartGossip(string server) => Shield.InTransaction(() =>
        {
            if (IsGossipActive(server))
                return false;
            _lastSendTime[server] = DateTimeOffset.UtcNow;
            var toSend = GetPackage(Configuration.AntiEntropyPushPackages, null, null, out var _);
            Shield.SideEffect(() =>
            {
                Transport.Send(server, new NewGossip
                {
                    From = Transport.OwnId,
                    DatabaseHash = _databaseHash.Value,
                    Items = toSend,
                    WindowStart = toSend.Length == 0 ? (long?)null : toSend[toSend.Length - 1].Freshness,
                    WindowEnd = toSend.Length == 0 ? (long?)null : toSend[0].Freshness
                });
            });
            return true;
        });

        private void Transport_MessageReceived(object sender, object msg)
        {
            switch (msg)
            {
                case DirectMail trans:
                    ApplyItems(trans.Items);
                    break;

                case NewGossip pkg:
                    if (pkg.DatabaseHash != _databaseHash.Value)
                        ApplyItems(pkg.Items);
                    SendReply(pkg);
                    break;

                case GossipEnd end:
                    SendReply(end);
                    break;
            }
        }

        private readonly ApplyMethods _applyMethods = new ApplyMethods(typeof(GossipBackend)
            .GetMethod("SetInternal", BindingFlags.Instance | BindingFlags.NonPublic));

        /// <summary>
        /// Applies the given items internally, does not cause any direct mail.
        /// </summary>
        public void ApplyItems(IEnumerable<MessageItem> items) => Shield.InTransaction(() =>
        {
            if (items == null)
                return;
            foreach (var item in items)
            {
                if (_local.TryGetValue(item.Key, out var curr) && IsByteEqual(curr.Data, item.Data))
                    continue;
                var obj = item.Value;
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

        private bool ShouldKillChain(string server, DateTimeOffset? ourLast)
        {
            return
                ourLast != null && (DateTimeOffset.UtcNow - ourLast.Value).TotalMilliseconds >= Configuration.AntiEntropyIdleTimeout
                ||
                StringComparer.InvariantCultureIgnoreCase.Compare(server, Transport.OwnId) < 0 &&
                _lastSendTime.TryGetValue(server, out var ourLastAny) &&
                ourLastAny > (ourLast ?? DateTimeOffset.UtcNow.AddMilliseconds(-Configuration.AntiEntropyIdleTimeout));
        }

        private void SendReply(GossipMessage replyTo) => Shield.InTransaction(() =>
        {
            var server = replyTo.From;
            var hisReply = replyTo as IGossipReply;
            var hisNews = replyTo as NewGossip;

            var ourLast = hisReply?.LastTime;
            if (ShouldKillChain(replyTo.From, ourLast))
                return;

            var ownHash = _databaseHash.Value;
            if (ownHash == replyTo.DatabaseHash)
            {
                // hisNews == null means his message was already a GossipEnd.
                if (hisNews == null)
                    _lastSendTime.Remove(server);
                else
                    SendEnd(hisNews, true);
                return;
            }

            var toSend = GetPackage(Configuration.AntiEntropyReplyPackages,
                hisReply?.LastWindowStart, hisReply?.LastWindowEnd, out var connectedWithLast);

            if (toSend.Length == 0)
            {
                if (hisNews == null)
                    _lastSendTime.Remove(server);
                else if (hisNews.Items == null || hisNews.Items.Length == 0)
                    SendEnd(hisNews, false);
                else
                    SendReply(server, new GossipReply
                    {
                        From = Transport.OwnId,
                        DatabaseHash = ownHash,
                        Items = null,
                        WindowStart = hisReply?.LastWindowStart,
                        WindowEnd = hisReply?.LastWindowEnd,
                        LastWindowStart = hisNews.WindowStart,
                        LastWindowEnd = hisNews.WindowEnd,
                        LastTime = replyTo.Time,
                    });
                return;
            }

            var windowStart = toSend[toSend.Length - 1].Freshness;
            if (connectedWithLast && hisReply?.LastWindowStart != null && hisReply.LastWindowStart < windowStart)
                windowStart = hisReply.LastWindowStart.Value;

            var windowEnd = GetMaxFreshness();

            SendReply(server, new GossipReply
            {
                From = Transport.OwnId,
                DatabaseHash = ownHash,
                Items = toSend,
                WindowStart = windowStart,
                WindowEnd = windowEnd,
                LastWindowStart = hisNews?.WindowStart,
                LastWindowEnd = hisNews?.WindowEnd,
                LastTime = replyTo.Time,
            });
        });

        private void SendEnd(NewGossip hisNews, bool success)
        {
            // if we're sending GossipEnd, we clear this in transaction, to make sure
            // IsGossipActive is correct, and to guarantee that we actually are done.
            _lastSendTime.Remove(hisNews.From);
            var ourHash = _databaseHash.Value;
            Shield.SideEffect(() => Transport.Send(hisNews.From, new GossipEnd
            {
                From = Transport.OwnId,
                Success = success,
                DatabaseHash = ourHash,
                LastWindowStart = hisNews.WindowStart,
                LastWindowEnd = hisNews.WindowEnd,
                LastTime = hisNews.Time,
            }));
        }

        private void SendReply(string server, GossipReply msg)
        {
            Shield.SideEffect(() =>
            {
                // reply transactions are kept read-only since they conflict too easily,
                // and it really makes no difference, whatever we skipped now, we'll see
                // in the next reply. so we change this only in the side-effect.
                Shield.InTransaction(() => _lastSendTime[server] = msg.Time);
                Transport.Send(server, msg);
            });
        }

        private MessageItem[] GetPackage(int packageSize, long? lastWindowStart, long? lastWindowEnd, out bool connectedWithLast)
        {
            int cutoff = Configuration.AntiEntropyCutoff;
            long? prevFreshness = null;
            var connected = false;
            long? connectedAt = null;
            var toSend = YieldItems(lastWindowStart, lastWindowEnd)
                .TakeWhile(item =>
                {
                    if (item == null)
                    {
                        connected = true;
                        connectedAt = prevFreshness;
                        return true;
                    }
                    if (prevFreshness == null || prevFreshness.Value != item.Freshness)
                    {
                        if (cutoff <= 0 || (connected && --packageSize < 0))
                            return false;
                        prevFreshness = item.Freshness;
                    }
                    --cutoff;
                    return true;
                })
                .Where(i => i != null)
                .ToArray();
            // if crossed the cutoff, remove the last package, except if it is the only one.
            // this forces any package > cutoff to be sent alone.
            if (cutoff < 0 && toSend[0].Freshness != toSend[toSend.Length - 1].Freshness)
            {
                var removeFreshness = toSend[toSend.Length - 1].Freshness;
                if (removeFreshness == connectedAt)
                    connected = false;
                toSend = toSend.TakeWhile(item => item.Freshness > removeFreshness).ToArray();
            }
            connectedWithLast = connected;
            return toSend;
        }

        private IEnumerable<MessageItem> YieldItems(long? prevWindowStart, long? prevWindowEnd)
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

        /// <summary>
        /// Tries to read the value under the given key. The type of the value must be a CRDT.
        /// </summary>
        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            item = default;
            if (!_local.TryGetValue(key, out MessageItem i))
                return false;
            item = (TItem)i.Value;
            return true;
        }

        internal MessageItem GetItem(string key)
        {
            return _local.TryGetValue(key, out MessageItem i) ? i : null;
        }

        private VersionHash GetHash<TItem>(string key, TItem i) where TItem : IHasVersionHash
        {
            return FNV1a64.Hash(
                Encoding.UTF8.GetBytes(key),
                BitConverter.GetBytes(i.GetVersionHash()));
        }

        /// <summary>
        /// Sets the given value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value. The storage gets affected
        /// only if the result of comparison is Greater or Conflict.
        /// </summary>
        public VectorRelationship Set<TItem>(string key, TItem val) where TItem : IMergeable<TItem>
        {
            var res = SetInternal(key, val);
            if ((res & VectorRelationship.Greater) == 0)
                return res;
            var set = _keysToMail.HasValue ? _keysToMail.Value : (_keysToMail.Value = new HashSet<string>());
            set.Add(key);
            return res;
        }

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            if (val == null)
                throw new ArgumentNullException("val");
            if (_changeLock.GetValueOrDefault())
                throw new InvalidOperationException("Changes are blocked at this time.");
            if (_local.TryGetValue(key, out MessageItem oldItem))
            {
                var oldVal = (TItem)oldItem.Value;
                var cmp = val.VectorCompare(oldVal);
                if (cmp == VectorRelationship.Less || cmp == VectorRelationship.Equal)
                    return cmp;
                // we support this only for safety - a CanDelete should never accept any changes, nor switch to !CanDelete.
                var oldDeletable = oldVal is IDeletable oldDel && oldDel.CanDelete;
                var oldHash = oldDeletable ? 0 : GetHash(key, oldVal);
                // in case someone screws up the MergeWith impl, we call it after extracting the critical info above.
                val = oldVal.MergeWith(val);

                var deletable = val is IDeletable del && del.CanDelete;
                _local[key] = new MessageItem
                {
                    Key = key,
                    Value = val,
                    Deletable = deletable,
                };
                var hash = oldHash ^ (deletable ? 0 : GetHash(key, val));
                _databaseHash.Commute((ref VersionHash h) => h ^= hash);

                OnChanged(key, oldVal, val);
                return cmp;
            }
            else
            {
                if (val is IDeletable del && del.CanDelete)
                    return VectorRelationship.Equal;
                _local[key] = new MessageItem
                {
                    Key = key,
                    Value = val
                };
                var hash = GetHash(key, val);
                _databaseHash.Commute((ref VersionHash h) => h ^= hash);

                OnChanged(key, null, val);
                return VectorRelationship.Greater;
            }
        }

        private void OnChanged(string key, object oldVal, object newVal)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired when any key changes.
        /// </summary>
        public readonly ShieldedEvent<ChangedEventArgs> Changed = new ShieldedEvent<ChangedEventArgs>();

        public void Dispose()
        {
            Transport.Dispose();
            _gossipTimer.Dispose();
            _deletableTimer.Dispose();
            _preCommit.Dispose();
        }

        /// <summary>
        /// An enumerable of keys read or written into by the current transaction. Includes
        /// keys that did not have a value.
        /// </summary>
        public IEnumerable<string> Reads => _local.Reads;

        /// <summary>
        /// An enumerable of keys written into by the current transaction.
        /// </summary>
        public IEnumerable<string> Changes => _local.Changes;
    }
}
