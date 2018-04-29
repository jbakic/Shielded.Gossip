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
    public class GossipBackend : IBackend, IDisposable
    {
        private readonly ShieldedDictNc<string, MessageItem> _local = new ShieldedDictNc<string, MessageItem>();
        private readonly ShieldedTreeNc<long, string> _freshIndex = new ShieldedTreeNc<long, string>();
        private readonly Shielded<ulong> _databaseHash = new Shielded<ulong>();

        private readonly Timer _gossipTimer;
        private readonly IDisposable _preCommit;

        public readonly ITransport Transport;
        public readonly GossipConfiguration Configuration;

        public readonly ShieldedLocal<string> DirectMailRestriction = new ShieldedLocal<string>();

        public GossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            Transport = transport;
            Configuration = configuration;
            Transport.MessageReceived += Transport_MessageReceived;

            _gossipTimer = new Timer(_ => SpreadRumors(), null, Configuration.GossipInterval, Configuration.GossipInterval);

            _preCommit = Shield.PreCommit(() => _local.TryGetValue("any", out MessageItem _) || true, () =>
            {
                SyncIndexes();
            });
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
                    newItem.Freshness = newFresh;
                _freshIndex.Add(newFresh, key);
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

        private readonly ApplyMethods _applyMethods = new ApplyMethods(typeof(GossipBackend)
            .GetMethod("SetInternalWoEnlist", BindingFlags.Instance | BindingFlags.NonPublic));

        public void ApplyItems(IEnumerable<MessageItem> items)
        {
            if (items == null)
                return;
            foreach (var item in items)
            {
                if (item.Data == null)
                {
                    RemoveInternal(item.Key);
                    continue;
                }
                var obj = Serializer.Deserialize(item.Data);
                var method = _applyMethods.Get(this, obj.GetType());
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
            if (prevWindowEnd != null)
            {
                foreach (var kvp in _freshIndex.RangeDescending(long.MaxValue, prevWindowEnd.Value + 1))
                    yield return _local.TryGetValue(kvp.Value, out var mi) ? mi : new MessageItem { Key = kvp.Value };
                startFrom = prevWindowStart.Value - 1;
            }
            // to signal that the new result connects with the previous window.
            yield return null;
            foreach (var kvp in _freshIndex.RangeDescending(startFrom, long.MinValue))
                yield return _local.TryGetValue(kvp.Value, out var mi) ? mi : new MessageItem { Key = kvp.Value };
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

        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            item = default;
            if (!_local.TryGetValue(key, out MessageItem i))
                return false;
            item = (TItem)Serializer.Deserialize(i.Data);
            return true;
        }

        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
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

                if (OnChanging(key, oldVal, val, out var remove))
                    return VectorRelationship.Greater;
                if (remove)
                {
                    _local.Remove(key);
                    var hashOld = GetHash(key, oldVal);
                    _databaseHash.Commute((ref ulong h) => h ^= hashOld);
                    return VectorRelationship.Greater;
                }
                _local[key] = new MessageItem { Key = key, Data = Serializer.Serialize(val) };
                var hash = GetHash(key, oldVal) ^ GetHash(key, val);
                _databaseHash.Commute((ref ulong h) => h ^= hash);
                return cmp;
            }
            else
            {
                if (OnChanging(key, default(TItem), val, out var remove) || remove)
                    return VectorRelationship.Greater;

                _local[key] = new MessageItem { Key = key, Data = Serializer.Serialize(val) };
                var hash = GetHash(key, val);
                _databaseHash.Commute((ref ulong h) => h ^= hash);
                return VectorRelationship.Less;
            }
        }

        public void Remove(string key)
        {
            Distributed.EnlistBackend(this);
            RemoveInternal(key);
        }

        public void RemoveInternal(string key)
        {
            if (!_local.TryGetValue(key, out var current))
                return;
            var val = (IHasVersionHash)Serializer.Deserialize(current.Data);
            if (OnChanging(key, val, null, out var _))
                return;
            _local.Remove(key);
            var hash = GetHash(key, val);
            _databaseHash.Commute((ref ulong h) => h ^= hash);
        }

        private bool OnChanging(string key, object oldVal, object newVal, out bool remove)
        {
            remove = false;
            var ch = Changing;
            if (ch == null)
                return false;
            var ev = new ChangingEventArgs(key, oldVal, newVal);
            ch.Invoke(this, ev);
            remove = ev.Remove;
            return ev.Cancel;
        }

        public event EventHandler<ChangingEventArgs> Changing;

        public void Dispose()
        {
            Transport.Dispose();
            _gossipTimer.Dispose();
            _preCommit.Dispose();
        }
    }
}
