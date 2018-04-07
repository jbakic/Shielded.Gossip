using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public class GossipBackend : IBackend, IDisposable
    {
        [Serializable]
        public class Transaction
        {
            public Item[] Items;
        }

        [Serializable]
        public class GossipStart
        {
            public string From;
            public ulong DatabaseHash;
        }

        [Serializable]
        public class GossipReply
        {
            public string From;
            public ulong DatabaseHash;
            public Item[] Items;
            public long WindowStart;
            public long WindowEnd;

            public long? LastWindowStart;
            public long? LastWindowEnd;
        }

        //[Serializable]
        //public class GossipEnd
        //{
        //    public string From;
        //    public bool Success;
        //}

        [Serializable]
        public class Item
        {
            public string Key;
            public byte[] Data;

            [IgnoreDataMember, NonSerialized]
            public long Freshness;

            [IgnoreDataMember]
            public object Deserialized
            {
                get
                {
                    return Serializer.Deserialize(Data);
                }
            }

            public override string ToString()
            {
                return string.Format("{0}{1}: {2}", Key, Freshness != 0 ? " at " + Freshness : "", Deserialized);
            }
        }

        private readonly ShieldedDictNc<string, Item> _local = new ShieldedDictNc<string, Item>();
        private readonly ShieldedTreeNc<long, string> _freshIndex = new ShieldedTreeNc<long, string>();
        private readonly Shielded<ulong> _databaseHash = new Shielded<ulong>();

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

            _preCommit = Shield.PreCommit(() => _local.TryGetValue("any", out Item _) || true, SyncIndexes);
        }

        private void SyncIndexes()
        {
            long newFresh = checked(_freshIndex.Descending.FirstOrDefault().Key + 1);
            foreach (var key in _local.Changes)
            {
                var oldItem = Shield.ReadOldState(() => _local.TryGetValue(key, out Item o) ? o : null);
                if (oldItem != null)
                {
                    _freshIndex.Remove(oldItem.Freshness, key);
                }
                var newItem = _local[key];
                newItem.Freshness = newFresh;
                _freshIndex.Add(newFresh, key);
            }
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
                    var server = servers.Skip(new Random().Next(servers.Count)).First();

                    Shield.SideEffect(() => Transport.Send(server,
                        new GossipStart
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
                case Transaction trans:
                    Shield.InTransaction(() =>
                        ApplyItems(trans.Items));
                    break;

                case GossipStart start:
                    Shield.InTransaction(() =>
                        SendReply(start.From, start.DatabaseHash));
                    break;

                case GossipReply reply:
                    var doNotSend = Shield.InTransaction(() =>
                    {
                        ApplyItems(reply.Items);
                        return new HashSet<string>(_local.Changes);
                    });
                    Shield.InTransaction(() =>
                        SendReply(reply.From, reply.DatabaseHash, reply, doNotSend));
                    break;

                //case GossipEnd end:
                //    break;
            }
        }

        private static readonly MethodInfo _itemMsgMethod = typeof(GossipBackend)
            .GetMethod("SetInternal", BindingFlags.Instance | BindingFlags.NonPublic);

        private void ApplyItems(Item[] items)
        {
            if (items == null)
                return;
            foreach (var item in items)
            {
                var obj = Serializer.Deserialize(item.Data);
                try
                {
                    _itemMsgMethod.MakeGenericMethod(obj.GetType())
                        .Invoke(this, new object[] { item.Key, obj });
                }
                catch (TargetInvocationException ex)
                {
                    // why, .NET, whyyy?!
                    throw ex.InnerException;
                }
            }
        }

        private void SendReply(string server, ulong hisHash, GossipReply reply = null, HashSet<string> doNotSend = null)
        {
            var ownHash = _databaseHash.Value;
            if (ownHash == hisHash)
                return;

            var toSend = YieldReplyItems(reply?.LastWindowStart, reply?.LastWindowEnd, doNotSend)
                .Take(Configuration.AntiEntropyPackageCutoff).ToArray();
            if (toSend.Length == 0)
                return;

            var windowStart = toSend[toSend.Length - 1].Freshness;
            if (reply?.LastWindowStart != null && reply.LastWindowStart < windowStart)
                windowStart = reply.LastWindowStart.Value;

            var windowEnd = _freshIndex.Descending.First().Key;

            Shield.SideEffect(() => Transport.Send(server,
                new GossipReply
                {
                    From = Transport.OwnId,
                    DatabaseHash = ownHash,
                    Items = toSend,
                    WindowStart = windowStart,
                    WindowEnd = windowEnd,
                    LastWindowStart = reply?.WindowStart,
                    LastWindowEnd = reply?.WindowEnd
                }));
        }

        private IEnumerable<Item> YieldReplyItems(long? prevWindowStart, long? prevWindowEnd, HashSet<string> doNotSend)
        {
            var startFrom = long.MaxValue;
            var result = new HashSet<string>();
            if (prevWindowEnd != null)
            {
                foreach (var kvp in _freshIndex.RangeDescending(long.MaxValue, prevWindowEnd.Value + 1))
                    if (!(doNotSend?.Contains(kvp.Value) ?? false) && result.Add(kvp.Value))
                        yield return _local[kvp.Value];
                startFrom = prevWindowStart.Value - 1;
            }
            int countDistinct = 0;
            long? prevFreshness = null;
            foreach (var kvp in _freshIndex.RangeDescending(startFrom, long.MinValue))
            {
                if (doNotSend?.Contains(kvp.Value) ?? false)
                    continue;
                if (prevFreshness == null || kvp.Key != prevFreshness.Value)
                {
                    if (countDistinct == Configuration.AntiEntropyPackageSize)
                        break;
                    countDistinct++;
                    prevFreshness = kvp.Key;
                }
                if (result.Add(kvp.Value))
                    yield return _local[kvp.Value];
            }
        }

        public Task Commit(CommitContinuation cont)
        {
            if (!Configuration.DirectMail)
                return Task.FromResult<object>(null);
            var transaction = new Transaction();
            cont.InContext(() => transaction.Items = _local.Changes.Select(key => _local[key]).ToArray());
            if (transaction.Items.Any())
                Transport.Broadcast(transaction);
            return Task.FromResult<object>(null);
        }

        public void Rollback() { }

        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException();

            item = default;
            if (!_local.TryGetValue(key, out Item i))
                return false;
            item = (TItem)Serializer.Deserialize(i.Data);
            return true;
        }

        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            Distributed.EnlistBackend(this);
            return SetInternal(key, item);
        }

        private ulong GetHash<TItem>(string key, TItem i) where TItem : IMergeable<TItem, TItem>
        {
            return FNV1a64.Hash(
                Encoding.UTF8.GetBytes(key),
                BitConverter.GetBytes(i.GetVersionHash()));
        }

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem, TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException();
            if (_local.TryGetValue(key, out Item oldItem))
            {
                var oldVal = (TItem)Serializer.Deserialize(oldItem.Data);
                var cmp = oldVal.VectorCompare(val);
                if (cmp == VectorRelationship.Greater || cmp == VectorRelationship.Equal)
                    return cmp;

                val = oldVal.MergeWith(val);
                _local[key] = new Item { Key = key, Data = Serializer.Serialize(val) };
                var hash = GetHash(key, oldVal) ^ GetHash(key, val);
                if (hash != 0UL)
                    _databaseHash.Commute((ref ulong h) => h ^= hash);
                return cmp;
            }
            else
            {
                _local[key] = new Item { Key = key, Data = Serializer.Serialize(val) };
                var hash = GetHash(key, val);
                if (hash != 0UL)
                    _databaseHash.Commute((ref ulong h) => h ^= hash);
                return VectorRelationship.Less;
            }
        }

        public VectorRelationship SetVersion<TItem>(string key, TItem item) where TItem : IHasVectorClock
        {
            return Set(key, (Multiple<TItem>)item);
        }

        public void Dispose()
        {
            Transport.Dispose();
            _gossipTimer.Dispose();
            _preCommit.Dispose();
        }
    }
}
