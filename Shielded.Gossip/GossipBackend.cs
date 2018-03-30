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
    public class GossipBackend : IBackend
    {
        private readonly ShieldedDict<string, object> _local = new ShieldedDict<string, object>();

        public readonly GossipProtocol Protocol;

        public GossipBackend(GossipProtocol protocol)
        {
            Protocol = protocol;
            Protocol.MessageReceived += _protocol_MessageReceived;
        }

        private static readonly MethodInfo _itemMsgMethod = typeof(GossipBackend)
            .GetMethod("HandleItemMsg", BindingFlags.Instance | BindingFlags.NonPublic);

        private void _protocol_MessageReceived(object sender, Message msg)
        {
            switch (msg)
            {
                case TransactionMessage trans:
                    Distributed.RunLocal(() =>
                    {
                        foreach (var item in trans.Items)
                        {
                            _itemMsgMethod.MakeGenericMethod(Type.GetType(item.TypeName))
                                .Invoke(this, new object[] { item });
                        }
                    });
                    break;
            }
        }

        private void HandleItemMsg<TItem>(MessageItem item) where TItem : IMergeable<TItem, TItem>
        {
            var value = Serializer.Deserialize<TItem>(item.Data);
            Set(item.Key, value);
        }

        public Task Commit(CommitContinuation cont)
        {
            // what changed?
            MessageItem[] changes = null;
            cont.InContext(() =>
                changes = _local.Changes.Select(key =>
                {
                    var val = _local[key];
                    return new MessageItem
                    {
                        Key = key,
                        TypeName = val.GetType().FullName,
                        Data = Serializer.Serialize(val),
                    };
                }).ToArray());

            if (!changes.Any())
                return Task.FromResult<object>(null);

            return Protocol.Broadcast(new TransactionMessage { Items = changes });
        }

        public void Rollback() { }

        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            item = default;
            if (!_local.TryGetValue(key, out object obj))
                return false;
            item = (TItem)obj;
            return true;
        }

        public void Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            Distributed.EnlistBackend(this);
            if (_local.TryGetValue(key, out object oldVal))
                _local[key] = ((TItem)oldVal).MergeWith(item);
            else
                _local[key] = item;
        }

        public void SetVersion<TItem>(string key, TItem item) where TItem : IHasVectorClock
        {
            Distributed.EnlistBackend(this);
            if (_local.TryGetValue(key, out object oldVal))
                _local[key] = ((Multiple<TItem>)oldVal).MergeWith(item);
            else
                _local[key] = (Multiple<TItem>)item;
        }
    }
}
