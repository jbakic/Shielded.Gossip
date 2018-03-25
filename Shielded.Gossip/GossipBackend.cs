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
        private readonly GossipProtocol _protocol;

        public GossipBackend(GossipProtocol protocol)
        {
            _protocol = protocol;
            _protocol.MessageReceived += _protocol_MessageReceived;
        }

        private readonly Lazy<MethodInfo> _itemMsgMethod = new Lazy<MethodInfo>(
            () => typeof(GossipBackend).GetMethod("HandleItemMsg", BindingFlags.Instance | BindingFlags.NonPublic),
            LazyThreadSafetyMode.PublicationOnly);

        private void _protocol_MessageReceived(object sender, Message msg)
        {
            switch (msg)
            {
                case TransactionMessage trans:
                    Shield.InTransaction(() =>
                    {
                        foreach (var item in trans.Items)
                        {
                            _itemMsgMethod.Value.MakeGenericMethod(Type.GetType(item.TypeName))
                                .Invoke(this, new object[] { item });
                        }
                    });
                    break;
            }
        }

        private void HandleItemMsg<TItem>(MessageItem item)
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

            return _protocol.Broadcast(new TransactionMessage { Items = changes });
        }

        public void Rollback() { }

        public bool TryGet<TItem>(string key, out TItem item)
        {
            item = default;
            if (!_local.TryGetValue(key, out object obj))
                return false;
            item = (TItem)obj;
            return true;
        }

        public void Set<TItem>(string key, TItem item)
        {
            if (_local.TryGetValue(key, out object oldVal))
                _local[key] = ((IMergeable<TItem, object>)oldVal).MergeWith(item);
            else
                _local[key] = ((IMergeable<TItem, object>)item).Wrap();
        }

        public void Del<TItem>(string key, TItem item)
        {
        }
    }
}
