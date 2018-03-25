using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public class GossipBackend : IBackend
    {
        private ShieldedDict<string, object> _cache = new ShieldedDict<string, object>();

        public Task Commit()
        {
            return Task.FromResult<object>(null);
        }

        public void Rollback() { }

        public bool TryGet<TItem>(string key, out TItem item)
        {
            item = default;
            if (!_cache.TryGetValue(key, out object obj))
                return false;
            item = (TItem)obj;
            return true;
        }

        public void Set<TItem>(string key, TItem item)
        {
            if (_cache.TryGetValue(key, out object oldVal))
                _cache[key] = ((IMergeable<TItem, object>)oldVal).MergeWith(item);
            else
                _cache[key] = (IMergeable<TItem, object>)item;
        }

        public void Del<TItem>(string key, TItem item)
        {
        }
    }
}
