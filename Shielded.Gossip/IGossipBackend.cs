using Shielded.Cluster;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// The basic common interface provided by both gossip backends.
    /// </summary>
    public interface IGossipBackend : IBackend
    {
        bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>;
        VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>;
    }

    public static class GossipBackendExtensions
    {
        /// <summary>
        /// Helper for types which implement <see cref="IHasVectorClock"/>, or are wrapped in a <see cref="Vc{T}"/>
        /// wrapper. The value will be readable by <see cref="TryGetMultiple{T}(IGossipBackend, string)"/> or
        /// <see cref="TryGetClocked{T}(IGossipBackend, string)"/>, respectively.
        /// </summary>
        public static VectorRelationship SetVc<TItem>(this IGossipBackend backend, string key, TItem item)
            where TItem : IHasVectorClock
        {
            return backend.Set(key, (Multiple<TItem>)item);
        }

        /// <summary>
        /// Tries to read the value(s) under the given key. Used with types that implement
        /// <see cref="IHasVectorClock"/>. In case of conflict we return all conflicting versions.
        /// If the key is not found, returns an empty Multiple.
        /// </summary>
        public static Multiple<T> TryGetMultiple<T>(this IGossipBackend backend, string key) where T : IHasVectorClock
        {
            return backend.TryGet(key, out Multiple<T> multi) ? multi : default;
        }

        /// <summary>
        /// Tries to read the value(s) under the given key. Can be used with any type - wrapped
        /// in a <see cref="Vc{T}"/> for vector clock versioning. In case of conflict we return all
        /// conflicting versions.
        /// If the key is not found, returns an empty Multiple.
        /// </summary>
        public static Multiple<Vc<T>> TryGetClocked<T>(this IGossipBackend backend, string key)
        {
            return backend.TryGet(key, out Multiple<Vc<T>> multi) ? multi : default;
        }
    }
}
