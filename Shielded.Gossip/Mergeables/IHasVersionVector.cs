using Shielded.Gossip.Backend;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// Interface for classes that have a <see cref="VersionVector"/> property, allowing
    /// them to be wrapped in a <see cref="Multiple{T}"/> and used as values with the
    /// gossip backend.
    /// </summary>
    /// <remarks><para>
    /// Vector clock is a versioning concept used in distributed systems. Every server
    /// has their own version counter, hence "vector", and may only increment it. The
    /// type is a CRDT, the merge operation is simply taking the bigger value per server
    /// thus producing a new vector.</para>
    /// 
    /// <para>In case of conflict, the <see cref="Multiple{T}"/> will contain multiple
    /// versions of the data - the ones which were created independently of each other.
    /// The vector clock algorithm always correctly recognizes this, and the Multiple
    /// allows the library user to resolve the conflicts in a way best suited to the
    /// use case.</para></remarks>
    public interface IHasVersionVector
    {
        /// <summary>
        /// The version of this data item.
        /// </summary>
        VersionVector Version { get; set; }
    }

    public static class HasVersionVectorExtensions
    {
        /// <summary>
        /// Helper for types which implement <see cref="IHasVersionVector"/>, or are wrapped in a <see cref="VecVersioned{T}"/>
        /// wrapper. The value will be readable by <see cref="TryGetHasVec{T}(IGossipBackend, string)"/> or
        /// <see cref="VecVersionedExtensions.TryGetVecVersioned{T}(IGossipBackend, string)"/>, respectively.
        /// </summary>
        public static VectorRelationship SetHasVec<TItem>(this IGossipBackend backend, string key, TItem item, int? expireInMs = null)
            where TItem : IHasVersionVector
        {
            return backend.Set(key, (Multiple<TItem>)item, expireInMs);
        }

        /// <summary>
        /// Tries to read the value(s) under the given key. Used with types that implement
        /// <see cref="IHasVersionVector"/>. In case of conflict we return all conflicting versions.
        /// If the key is not found, returns an empty Multiple.
        /// </summary>
        public static Multiple<T> TryGetHasVec<T>(this IGossipBackend backend, string key) where T : IHasVersionVector
        {
            return backend.TryGet(key, out Multiple<T> multi) ? multi : default;
        }
    }
}
