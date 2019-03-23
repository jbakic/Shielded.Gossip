namespace Shielded.Gossip
{
    /// <summary>
    /// The basic common interface provided by both gossip backends.
    /// </summary>
    public interface IGossipBackend
    {
        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>;

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        FieldInfo<TItem> TryGetWithInfo<TItem>(string key) where TItem : IMergeable<TItem>;

        /// <summary>
        /// Set a value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value.
        /// </summary>
        /// <param name="expireInMs">If given, the item will expire and be removed from the storage in
        /// this many milliseconds. If not null, must be > 0.</param>
        VectorRelationship Set<TItem>(string key, TItem value, int? expireInMs = null) where TItem : IMergeable<TItem>;

        /// <summary>
        /// Remove the given key from the storage.
        /// </summary>
        bool Remove(string key);

        /// <summary>
        /// A non-update, which ensures that when your local transaction is transmitted to other servers, this
        /// field will be transmitted as well, even if you did not change its value.
        /// </summary>
        void Touch(string key);

        /// <summary>
        /// Fired after any key changes.
        /// </summary>
        ShieldedEvent<ChangedEventArgs> Changed { get; }

        /// <summary>
        /// Fired when accessing a key that has no value or has expired. Handlers can specify a value to use,
        /// which will be saved in the backend and returned to the original reader.
        /// </summary>
        ShieldedEvent<KeyMissingEventArgs> KeyMissing { get; }
    }

    public static class GossipBackendExtensions
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
