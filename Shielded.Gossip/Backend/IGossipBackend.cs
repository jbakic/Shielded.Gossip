using Shielded.Gossip.Mergeables;
using System;
using System.Collections.Generic;

namespace Shielded.Gossip.Backend
{
    /// <summary>
    /// The basic common interface provided by both gossip backends.
    /// </summary>
    public interface IGossipBackend : IDisposable
    {
        /// <summary>
        /// Returns true if the backend contains a (non-deleted and non-expired) value under the key.
        /// </summary>
        bool ContainsKey(string key);

        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        bool TryGet<T>(string key, out T item) where T : IMergeableEx<T>;

        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        bool TryGet(string key, out object item);

        /// <summary>
        /// Returns true if the backend contains a value under the key, including any expired or deleted value
        /// that still lingers.
        /// </summary>
        bool ContainsKeyWithInfo(string key);

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        FieldInfo<T> TryGetWithInfo<T>(string key) where T : IMergeableEx<T>;

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        FieldInfo TryGetWithInfo(string key);

        /// <summary>
        /// Gets all (non-deleted and non-expired) keys contained in the backend.
        /// </summary>
        ICollection<string> Keys { get; }

        /// <summary>
        /// Gets all keys contained in the backend, including deleted and expired keys that still linger.
        /// </summary>
        ICollection<string> KeysWithInfo { get; }

        /// <summary>
        /// Set a value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value.
        /// </summary>
        /// <param name="expireInMs">If given, the item will expire and be removed from the storage in
        /// this many milliseconds. If not null, must be > 0.</param>
        VectorRelationship Set<T>(string key, T value, int? expireInMs = null) where T : IMergeableEx<T>;

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
        /// Fired after any key changes. Please note that it also fires during processing of incoming gossip
        /// messages, so, unless you really need to, don't do anything slow here.
        /// </summary>
        ShieldedEvent<ChangedEventArgs> Changed { get; }

        /// <summary>
        /// An enumerable of keys read or written into by the current transaction. Includes
        /// keys that did not have a value.
        /// </summary>
        IEnumerable<string> Reads { get; }

        /// <summary>
        /// An enumerable of keys written into by the current transaction.
        /// </summary>
        IEnumerable<string> Changes { get; }
    }
}
