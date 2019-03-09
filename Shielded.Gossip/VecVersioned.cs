using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    /// <summary>
    /// A wrapper which adds a <see cref="VersionVector"/> to any type, making it work with
    /// the gossip backends.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct VecVersioned<T> : IHasVersionVector, IDeletable
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public VersionVector Version { get; set; }

        public bool CanDelete => Value is IDeletable del && del.CanDelete;

        /// <summary>
        /// Produces a new wrapper with the same value, but with the version moved up by one
        /// for this server.
        /// </summary>
        public VecVersioned<T> NextVersion(string ownServerId) => Value.Version(Version?.Next(ownServerId) ?? (ownServerId, 1));
    }

    public static class VecVersionedExtensions
    {
        /// <summary>
        /// Wraps the value in a <see cref="Gossip.VecVersioned{T}"/> with the given version vector.
        /// </summary>
        public static VecVersioned<T> Version<T>(this T val, VersionVector version = null) =>
            new VecVersioned<T> { Value = val, Version = version ?? new VersionVector() };

        /// <summary>
        /// Wraps the value in a <see cref="Gossip.VecVersioned{T}"/> with a new version vector. To produce new
        /// versions for existing items, use <see cref="VecVersioned{T}.NextVersion(string)"/>.
        /// </summary>
        public static VecVersioned<T> Version<T>(this T val, string ownServerId, int init = 1) =>
            new VecVersioned<T> { Value = val, Version = (ownServerId, init) };

        /// <summary>
        /// Tries to read the value(s) under the given key. Can be used with any type - wrapped
        /// in a <see cref="Gossip.VecVersioned{T}"/> to add a version vector. In case of conflict we return all
        /// conflicting versions. If the key is not found, returns an empty Multiple.
        /// </summary>
        public static Multiple<VecVersioned<T>> TryGetVecVersioned<T>(this IGossipBackend backend, string key)
        {
            return backend.TryGet(key, out Multiple<VecVersioned<T>> multi) ? multi : default;
        }
    }
}
