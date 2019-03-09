using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// A wrapper which adds an int Version to any type, making it work with the gossip backends.
    /// Useful as a minimal wrapper for keys which get changed in consistent transactions only.
    /// In non-consistent transactions it can lose writes or lead to servers holding different
    /// values for one key. 
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct IntVersioned<T> : IMergeable<IntVersioned<T>>, IDeletable
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public int Version { get; set; }

        public bool CanDelete => Value is IDeletable del && del.CanDelete;

        public IEnumerable<byte[]> GetVersionBytes() => new[] { BitConverter.GetBytes(Version) };

        public IntVersioned<T> MergeWith(IntVersioned<T> other) => Version >= other.Version ? this : other;

        public VectorRelationship VectorCompare(IntVersioned<T> other) => Version.VectorCompare(other.Version);

        /// <summary>
        /// Produces a new wrapper with the same (by ref!) value, but with the version moved up by one.
        /// </summary>
        public IntVersioned<T> NextVersion() => Value.Version(checked(Version + 1));
    }

    public static class VersionedExtensions
    {
        /// <summary>
        /// A wrapper which adds an int version to any type, making it work with the gossip backends.
        /// Useful as a minimal wrapper for keys which get changed in consistent transactions only.
        /// In non-consistent transactions it can lose writes or lead to servers holding different
        /// values for one key.
        /// </summary>
        public static IntVersioned<T> Version<T>(this T val, int version) =>
            new IntVersioned<T> { Value = val, Version = version };

        /// <summary>
        /// Tries to read the <see cref="IntVersioned{T}"/> value under the given key. If the key is not
        /// found, returns default.
        /// </summary>
        public static IntVersioned<T> TryGetIntVersioned<T>(this IGossipBackend backend, string key)
        {
            return backend.TryGet(key, out IntVersioned<T> v) ? v : default;
        }
    }
}
