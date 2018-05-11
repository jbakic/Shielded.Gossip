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
    public struct Versioned<T> : IMergeable<Versioned<T>>, IDeletable
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public int Version { get; set; }

        public bool CanDelete => Value is IDeletable del && del.CanDelete;

        public VersionHash GetVersionHash() => FNV1a64.Hash(BitConverter.GetBytes(Version));

        public Versioned<T> MergeWith(Versioned<T> other) => Version >= other.Version ? this : other;

        public VectorRelationship VectorCompare(Versioned<T> other)
        {
            return Version == other.Version ? VectorRelationship.Equal :
                Version > other.Version ? VectorRelationship.Greater : VectorRelationship.Less;
        }

        /// <summary>
        /// Produces a new wrapper with the same value, but with the version moved up by one.
        /// </summary>
        public Versioned<T> NextVersion() =>
            new Versioned<T> { Value = Value, Version = checked(Version + 1) };
    }

    public static class VersionedExtensions
    {
        /// <summary>
        /// A wrapper which adds an int Version to any type, making it work with the gossip backends.
        /// Useful as a minimal wrapper for keys which get changed in consistent transactions only.
        /// In non-consistent transactions it can lose writes or lead to servers holding different
        /// values for one key. 
        /// </summary>
        public static Versioned<T> Version<T>(this T val, int version = 1) =>
            new Versioned<T> { Value = val, Version = version };

        /// <summary>
        /// Tries to read the <see cref="Versioned{T}"/> value under the given key. If the key is not
        /// found, returns default.
        /// </summary>
        public static Versioned<T> TryGetVersioned<T>(this IGossipBackend backend, string key)
        {
            return backend.TryGet(key, out Versioned<T> v) ? v : default;
        }
    }
}
