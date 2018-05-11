using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Simple but dangerous <see cref="IMergeable{T}"/> implementor with Last Write Wins semantics.
    /// In non-consistent transactions it can lose writes. In consistent transactions it is better to use
    /// <see cref="Versioned{T}"/>.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct Lww<T> : IMergeable<Lww<T>>, IDeletable
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public DateTimeOffset Time { get; set; }

        public bool CanDelete => Value is IDeletable del && del.CanDelete;

        public VersionHash GetVersionHash() => FNV1a64.Hash(BitConverter.GetBytes(Time.UtcDateTime.ToBinary()));

        public Lww<T> MergeWith(Lww<T> other) => Time >= other.Time ? this : other;

        public VectorRelationship VectorCompare(Lww<T> other)
        {
            var cmp = Time.CompareTo(other.Time);
            return cmp == 0 ? VectorRelationship.Equal :
                cmp > 0 ? VectorRelationship.Greater : VectorRelationship.Less;
        }

        /// <summary>
        /// Produces a new wrapper with the same value, and a Time that is definitely greater
        /// than this wrapper's time. The result will either have the current system time, or this
        /// wrapper's time plus one tick, whichever is greater.
        /// </summary>
        /// <remarks>If one server's clock is ahead, then after he makes a write, other servers
        /// would, if they just use their own current time, keep failing to write into that
        /// field. This method should be used to produce successor versions reliably.</remarks>
        public Lww<T> NextVersion()
        {
            var now = DateTimeOffset.UtcNow;
            return now > Time ? Value.Lww(now) : Value.Lww(Time.AddTicks(1));
        }
    }

    public static class LwwExtensions
    {
        /// <summary>
        /// Simple but dangerous <see cref="IMergeable{T}"/> implementor with Last Write Wins semantics.
        /// In non-consistent transactions it can lose writes. In consistent transactions it is better to use
        /// <see cref="Versioned{T}"/>.
        /// </summary>
        public static Lww<T> Lww<T>(this T val, DateTimeOffset? time = null) =>
            new Lww<T> { Value = val, Time = time ?? DateTimeOffset.UtcNow };

        /// <summary>
        /// Tries to read the <see cref="Gossip.Lww{T}"/> value under the given key. If the key is not
        /// found, returns default.
        /// </summary>
        public static Lww<T> TryGetLww<T>(this IGossipBackend backend, string key)
        {
            return backend.TryGet(key, out Lww<T> v) ? v : default;
        }
    }
}
