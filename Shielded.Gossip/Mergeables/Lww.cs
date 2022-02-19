using Shielded.Gossip.Backend;
using Shielded.Gossip.Utils;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// Simple but dangerous <see cref="IMergeableEx{T}"/> implementor with Last Write Wins semantics.
    /// In non-consistent transactions it can lose writes. In consistent transactions it is better to use
    /// <see cref="IntVersioned{T}"/>.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct Lww<T> : IMergeableEx<Lww<T>>, IDeletable
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public DateTimeOffset Time { get; set; }

        public bool CanDelete => Value is IDeletable del && del.CanDelete;

        public IEnumerable<byte[]> GetVersionBytes() => new[] { SafeBitConverter.GetBytes(Time.UtcDateTime.ToBinary()) };

        public Lww<T> MergeWith(Lww<T> other) => Time >= other.Time ? this : other;

        public VectorRelationship VectorCompare(Lww<T> other) => Time.VectorCompare(other.Time);

        public IVersion<Lww<T>> GetVersionOnly() => new VersionOnly { Time = Time };

        [DataContract(Namespace = ""), Serializable]
        public struct VersionOnly : IVersion<Lww<T>, VersionOnly>
        {
            [DataMember]
            public DateTimeOffset Time { get; set; }

            public VectorRelationship CompareWithValue(Lww<T> value) => Time.VectorCompare(value.Time);

            public VersionOnly MergeWith(VersionOnly other) => Time >= other.Time ? this : other;

            public VectorRelationship VectorCompare(VersionOnly other) => Time.VectorCompare(other.Time);
        }

        /// <summary>
        /// Produces a new wrapper with the same value, and a Time that is definitely greater
        /// than this wrapper's time.
        /// </summary>
        /// <param name="time">Time of the write. If not given, will use DateTimeOffset.UtcNow.</param>
        /// <remarks>If one server's clock is ahead, then after he makes a write, other servers
        /// would, if they just use their own current time, keep failing to write into that
        /// field. This method should be used to produce successor versions reliably.</remarks>
        public Lww<T> NextVersion()
        {
            var now = DateTimeOffset.UtcNow;
            return Value.Lww(now > Time ? now : Time.AddTicks(1));
        }
    }

    public static class LwwExtensions
    {
        /// <summary>
        /// Simple but dangerous <see cref="IMergeableEx{T}"/> implementor with Last Write Wins semantics.
        /// In non-consistent transactions it can lose writes. In consistent transactions it is better to use
        /// <see cref="IntVersioned{T}"/>.
        /// </summary>
        public static Lww<T> Lww<T>(this T val, DateTimeOffset? time = null) =>
            new Lww<T> { Value = val, Time = time ?? DateTimeOffset.UtcNow };

        /// <summary>
        /// Tries to read the <see cref="Mergeables.Lww{T}"/> value under the given key. If the key is not
        /// found, returns default.
        /// </summary>
        public static Lww<T> TryGetLww<T>(this IGossipBackend backend, string key)
        {
            return backend.TryGet(key, out Lww<T> v) ? v : default;
        }
    }
}
