using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Simple but dangerous <see cref="IMergeable{TIn, TOut}"/> implementor with Last Write Wins semantics.
    /// In non-consistent transactions it can lose writes.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct Lww<T> : IMergeable<Lww<T>, Lww<T>>, IDeletable
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public DateTimeOffset Time { get; set; }

        public bool CanDelete => Value is IDeletable del && del.CanDelete;

        public VersionHash GetVersionHash() => FNV1a64.Hash(BitConverter.GetBytes(Time.GetHashCode()));

        public Lww<T> MergeWith(Lww<T> other) => Time >= other.Time ? this : other;

        public VectorRelationship VectorCompare(Lww<T> other)
        {
            var cmp = Time.CompareTo(other.Time);
            return cmp == 0 ? VectorRelationship.Equal :
                cmp > 0 ? VectorRelationship.Greater : VectorRelationship.Less;
        }
    }

    public static class LwwExtensions
    {
        /// <summary>
        /// Last Write Wins - wraps the value with the current date and time, making it
        /// <see cref="IMergeable{TIn, TOut}"/>. In non-consistent transactions it can
        /// lose writes.
        /// </summary>
        public static Lww<T> Lww<T>(this T val, DateTimeOffset? time = null) =>
            new Lww<T> { Value = val, Time = time ?? DateTimeOffset.UtcNow };
    }
}
