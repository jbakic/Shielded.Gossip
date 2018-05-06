using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    /// <summary>
    /// A wrapper which adds a <see cref="VectorClock"/> to any type, making it work with
    /// the gossip backends.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct Vc<T> : IHasVectorClock
    {
        [DataMember]
        public T Value { get; set; }
        [DataMember]
        public VectorClock Clock { get; set; }

        /// <summary>
        /// Produces a new wrapper with the same value, but with the version moved up by one
        /// for this server.
        /// </summary>
        public Vc<T> NextVersion(string ownServerId) =>
            new Vc<T> { Value = Value, Clock = Clock?.Next(ownServerId) ?? (ownServerId, 1) };
    }

    public static class VcExtensions
    {
        /// <summary>
        /// Wraps the value in a <see cref="Vc{T}"/> with the given vector clock value.
        /// </summary>
        public static Vc<T> Clock<T>(this T val, VectorClock clock = null) =>
            new Vc<T> { Value = val, Clock = clock ?? new VectorClock() };

        /// <summary>
        /// Wraps the value in a <see cref="Vc{T}"/> with a new vector clock.
        /// </summary>
        public static Vc<T> Clock<T>(this T val, string ownServerId, int init = 1) =>
            new Vc<T> { Value = val, Clock = (ownServerId, init) };
    }
}
