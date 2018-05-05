using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    /// <summary>
    /// CRDT type for versioning data. Each server gets his own int version which they may
    /// only increment. The CRDT merge operation is simply Max.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class VectorClock : VectorBase<VectorClock, int>
    {
        public VectorClock() : base() { }
        public VectorClock(params VectorItem<int>[] items) : base(items) { }
        public VectorClock(string ownServerId, int init) : base(ownServerId, init) { }

        public static implicit operator VectorClock((string, int) t) => new VectorClock(t.Item1, t.Item2);

        protected override int Merge(int left, int right) => Math.Max(left, right);

        /// <summary>
        /// Produce next version for this server, returning the result in a new VectorClock.
        /// </summary>
        /// <param name="ownServerId">ID of the local server producing the new version.</param>
        public VectorClock Next(string ownServerId)
        {
            if (string.IsNullOrWhiteSpace(ownServerId))
                throw new ArgumentNullException();
            return Modify(ownServerId, n => checked(n + 1));
        }
    }
}
