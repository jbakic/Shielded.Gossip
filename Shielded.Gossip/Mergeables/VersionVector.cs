using Shielded.Gossip.Utils;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// CRDT type for versioning data. Each server gets his own int version which they may
    /// only increment. The CRDT merge operation is simply Max.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class VersionVector : VectorBase<VersionVector, int>
    {
        public VersionVector() : base() { }
        public VersionVector(params VectorItem<int>[] items) : base(items) { }
        public VersionVector(string ownServerId, int init) : base(ownServerId, init) { }

        public static implicit operator VersionVector((string, int) t) => new VersionVector(t.Item1, t.Item2);

        protected override VectorRelationship Compare(int left, int right) => left.VectorCompare(right);

        protected override int Merge(int left, int right) => Math.Max(left, right);

        protected override IEnumerable<byte[]> GetBytes(int val)
        {
            yield return SafeBitConverter.GetBytes(val);
        }

        /// <summary>
        /// Produce next version for this server, returning the result in a new VectorClock.
        /// </summary>
        /// <param name="ownServerId">ID of the local server producing the new version.</param>
        public VersionVector Next(string ownServerId)
        {
            if (string.IsNullOrWhiteSpace(ownServerId))
                throw new ArgumentNullException();
            return Modify(ownServerId, n => checked(n + 1));
        }
    }
}
