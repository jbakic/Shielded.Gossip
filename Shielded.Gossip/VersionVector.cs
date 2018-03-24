using System;

namespace Shielded.Gossip
{
    public class VersionVector : VectorBase<VersionVector, int>
    {
        public VersionVector() : base() { }
        public VersionVector(params VectorItem<int>[] items) : base(items) { }
        public VersionVector(string ownServerId, int init) : base(ownServerId, init) { }

        protected override int Merge(int left, int right) => Math.Max(left, right);

        public VersionVector Next(string ownServerId)
        {
            if (string.IsNullOrWhiteSpace(ownServerId))
                throw new ArgumentNullException();
            checked
            {
                return Modify(ownServerId, n => n + 1);
            }
        }

        public new VectorRelationship CompareWith(VersionVector other) => base.CompareWith(other);
    }
}
