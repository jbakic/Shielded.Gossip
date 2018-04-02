using System;
using System.Collections.Generic;

namespace Shielded.Gossip
{
    [Serializable]
    public class VectorClock : VectorBase<VectorClock, int>
    {
        public VectorClock() : base() { }
        public VectorClock(params VectorItem<int>[] items) : base(items) { }
        public VectorClock(string ownServerId, int init) : base(ownServerId, init) { }

        public static implicit operator VectorClock((string, int) t) => new VectorClock(t.Item1, t.Item2);

        protected override int Merge(int left, int right) => Math.Max(left, right);

        public VectorClock Next(string ownServerId)
        {
            if (string.IsNullOrWhiteSpace(ownServerId))
                throw new ArgumentNullException();
            return Modify(ownServerId, n => checked(n + 1));
        }
    }
}
