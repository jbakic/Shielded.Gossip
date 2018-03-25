using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.Gossip
{
    [Serializable]
    public class CountVector : VectorBase<CountVector, CountVector.Item>
    {
        public struct Item
        {
            public long Increments { get; set; }
            public long Decrements { get; set; }

            public Item(long inc, long dec)
            {
                Increments = inc;
                Decrements = dec;
            }
        }

        public CountVector() : base() { }
        public CountVector(string ownServerId, long init) : base(ownServerId, new Item { Increments = init }) { }

        public static implicit operator CountVector((string, long) t) => new CountVector(t.Item1, t.Item2);

        protected override Item Merge(Item left, Item right) =>
            new Item
            {
                Increments = Math.Max(left.Increments, right.Increments),
                Decrements = Math.Max(left.Decrements, right.Decrements),
            };

        public long Value =>
            (Items ?? Enumerable.Empty<VectorItem<Item>>())
                .Aggregate(0L, (acc, next) => acc + (next.Value.Increments - next.Value.Decrements));

        public static implicit operator long(CountVector cv) => cv.Value;

        public CountVector Increment(string ownServerId, int by = 1)
        {
            if (by < 0)
                throw new ArgumentOutOfRangeException();
            if (by == 0)
                return this;
            return Modify(ownServerId, i => new Item(i.Increments + by, i.Decrements));
        }

        public CountVector Decrement(string ownServerId, int by = 1)
        {
            if (by < 0)
                throw new ArgumentOutOfRangeException();
            if (by == 0)
                return this;
            return Modify(ownServerId, i => new Item(i.Increments, i.Decrements + by));
        }
    }
}
