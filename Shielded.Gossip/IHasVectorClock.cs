using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    public interface IHasVectorClock<T> : IMergeable<T, MultiVersion<T>> where T : IHasVectorClock<T>
    {
        VectorClock Clock { get; set; }
    }

    public static class HasVectorClock
    {
        public static MultiVersion<T> DefaultMerge<T>(this T left, T right) where T : IHasVectorClock<T>
        {
            return ((MultiVersion<T>)left).MergeWith(right);
        }
    }
}
