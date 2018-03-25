using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    public interface IHasVectorClock<T> : IMergeable<T, Multiple<T>> where T : IHasVectorClock<T>
    {
        VectorClock Clock { get; set; }
    }

    public static class HasVectorClock
    {
        public static Multiple<T> DefaultMerge<T>(this T left, T right) where T : IHasVectorClock<T>
        {
            return ((Multiple<T>)left).MergeWith(right);
        }
    }
}
