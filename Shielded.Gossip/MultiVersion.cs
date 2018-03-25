using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    public class MultiVersion<T> : IMergeable<T, MultiVersion<T>>, IMergeable<MultiVersion<T>, MultiVersion<T>>
        where T : IHasVectorClock<T>
    {
        public T[] Versions { get; set; }

        public VectorClock MergedClock => Versions?.Aggregate((VectorClock)null, (acc, n) => acc | n.Clock);

        public MultiVersion<T> MergeWith(MultiVersion<T> other) =>
            new MultiVersion<T>
            {
                Versions = Filter(SafeConcat(other?.Versions, Versions))?.ToArray()
            };

        public MultiVersion<T> MergeWith(T other) =>
            new MultiVersion<T>
            {
                Versions = Filter(SafeConcat(new[] { other }, Versions)).ToArray()
            };

        public static MultiVersion<T> operator |(MultiVersion<T> left, MultiVersion<T> right) =>
            left?.MergeWith(right) ?? right ?? new MultiVersion<T>();

        private static IEnumerable<T> SafeConcat(IEnumerable<T> first, IEnumerable<T> second)
        {
            if (first != null && second != null)
                return first.Concat(second);
            else if (first != null)
                return first;
            else
                return second;
        }

        private IEnumerable<T> Filter(IEnumerable<T> input)
        {
            if (input == null)
                return null;
            var res = new List<T>();
            foreach (var v in input)
            {
                var skipCurrent = false;
                res.RemoveAll(r =>
                {
                    if (skipCurrent)
                        return false;
                    var comp = r.Clock.CompareWith(v.Clock);
                    if (comp == VectorRelationship.Greater)
                    {
                        skipCurrent = true;
                        return false;
                    }
                    // this is "less or equal", removing any r smaller than v, or equal - in case we have duplicates (?)
                    return (comp | VectorRelationship.Less) == VectorRelationship.Less;
                });
                if (!skipCurrent)
                    res.Add(v);
            }
            return res;
        }

        public static implicit operator MultiVersion<T>(T val)
        {
            return new MultiVersion<T> { Versions = new[] { val } };
        }
    }
}
