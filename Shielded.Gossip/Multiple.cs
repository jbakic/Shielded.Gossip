using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    [DataContract(Namespace = "")]
    public struct Multiple<T> : IMergeable<T, Multiple<T>>, IMergeable<Multiple<T>, Multiple<T>>, IEnumerable<T>
        where T : IHasVectorClock
    {
        [DataMember]
        public T[] Versions { get; set; }

        public T this[int index] => Versions != null ? Versions[index] : throw new ArgumentOutOfRangeException();

        public VectorClock MergedClock => Versions?.Aggregate((VectorClock)null, (acc, n) => acc | n.Clock) ?? new VectorClock();


        public Multiple<T> MergeWith(Multiple<T> other) =>
            new Multiple<T>
            {
                Versions = Filter(SafeConcat(other.Versions, Versions))?.ToArray()
            };

        public VectorRelationship VectorCompare(Multiple<T> other) => MergedClock.VectorCompare(other.MergedClock);

        public Multiple<T> MergeWith(T other) =>
            new Multiple<T>
            {
                Versions = Filter(other == null ? Versions : SafeConcat(new[] { other }, Versions)).ToArray()
            };

        public VectorRelationship VectorCompare(T other) => MergedClock.VectorCompare(other.Clock);

        public VersionHash GetVersionHash() => MergedClock.GetVersionHash();

        public static Multiple<T> operator |(Multiple<T> left, Multiple<T> right) => left.MergeWith(right);

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
                    var comp = r.Clock.VectorCompare(v.Clock);
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

        public static implicit operator Multiple<T>(T val) => new Multiple<T> { Versions = val == null ? null : new[] { val } };

        public IEnumerator<T> GetEnumerator() => (Versions ?? Enumerable.Empty<T>()).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<T>)this).GetEnumerator();
    }
}
