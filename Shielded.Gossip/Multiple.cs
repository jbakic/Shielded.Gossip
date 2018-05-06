using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    /// <summary>
    /// A container type which turns <see cref="IHasVectorClock"/> implementors into CRDT types. Vector
    /// clocks are used for versioning, and in case of conflict, all conflicting versions are preserved,
    /// for the user of the library to resolve the conflict in a way best suited to the use case. If the
    /// wrapped type implements <see cref="IDeletable"/>, this will become deletable when all contained
    /// versions are deletable.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct Multiple<T> : IMergeable<T, Multiple<T>>, IMergeable<Multiple<T>, Multiple<T>>, IDeletable, IEnumerable<T>
        where T : IHasVectorClock
    {
        [DataMember]
        public T[] Items { get; set; }

        public T this[int index] => Items != null ? Items[index] : throw new ArgumentOutOfRangeException();

        /// <summary>
        /// The result of merging the vector clocks of all contained versions, or a new empty clock if
        /// we're empty.
        /// </summary>
        public VectorClock MergedClock => Items?.Aggregate((VectorClock)null, (acc, n) => acc | n.Clock) ?? new VectorClock();

        /// <summary>
        /// True if the Items are empty, of if the wrapped type implements <see cref="IDeletable"/>
        /// and all the items' CanDelete return true.
        /// </summary>
        public bool CanDelete => Items == null || Items.All(i => i is IDeletable del && del.CanDelete);

        public Multiple<T> MergeWith(Multiple<T> other) =>
            new Multiple<T>
            {
                Items = Filter(SafeConcat(other.Items, Items))?.ToArray()
            };

        public VectorRelationship VectorCompare(Multiple<T> other) => MergedClock.VectorCompare(other.MergedClock);

        public Multiple<T> MergeWith(T other) =>
            new Multiple<T>
            {
                Items = Filter(other == null ? Items : SafeConcat(new[] { other }, Items)).ToArray()
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

        public static implicit operator Multiple<T>(T val) => new Multiple<T> { Items = val == null ? null : new[] { val } };

        public IEnumerator<T> GetEnumerator() => (Items ?? Enumerable.Empty<T>()).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<T>)this).GetEnumerator();
    }
}
