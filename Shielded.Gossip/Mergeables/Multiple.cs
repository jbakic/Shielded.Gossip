using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// A container type which turns <see cref="IHasVersionVector"/> implementors into CRDT types. Vector
    /// clocks are used for versioning, and in case of conflict, all conflicting versions are preserved,
    /// for the user of the library to resolve the conflict in a way best suited to the use case. If the
    /// wrapped type implements <see cref="IDeletable"/>, this will become deletable when all contained
    /// versions are deletable.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct Multiple<T> : IMergeableEx<Multiple<T>>, IDeletable, IReadOnlyCollection<T>
        where T : IHasVersionVector
    {
        [DataMember]
        public T[] Items { get; set; }

        public T this[int index] => Items != null ? Items[index] : throw new ArgumentOutOfRangeException();

        public int Count => Items?.Length ?? 0;

        /// <summary>
        /// The result of merging the vector clocks of all contained versions, or a new empty clock if
        /// we're empty.
        /// </summary>
        public VersionVector MergedClock => Items?.Aggregate((VersionVector)null, (acc, n) => acc | n.Version) ?? new VersionVector();

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

        public VectorRelationship VectorCompare(T other) => MergedClock.VectorCompare(other.Version);

        public IEnumerable<byte[]> GetVersionBytes() => MergedClock.GetVersionBytes();

        public IVersion<Multiple<T>> GetVersionOnly() => new VersionOnly { MergedClock = MergedClock };

        [DataContract(Namespace = ""), Serializable]
        public struct VersionOnly : IVersion<Multiple<T>, VersionOnly>
        {
            [DataMember]
            public VersionVector MergedClock { get; set; }

            public VectorRelationship CompareWithValue(Multiple<T> value) => (MergedClock ?? new VersionVector()).VectorCompare(value.MergedClock);

            public VersionOnly MergeWith(VersionOnly other) => new VersionOnly { MergedClock = (MergedClock ?? new VersionVector()).MergeWith(other.MergedClock) };

            public VectorRelationship VectorCompare(VersionOnly other) => (MergedClock ?? new VersionVector()).VectorCompare(other.MergedClock);
        }

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
                    var comp = r.Version.VectorCompare(v.Version);
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
