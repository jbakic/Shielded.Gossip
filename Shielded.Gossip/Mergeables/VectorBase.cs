using Shielded.Gossip.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// The base type for items of a <see cref="VectorBase{TVec, T}"/> inheritor. Contains the
    /// server ID, and a generic value.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct VectorItem<T>
    {
        [DataMember]
        public string ServerId { get; set; }
        [DataMember]
        public T Value { get; set; }

        public VectorItem(string serverId, T value)
        {
            if (string.IsNullOrWhiteSpace(serverId))
                throw new ArgumentNullException(nameof(serverId));
            ServerId = serverId;
            Value = value;
        }
    }

    /// <summary>
    /// Abstract base class for CRDT vector types - vectors where each server has their own
    /// value, and which define a merge operation for these values that is
    /// idempotent, commutative and associative.
    /// </summary>
    /// <typeparam name="TVec">The type of the vector, i.e. of the child class itself.</typeparam>
    /// <typeparam name="T">The type of an individual server's value.</typeparam>
    [DataContract(Namespace = ""), Serializable]
    public abstract class VectorBase<TVec, T> : IEquatable<TVec>, IMergeableEx<TVec>, IVersion<TVec, TVec>, IReadOnlyCollection<VectorItem<T>>
        where TVec : VectorBase<TVec, T>, new()
    {
        public VectorBase() { }

        public VectorBase(params VectorItem<T>[] items)
        {
            Items = items;
        }

        public VectorBase(string ownServerId, T item) : this(new VectorItem<T>(ownServerId, item)) { }

        [DataMember]
        public VectorItem<T>[] Items { get; set; }

        public int Count => Items?.Length ?? 0;

        public bool Equals(TVec other)
        {
            return Join(other, (left, right) => Compare(left, right) == VectorRelationship.Equal).All(b => b);
        }

        public override bool Equals(object obj) => obj is TVec vec && Equals(vec);

        public override int GetHashCode()
        {
            if (Items == null || Items.Length == 0)
                return 0;
            return FNV1a32.Hash(GetVersionBytes());
        }

        public static bool operator ==(VectorBase<TVec, T> left, TVec right) => left.Equals(right);
        public static bool operator !=(VectorBase<TVec, T> left, TVec right) => !left.Equals(right);

        public IEnumerable<byte[]> GetVersionBytes()
        {
            if (Items == null || Items.Length == 0)
                yield break;
            var idComparer = StringComparer.InvariantCultureIgnoreCase;
            foreach (var item in Items.OrderBy(vi => vi.ServerId, idComparer))
            {
                yield return Encoding.UTF8.GetBytes(item.ServerId.ToLowerInvariant());
                foreach (var subItem in GetBytes(item.Value))
                    yield return subItem;
            }
        }

        /// <summary>
        /// Gets the bytes of the items, used to determine the hash of the version. Needed to make
        /// sure that the hash is equal on all servers. Do not use .NET's GetHashCode, and if you
        /// use the BitConverter, pay attention to IsLittleEndian!
        /// </summary>
        protected abstract IEnumerable<byte[]> GetBytes(T val);

        public override string ToString()
        {
            if (Items == null || Items.Length == 0)
                return "";
            return string.Join(" | ", Items.Select(i =>
                string.Format("(\"{0}\", {1})", i.ServerId, i.Value)));
        }

        protected abstract VectorRelationship Compare(T left, T right);

        public VectorRelationship VectorCompare(TVec other)
        {
            return Join(other, Compare).Aggregate(VectorRelationship.Equal, (a, b) => a | b);
        }

        public IVersion<TVec> GetVersionOnly() => this;

        public VectorRelationship CompareWithValue(TVec value) => VectorCompare(value);

        public IEnumerable<TRes> Join<TRVec, TR, TRes>(VectorBase<TRVec, TR> rightVec, Func<T, TR, TRes> resultSelector)
            where TRVec : VectorBase<TRVec, TR>, new()
        {
            return Join(rightVec, (_, l, r) => resultSelector(l, r));
        }

        public IEnumerable<TRes> Join<TRVec, TR, TRes>(VectorBase<TRVec, TR> rightVec, Func<string, T, TR, TRes> resultSelector)
            where TRVec : VectorBase<TRVec, TR>, new()
        {
            var rightEnum = rightVec?.Items ?? Enumerable.Empty<VectorItem<TR>>();
            foreach (var grp in this.Select(i => (left: true, server: i.ServerId, leftval: i.Value, rightval: default(TR)))
                .Concat(rightEnum.Select(i => (left: false, server: i.ServerId, leftval: default(T), rightval: i.Value)))
                .GroupBy(t => t.server, StringComparer.InvariantCultureIgnoreCase))
            {
                // note that this validates that the same server ID is not present multiple times in either.
                var leftval = grp.SingleOrDefault(t => t.left).leftval;
                var rightval = grp.SingleOrDefault(t => !t.left).rightval;
                var serverId = grp.Key;
                if (string.IsNullOrWhiteSpace(serverId))
                    throw new InvalidOperationException("Vector server IDs may not be empty.");

                yield return resultSelector(serverId, leftval, rightval);
            }
        }

        public TVec Modify(string ownServerId, T value)
        {
            return Modify(ownServerId, _ => value);
        }

        public TVec Modify(string ownServerId, Func<T, T> modifier)
        {
            return new TVec
            {
                Items = GetModifiedItems(ownServerId, modifier).ToArray()
            };
        }

        protected IEnumerable<VectorItem<T>> GetModifiedItems(string ownServerId, Func<T, T> modifier)
        {
            bool foundIt = false;
            foreach (var item in this)
            {
                if (string.IsNullOrWhiteSpace(item.ServerId))
                    throw new InvalidOperationException("Vector server IDs may not be empty.");
                if (item.ServerId.Equals(ownServerId, StringComparison.InvariantCultureIgnoreCase))
                {
                    if (foundIt)
                        throw new InvalidOperationException("Vector may not mention the same server multiple times.");
                    foundIt = true;
                    yield return new VectorItem<T>(ownServerId, modifier(item.Value));
                }
                else
                    yield return item;
            }
            if (!foundIt)
                yield return new VectorItem<T>(ownServerId, modifier(default));
        }

        protected abstract T Merge(T left, T right);

        public TVec MergeWith(TVec other)
        {
            return new TVec
            {
                Items = Join(other, (serverId, left, right) => new VectorItem<T>
                {
                    ServerId = serverId,
                    Value = Merge(left, right),
                }).ToArray()
            };
        }

        public static TVec operator |(VectorBase<TVec, T> first, TVec second) =>
            first?.MergeWith(second) ?? second ?? new TVec();

        public IEnumerable<VectorItem<T>> Without(string serverId)
        {
            if (string.IsNullOrWhiteSpace(serverId))
                throw new ArgumentNullException();
            return this.Where(i => !i.ServerId.Equals(serverId, StringComparison.InvariantCultureIgnoreCase));
        }

        public IEnumerator<VectorItem<T>> GetEnumerator() => (Items ?? Enumerable.Empty<VectorItem<T>>()).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<VectorItem<T>>)this).GetEnumerator();

        public T this[string serverId]
        {
            get
            {
                if (string.IsNullOrWhiteSpace(serverId))
                    throw new ArgumentNullException();
                return Items == null ? default :
                    Items.FirstOrDefault(i => i.ServerId.Equals(serverId, StringComparison.InvariantCultureIgnoreCase)).Value;
            }
        }
    }
}
