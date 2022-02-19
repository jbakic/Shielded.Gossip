using Shielded.Gossip.Mergeables;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Backend
{
    /// <summary>
    /// Contains a value together with its metadata, like expiry and whether it was
    /// deleted from the database.
    /// </summary>
    public class FieldInfo
    {
        // this is virtual to avoid having to box values in the child class every time it gets constructed.
        // the child will override it and thus box only if someone asks for this.
        public virtual object ValueObject { get; }
        public bool Deleted { get; protected set; }
        /// <summary>
        /// For internal use. Set to true when the item has been excluded from the database
        /// hash, which may happen some time after its expiry time runs out.
        /// </summary>
        public bool Expired { get; protected set; }
        public int? ExpiresInMs { get; protected set; }

        protected FieldInfo() { }

        public FieldInfo(MessageItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            ValueObject = item.Value;
            Deleted = item.Deleted || ValueObject is IDeletable del && del.CanDelete;
            Expired = !Deleted && item.Expired;
            ExpiresInMs = Deleted ? null : item.ExpiresInMs;
        }

        internal FieldInfo(StoredItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            ValueObject = item.Value;
            Deleted = item.Deleted || ValueObject is IDeletable del && del.CanDelete;
            Expired = !Deleted && item.Expired;
            ExpiresInMs = Deleted ? null : item.ExpiresInMs;
        }
    }

    /// <summary>
    /// Contains a value together with its metadata, like expiry and whether it was
    /// deleted from the database.
    /// </summary>
    public sealed class FieldInfo<T> : FieldInfo where T : IMergeableEx<T>
    {
        public T Value { get; private set; }

        public override object ValueObject => Value;

        public FieldInfo(T value, int? expiresInMs) : this(value, false, false, expiresInMs) { }

        public FieldInfo(T value, bool deleted, bool expired, int? expiresInMs)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            Value = value;
            Deleted = deleted || value is IDeletable del && del.CanDelete;
            Expired = !Deleted && expired;
            ExpiresInMs = Deleted ? null : expiresInMs;
        }

        public FieldInfo(MessageItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            Value = (T)item.Value;
            Deleted = item.Deleted || Value is IDeletable del && del.CanDelete;
            Expired = !Deleted && item.Expired;
            ExpiresInMs = Deleted ? null : item.ExpiresInMs;
        }

        internal FieldInfo(StoredItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            Value = (T)item.Value;
            Deleted = item.Deleted || Value is IDeletable del && del.CanDelete;
            Expired = !Deleted && item.Expired;
            ExpiresInMs = Deleted ? null : item.ExpiresInMs;
        }

        // this is one long signature...
        internal (FieldInfo<T> Result, ComplexRelationship Relation) MergeWith(FieldInfo<T> other, int expiryComparePrecision)
        {
            var cmp = VectorCompare(other, expiryComparePrecision);
            if (cmp == ComplexRelationship.Greater || cmp == ComplexRelationship.EqualButGreater || cmp == ComplexRelationship.Equal)
                return (this, cmp);
            if (cmp == ComplexRelationship.Less || cmp == ComplexRelationship.EqualButLess)
                return (other, cmp);

            var val = Value.MergeWith(other.Value);
            if (val == null)
                throw new ApplicationException("IMergeable.MergeWith should not return null.");
            var del = Deleted && other.Deleted;
            var exp = Expired && other.Expired;
            var expInMs =
                ExpiresInMs != null && other.ExpiresInMs != null ? Math.Max(ExpiresInMs.Value, other.ExpiresInMs.Value) :
                ExpiresInMs != null ? ExpiresInMs :
                other.ExpiresInMs;
            return (new FieldInfo<T>(val, del, exp, expInMs), cmp);
        }

        internal ComplexRelationship VectorCompare(FieldInfo<T> other, int expiryComparePrecision)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));

            var cmp = Value.VectorCompare(other.Value);
            if (cmp != VectorRelationship.Equal)
                return (ComplexRelationship)cmp;

            if (Deleted && other.Deleted)
                return ComplexRelationship.Equal;
            if (Deleted && !other.Deleted)
                return ComplexRelationship.Greater;
            if (!Deleted && other.Deleted)
                return ComplexRelationship.Less;

            if (!Expired && !other.Expired)
            {
                if (Util.RoughlyEqual(ExpiresInMs, other.ExpiresInMs, expiryComparePrecision))
                    return ComplexRelationship.Equal;
                if (ExpiresInMs > 0 && other.ExpiresInMs > 0)
                    return ExpiresInMs.Value.CompareTo(other.ExpiresInMs.Value).AsSecondaryRelation();
            }

            var leftRating =
                ExpiresInMs > 0 ? 3 :
                Expired ? 2 : 1;
            var rightRating =
                other.ExpiresInMs > 0 ? 3 :
                other.Expired ? 2 : 1;
            return leftRating.CompareTo(rightRating).AsSecondaryRelation();
        }
    }
}
