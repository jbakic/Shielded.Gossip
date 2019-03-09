using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Contains a value together with its metadata, like expiry and whether it was
    /// deleted from the database.
    /// </summary>
    public class FieldInfo<T> where T : IMergeable<T>
    {
        public readonly T Value;
        public readonly bool Deleted;
        /// <summary>
        /// For internal use. Set to true when the item has been excluded from the database
        /// hash, which may happen some time after its expiry time runs out.
        /// </summary>
        public readonly bool Expired;
        public readonly int? ExpiresInMs;

        public FieldInfo(T value, int? expiresInMs) : this(value, false, false, expiresInMs) { }

        public FieldInfo(T value, bool deleted, bool expired, int? expiresInMs)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            Value = value;
            Deleted = deleted || value is IDeletable del && del.CanDelete;
            Expired = expired;
            ExpiresInMs = expiresInMs;
        }

        public FieldInfo(MessageItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            Value = (T)item.Value;
            Deleted = item.Deleted || Value is IDeletable del && del.CanDelete;
            Expired = item.Expired;
            ExpiresInMs = item.ExpiresInMs;
        }

        // this is one long signature...
        internal (FieldInfo<T> Result, ComplexRelationship Relation) MergeWith(FieldInfo<T> other)
        {
            var cmp = VectorCompare(other);
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

        internal ComplexRelationship VectorCompare(FieldInfo<T> other)
        {
            if (other == null)
                throw new ArgumentNullException(nameof(other));

            var cmp = Value.VectorCompare(other.Value);
            if (cmp != VectorRelationship.Equal)
                return (ComplexRelationship)cmp;

            if (Deleted && !other.Deleted)
                return ComplexRelationship.Greater;
            if (!Deleted && other.Deleted)
                return ComplexRelationship.Less;

            if (ExpiresInMs > 0 && other.ExpiresInMs > 0)
                return ExpiresInMs.Value.CompareTo(other.ExpiresInMs.Value).AsSecondaryRelation();

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
