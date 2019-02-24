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
        public readonly int? ExpiresInMs;

        public FieldInfo(T value, int? expiresInMs) : this(value, false, expiresInMs) { }

        public FieldInfo(T value, bool deleted, int? expiresInMs)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            Value = value;
            Deleted = deleted || expiresInMs <= 0 || value is IDeletable del && del.CanDelete;
            ExpiresInMs = Deleted ? null : expiresInMs;
        }

        public FieldInfo(MessageItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            Value = (T)item.Value;
            Deleted = item.Deleted || item.ExpiresInMs <= 0 || Value is IDeletable del && del.CanDelete;
            ExpiresInMs = Deleted ? null : item.ExpiresInMs;
        }

        public static VectorRelationship CompareWithDeletes(T leftVal, bool leftDelete, T rightVal, bool rightDelete)
        {
            var cmp = leftVal.VectorCompare(rightVal);
            if (cmp == VectorRelationship.Equal)
            {
                if (leftDelete && !rightDelete)
                    cmp = VectorRelationship.Greater;
                else if (!leftDelete && rightDelete)
                    cmp = VectorRelationship.Less;
            }
            return cmp;
        }

        public (FieldInfo<T> Result, VectorRelationship Relation) MergeWith(FieldInfo<T> other)
        {
            var cmp = VectorCompare(other);
            if (cmp == VectorRelationship.Greater || cmp == VectorRelationship.Equal)
                return (this, cmp);
            if (cmp == VectorRelationship.Less)
                return (other, cmp);

            var expire = ExpiresInMs == null || other.ExpiresInMs == null ? (int?)null :
                Math.Max(ExpiresInMs.Value, other.ExpiresInMs.Value);
            var val = Value.MergeWith(other.Value);
            var delete = Deleted && other.Deleted;
            if (val == null)
                throw new ApplicationException("IMergeable.MergeWith should not return null.");
            return (new FieldInfo<T>(val, delete, expire), cmp);
        }

        public VectorRelationship VectorCompare(FieldInfo<T> other) =>
            CompareWithDeletes(Value, Deleted, other.Value, other.Deleted);
    }
}
