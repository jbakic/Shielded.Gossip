using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Describes the relationship between two CRDTs.
    /// </summary>
    [Flags]
    public enum VectorRelationship
    {
        Equal = 0,
        Less = 1,
        Greater = 2,
        Conflict = Less | Greater,
    }

    internal enum ComplexRelationship
    {
        Equal = 0,
        Less = 1,
        Greater = 2,
        Conflict = Less | Greater,
        EqualButLess = Conflict + 1,
        EqualButGreater = EqualButLess + 1
    }

    public static class VectorCompareExt
    {
        /// <summary>
        /// Helper which converts the int result of ordinary comparisons into a <see cref="VectorRelationship"/>.
        /// </summary>
        public static VectorRelationship VectorCompare<TL, TR>(this TL left, TR right) where TL : IComparable<TR>
        {
            var res = left.CompareTo(right);
            if (res == 0)
                return VectorRelationship.Equal;
            if (res < 0)
                return VectorRelationship.Less;
            return VectorRelationship.Greater;
        }

        internal static VectorRelationship GetValueRelationship(this ComplexRelationship c)
        {
            if (c == ComplexRelationship.EqualButLess || c == ComplexRelationship.EqualButGreater)
                return VectorRelationship.Equal;
            return (VectorRelationship)c;
        }

        internal static ComplexRelationship AsSecondaryRelation(this int comparison)
        {
            if (comparison == 0)
                return ComplexRelationship.Equal;
            if (comparison < 0)
                return ComplexRelationship.EqualButLess;
            return ComplexRelationship.EqualButGreater;
        }
    }
}
