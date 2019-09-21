using System.Collections.Generic;

namespace Shielded.Gossip
{
    /// <summary>
    /// Base interface of <see cref="IMergeable{T}"/>, used for providing access to
    /// the version bytes used for the DB hash, without having to know which mergeable type
    /// we're dealing with - this is non-generic.
    /// </summary>
    public interface IHasVersionBytes
    {
        /// <summary>
        /// Gets the bytes of fields which take part in determining the version of the data. Used
        /// when determining the hash of the whole database. We will add a zero byte between every
        /// two byte arrays in the returned enumerable.
        /// </summary>
        IEnumerable<byte[]> GetVersionBytes();
    }

    /// <summary>
    /// Interface for types which have a merge operation that is idempotent, commutative and
    /// associative. Required by the gossip backend to resolve conflicts.
    /// </summary>
    public interface IMergeable<T> : IHasVersionBytes where T : IMergeable<T>
    {
        /// <summary>
        /// Merge the value with another, returning the result. Should not change the current
        /// object! The result must be <see cref="VectorRelationship.Greater"/> than or
        /// <see cref="VectorRelationship.Equal"/> to both of the merged values.
        /// </summary>
        T MergeWith(T other);

        /// <summary>
        /// Compare with the other value, returning their <see cref="VectorRelationship"/>.
        /// Comparing the result of <see cref="MergeWith(T)"/> with any of the two merged values
        /// must return Greater or Equal.
        /// </summary>
        VectorRelationship VectorCompare(T other);
    }
}