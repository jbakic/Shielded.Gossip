using System.Collections.Generic;

namespace Shielded.Gossip
{
    /// <summary>
    /// Base interface of <see cref="IMergeable{TIn, TOut}"/>, used for providing access to
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
    /// Interface for CRDTs - types which have a merge operation that is idempotent,
    /// commutative and associative. Such an operation allows multiple servers to eventually
    /// reach the same state regardless of the order or the number of messages which they
    /// exchange.
    /// </summary>
    public interface IMergeable<T> : IHasVersionBytes where T : IMergeable<T>
    {
        /// <summary>
        /// Merge the value with another, returning the result. Must be idempotent, commutative
        /// and associative. Should not change the current object!
        /// </summary>
        T MergeWith(T other);

        /// <summary>
        /// Compare with the other value, returning their <see cref="VectorRelationship"/>. Connected
        /// to <see cref="MergeWith(T)"/> - comparing the result of a merge with any of the two
        /// original values must return Greater or Equal.
        /// </summary>
        VectorRelationship VectorCompare(T other);
    }
}