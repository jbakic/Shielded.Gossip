namespace Shielded.Gossip
{
    /// <summary>
    /// Interface for classes which can produce a version hash for use in determining
    /// the hash of the whole database.
    /// </summary>
    public interface IHasVersionHash
    {
        /// <summary>
        /// Calculates the version hash. Must guarantee same result on all servers.
        /// </summary>
        VersionHash GetVersionHash();
    }

    /// <summary>
    /// Interface for CRDTs - types which have a merge operation that is idempotent,
    /// commutative and associative. Such an operation allows multiple servers to eventually
    /// reach the same state regardless of the order or the number of messages which they
    /// exchange.
    /// </summary>
    /// <typeparam name="TIn">The type of the input for the merge operation, which in the
    /// general case may be different from the merge result type.</typeparam>
    /// <typeparam name="TOut">The merge result type, which typically is the same type
    /// implementing this interface.</typeparam>
    public interface IMergeable<in TIn, out TOut> : IHasVersionHash
        where TOut : IMergeable<TIn, TOut>
    {
        /// <summary>
        /// Merge the value with another, returning the result. Must be idempotent, commutative
        /// and associative.
        /// </summary>
        TOut MergeWith(TIn other);

        /// <summary>
        /// Compare with the other value, returning their <see cref="VectorRelationship"/>. Connected
        /// to <see cref="MergeWith(TIn)"/> - comparing the result of a merge with any of the two
        /// original values must return Greater or Equal.
        /// </summary>
        VectorRelationship VectorCompare(TIn other);
    }
}