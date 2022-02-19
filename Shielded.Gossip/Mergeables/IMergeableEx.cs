using System.Collections.Generic;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// Base interface for providing access to the version bytes used for the DB hash, without having
    /// to know which mergeable type we're dealing with - this is non-generic.
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
    /// Interface for anything that has a partial ordering. All mergeables are vector-comparable.
    /// </summary>
    public interface IVectorComparable<T>
    {
        /// <summary>
        /// Compare with the other value, returning their <see cref="VectorRelationship"/>.
        /// Comparing the result of <see cref="IMergeable{T}.MergeWith(T)"/> with any of the two merged values
        /// must return Greater or Equal.
        /// </summary>
        VectorRelationship VectorCompare(T other);
    }

    /// <summary>
    /// Interface for types which have a merge operation that is idempotent, commutative and
    /// associative. Required by the gossip backend to resolve conflicts.
    /// </summary>
    public interface IMergeable<T> : IVectorComparable<T> where T : IMergeable<T>
    {
        /// <summary>
        /// Merge the value with another, returning the result. Should not change the current
        /// object! The result must be <see cref="VectorRelationship.Greater"/> than or
        /// <see cref="VectorRelationship.Equal"/> to both of the merged values.
        /// </summary>
        T MergeWith(T other);
    }

    /// <summary>
    /// Main interface to implement to use something with the backends. Inherits <see cref="IMergeable{T}"/>,
    /// and adds <see cref="GetVersionOnly"/>.
    /// </summary>
    public interface IMergeableEx<T> : IHasVersionBytes, IMergeable<T> where T : IMergeableEx<T>
    {
        /// <summary>
        /// Returns a minimal mergeable which contains only the version information, without any payload.
        /// Any comparisons with any T must return the same result as if we were still using the full object.
        /// Must return something that implements <see cref="IVersion{T, TVersion}"/>! May return the same object.
        /// </summary>
        /// <remarks>
        /// If we were to insist here that the result implements <see cref="IVersion{T, TVersion}"/>, then IMergeableEx would need one
        /// more type param, and one more where clause. And every single place using IMergeableEx would need it as well. And C#
        /// type inference also fails to guess, if a function only has a T argument, what the TVersion should be.
        /// </remarks>
        IVersion<T> GetVersionOnly();
    }

    /// <summary>
    /// DO NOT IMPLEMENT THIS. Implement <see cref="IVersion{T, TVersion}"/>.
    /// </summary>
    public interface IVersion<T>
    {
        VectorRelationship CompareWithValue(T value);
    }

    /// <summary>
    /// Interface for results of <see cref="IMergeableEx{T}.GetVersionOnly"/>.
    /// </summary>
    /// <typeparam name="T">The main mergeable type.</typeparam>
    /// <typeparam name="TVersion">The type of this object, the version-only class.</typeparam>
    public interface IVersion<T, TVersion> : IVersion<T>, IMergeable<TVersion> where TVersion : IVersion<T, TVersion>
    {
    }
}