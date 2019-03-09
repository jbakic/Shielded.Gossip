namespace Shielded.Gossip
{
    /// <summary>
    /// Interface for classes that have a <see cref="VersionVector"/> property, allowing
    /// them to be wrapped in a <see cref="Multiple{T}"/> and used as values with the
    /// gossip backend.
    /// </summary>
    /// <remarks><para>
    /// Vector clock is a versioning concept used in distributed systems. Every server
    /// has their own version counter, hence "vector", and may only increment it. The
    /// type is a CRDT, the merge operation is simply taking the bigger value per server
    /// thus producing a new vector.</para>
    /// 
    /// <para>In case of conflict, the <see cref="Multiple{T}"/> will contain multiple
    /// versions of the data - the ones which were created independently of each other.
    /// The vector clock algorithm always correctly recognizes this, and the Multiple
    /// allows the library user to resolve the conflicts in a way best suited to the
    /// use case.</para></remarks>
    public interface IHasVersionVector
    {
        /// <summary>
        /// The version of this data item.
        /// </summary>
        VersionVector Version { get; set; }
    }
}
