using Shielded.Gossip.Backend;

namespace Shielded.Gossip.Mergeables
{
    /// <summary>
    /// Implement this interface on a gossip value type to indicate to the backend
    /// that it can be safely deleted at some point.
    /// </summary>
    /// <remarks><para>NB that any fully deleted object may be revived by receiving a very
    /// old msg, for example, and you should only indicate deletability if you can
    /// take care of such revivals - maybe by keeping a record of deleted keys
    /// somewhere else, or your data type might be idempotent, guaranteeing if revived
    /// to quickly again reach the deletable state.</para>
    /// 
    /// <para>As usual, it is important that the deletable state be independent of the
    /// server on which it is checked - all of them should reach the same decision for
    /// the same value.</para></remarks>
    public interface IDeletable
    {
        /// <summary>
        /// If true, the value will be marked deletable, and with a certain delay cleaned
        /// up from the gossip backend.
        /// </summary>
        bool CanDelete { get; }
    }
}
