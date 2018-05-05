using Shielded.Standard;
using System.Threading.Tasks;

namespace Shielded.Cluster
{
    /// <summary>
    /// Result of preparing a consistent transaction.
    /// </summary>
    public struct PrepareResult
    {
        public readonly bool Success;
        /// <summary>
        /// If given, and if <see cref="Distributed.Consistent"/> is planning on making
        /// another attempt, it will await on this task before retrying.
        /// </summary>
        public readonly Task WaitBeforeRetry;

        public PrepareResult(bool success, Task wait = null)
        {
            Success = success;
            WaitBeforeRetry = wait;
        }
    }

    /// <summary>
    /// Interface for a backend that takes part in <see cref="Distributed"/> operations. Allows extending
    /// this library to support custom external storage, and may be used to wrap two or more backends in
    /// one class in order to better control how they interact.
    /// </summary>
    public interface IBackend
    {
        /// <summary>
        /// Called when the backend enlists in a <see cref="Distributed.Consistent"/> transaction.
        /// The backend should verify the transaction, and prepare it for commit.
        /// </summary>
        /// <param name="cont">The Shielded transaction continuation, from which you can read the
        /// changes that the transaction made, or any <see cref="ShieldedLocal{T}"/> state the
        /// backend may have created.</param>
        Task<PrepareResult> Prepare(CommitContinuation cont);

        /// <summary>
        /// Called when the backend enlists in any non-local distributed operation,
        /// <see cref="Distributed.Run"/> or <see cref="Distributed.Consistent"/>.
        /// If in consistent mode, all backends have by now successfully prepared.
        /// </summary>
        /// <param name="cont">The Shielded transaction continuation, from which you can read the
        /// changes that the transaction made, or any <see cref="ShieldedLocal{T}"/> state the
        /// backend may have created.</param>
        Task Commit(CommitContinuation cont);

        /// <summary>
        /// Called if a <see cref="Distributed.Run"/> or <see cref="Distributed.Consistent"/>
        /// operation fails.
        /// </summary>
        void Rollback();
    }
}
