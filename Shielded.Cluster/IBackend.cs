using Shielded.Standard;
using System.Threading.Tasks;

namespace Shielded.Cluster
{
    public struct PrepareResult
    {
        public readonly bool Success;
        public readonly Task WaitBeforeRetry;

        public PrepareResult(bool success, Task wait = null)
        {
            Success = success;
            WaitBeforeRetry = wait;
        }
    }

    public interface IBackend
    {
        Task<PrepareResult> Prepare(CommitContinuation cont);
        Task Commit(CommitContinuation cont);
        void Rollback();
    }
}
