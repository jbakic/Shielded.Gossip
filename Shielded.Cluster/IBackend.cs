using Shielded.Standard;
using System.Threading.Tasks;

namespace Shielded.Cluster
{
    public interface IBackend
    {
        Task<bool> Prepare(CommitContinuation cont);
        Task Commit(CommitContinuation cont);
        void Rollback();
    }
}
