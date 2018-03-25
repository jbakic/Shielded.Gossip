using System.Threading.Tasks;

namespace Shielded.Cluster
{
    public interface IBackend
    {
        bool TryGet<TItem>(string key, out TItem item);
        void Set<TItem>(string key, TItem item);
        void Del<TItem>(string key, TItem item);
        Task Commit();
        void Rollback();
    }
}
