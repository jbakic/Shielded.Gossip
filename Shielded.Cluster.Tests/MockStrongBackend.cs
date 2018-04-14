using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Cluster.Tests
{
    class MockStrongBackend : IBackend
    {
        private static int _commitCounter;

        public Task<bool> Prepare(CommitContinuation cont) => Task.FromResult(true);

        public Task Commit(CommitContinuation cont)
        {
            TaskCompletionSource<object> tcs = new TaskCompletionSource<object>();
            ThreadPool.QueueUserWorkItem(_ =>
            {
                Thread.Sleep(20);
                Interlocked.Increment(ref _commitCounter);
                tcs.SetResult(null);
            });
            return tcs.Task;
        }

        public static bool ConfirmCommits(int expected = 1)
        {
            return Interlocked.Exchange(ref _commitCounter, 0) == expected;
        }

        private static ShieldedDictNc<string, object> _dict = new ShieldedDictNc<string, object>();

        public bool TryGet<TItem>(string key, out TItem item)
        {
            item = default(TItem);
            if (!_dict.TryGetValue(key, out object obj))
                return false;
            item = (TItem)obj;
            return true;
        }

        public void Set<TItem>(string key, TItem item)
        {
            Distributed.EnlistBackend(this);
            _dict[key] = item;
        }

        public void Rollback() { }
    }
}
