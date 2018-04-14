using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Cluster
{
    public static class Distributed
    {
        private class TransactionInfo
        {
            public bool IsConsistent;
            public HashSet<IBackend> Backends = new HashSet<IBackend>();
        }

        public static void AssertInTransaction()
        {
            if (_current == null)
                throw new InvalidOperationException("Operation needs to be in a Distributed transaction.");
        }

        public static bool IsConsistent
        {
            get
            {
                return _current != null && _current.IsConsistent;
            }
        }

        [ThreadStatic]
        private static TransactionInfo _current;

        public static async Task Run(Action act)
        {
            if (_current != null)
            {
                act();
                return;
            }

            var info = new TransactionInfo { IsConsistent = false };
            try
            {
                _current = info;
                using (var cont = Shield.RunToCommit(Timeout.Infinite, act))
                {
                    _current = null;
                    await Commit(cont, info);
                    cont.Commit();
                }
            }
            catch
            {
                Rollback(info);
                throw;
            }
            finally
            {
                _current = null;
            }
        }

        public static async Task<T> Run<T>(Func<T> func)
        {
            T res = default;
            await Run(() => { res = func(); });
            return res;
        }

        public static void RunLocal(Action act)
        {
            if (_current != null)
            {
                act();
                return;
            }

            try
            {
                _current = new TransactionInfo { IsConsistent = false };
                Shield.InTransaction(act);
            }
            finally
            {
                _current = null;
            }
        }

        public static async Task<bool> Consistent(Action act)
        {
            if (_current != null)
            {
                if (!_current.IsConsistent)
                    throw new InvalidOperationException("Consistent calls cannot nest within Run calls.");
                act();
                return true;
            }

            var info = new TransactionInfo { IsConsistent = true };
            try
            {
                _current = info;
                using (var cont = Shield.RunToCommit(Timeout.Infinite, act))
                {
                    _current = null;
                    if (!await Prepare(cont, info))
                    {
                        Rollback(info);
                        return false;
                    }
                    await Commit(cont, info);
                    cont.Commit();
                    return true;
                }
            }
            catch
            {
                Rollback(info);
                throw;
            }
            finally
            {
                _current = null;
            }
        }

        public static async Task<(bool Success, T Value)> Consistent<T>(Func<T> func)
        {
            T res = default;
            var success = await Consistent(() => { res = func(); });
            return (success, success ? res : default);
        }

        private static async Task<bool> Prepare(CommitContinuation cont, TransactionInfo info)
        {
            var bools = await Task.WhenAll(info.Backends.Select(b => b.Prepare(cont)));
            return bools.All(b => b);
        }

        private static Task Commit(CommitContinuation cont, TransactionInfo info)
        {
            return Task.WhenAll(info.Backends.Select(b => b.Commit(cont)));
        }

        private static void Rollback(TransactionInfo info)
        {
            foreach (var b in info.Backends)
                b.Rollback();
        }

        public static void EnlistBackend(IBackend backend)
        {
            AssertInTransaction();
            _current.Backends.Add(backend);
        }
    }
}
