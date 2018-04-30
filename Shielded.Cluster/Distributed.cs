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

        public static void AssertIsRunning()
        {
            if (!IsRunning)
                throw new InvalidOperationException("Operation needs to be in a Distributed transaction.");
        }

        public static bool IsRunning => Shield.IsInTransaction && _current.HasValue;
        public static bool IsConsistent => Shield.IsInTransaction && _current.HasValue && _current.Value.IsConsistent;

        private static readonly ShieldedLocal<TransactionInfo> _current = new ShieldedLocal<TransactionInfo>();

        public static async Task Run(Action act)
        {
            if (IsRunning)
            {
                act();
                return;
            }

            TransactionInfo info = null;
            try
            {
                using (var cont = Shield.RunToCommit(Timeout.Infinite, () =>
                {
                    _current.Value = info = new TransactionInfo { IsConsistent = false };
                    act();
                }))
                {
                    await Commit(cont, info);
                    cont.Commit();
                }
            }
            catch
            {
                if (info != null)
                    Rollback(info);
                throw;
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
            if (IsRunning)
            {
                act();
                return;
            }
            Shield.InTransaction(() =>
            {
                _current.Value = new TransactionInfo { IsConsistent = false };
                act();
            });
        }

        public static Task<bool> Consistent(Action act)
        {
            return Consistent(1, act);
        }

        public static async Task<bool> Consistent(int attempts, Action act)
        {
            if (attempts <= 0)
                throw new ArgumentOutOfRangeException();

            if (IsRunning)
            {
                if (!IsConsistent)
                    throw new InvalidOperationException("Consistent calls cannot nest within Run calls.");
                act();
                return true;
            }

            while (attempts --> 0)
            {
                TransactionInfo info = null;
                try
                {
                    using (var cont = Shield.RunToCommit(Timeout.Infinite, () =>
                    {
                        _current.Value = info = new TransactionInfo { IsConsistent = true };
                        act();
                    }))
                    {
                        var prepareRes = await Prepare(cont, info);
                        if (!prepareRes.Success)
                        {
                            Rollback(info);
                            cont.Rollback();
                            if (prepareRes.WaitBeforeRetry != null && attempts > 0)
                                await prepareRes.WaitBeforeRetry;
                            continue;
                        }
                        await Commit(cont, info);
                        cont.Commit();
                        return true;
                    }
                }
                catch
                {
                    if (info != null)
                        Rollback(info);
                    throw;
                }
            }
            return false;
        }

        public static Task<(bool Success, T Value)> Consistent<T>(Func<T> func)
        {
            return Consistent(1, func);
        }

        public static async Task<(bool Success, T Value)> Consistent<T>(int attempts, Func<T> func)
        {
            T res = default;
            var success = await Consistent(attempts, () => { res = func(); });
            return (success, success ? res : default);
        }

        private static async Task<PrepareResult> Prepare(CommitContinuation cont, TransactionInfo info)
        {
            var prepares = await Task.WhenAll(info.Backends.Select(b => b.Prepare(cont)));
            return new PrepareResult(prepares.All(p => p.Success),
                Task.WhenAll(prepares.Where(p => p.WaitBeforeRetry != null).Select(p => p.WaitBeforeRetry)));
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
            AssertIsRunning();
            _current.Value.Backends.Add(backend);
        }
    }
}
