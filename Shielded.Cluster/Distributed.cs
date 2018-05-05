using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Cluster
{
    /// <summary>
    /// Static class with methods for running distributed operations. It deals only with
    /// coordinating the backends used, the backends themselves should provide methods
    /// for actually reading and writing data, in any way that is best suited for them.
    /// </summary>
    public static class Distributed
    {
        private class TransactionInfo
        {
            public bool IsConsistent;
            public HashSet<IBackend> Backends = new HashSet<IBackend>();
        }

        /// <summary>
        /// If not <see cref="IsRunning"/>, throws InvalidOperationException.
        /// </summary>
        public static void AssertIsRunning()
        {
            if (!IsRunning)
                throw new InvalidOperationException("Operation needs to be in a Distributed transaction.");
        }

        /// <summary>
        /// True if inside a distributed transaction, both for <see cref="Run"/> and <see cref="Consistent"/>.
        /// </summary>
        public static bool IsRunning => Shield.IsInTransaction && _current.HasValue;

        /// <summary>
        /// True if inside a <see cref="Consistent"/> distributed transaction.
        /// </summary>
        public static bool IsConsistent => Shield.IsInTransaction && _current.HasValue && _current.Value.IsConsistent;

        private static readonly ShieldedLocal<TransactionInfo> _current = new ShieldedLocal<TransactionInfo>();

        /// <summary>
        /// Runs a non-consistent distributed transaction. The lambda is executed in a Shielded
        /// transaction.
        /// WARNING: Does not nest! Awaiting on this inside a running transaction might switch
        /// you to another thread, and Shielded does not support that.
        /// </summary>
        /// <param name="act">The operation to run.</param>
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

        /// <summary>
        /// Runs a non-consistent distributed transaction. The lambda is executed in a Shielded
        /// transaction.
        /// WARNING: Does not nest! Awaiting on this inside a running transaction might switch
        /// you to another thread, and Shielded does not support that.
        /// </summary>
        /// <param name="act">The operation to run.</param>
        public static async Task<T> Run<T>(Func<T> func)
        {
            T res = default;
            await Run(() => { res = func(); });
            return res;
        }

        /// <summary>
        /// Like <see cref="Run"/>, but does not perform any commit calls on backends,
        /// it changes only the local state of the backends.
        /// </summary>
        /// <param name="act">The operation to run.</param>
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

        /// <summary>
        /// Runs a consistent distributed transaction. The lambda is executed in a Shielded
        /// transaction. Makes only one attempt. 
        /// WARNING: Does not nest! Awaiting on this inside a running transaction might switch
        /// you to another thread, and Shielded does not support that.
        /// </summary>
        /// <param name="act">The operation to run.</param>
        /// <returns>True if we succeeded to perform the transaction.</returns>
        public static Task<bool> Consistent(Action act)
        {
            return Consistent(1, act);
        }

        /// <summary>
        /// Runs a consistent distributed transaction. The lambda is executed in a Shielded
        /// transaction.
        /// WARNING: Does not nest! Awaiting on this inside a running transaction might switch
        /// you to another thread, and Shielded does not support that.
        /// </summary>
        /// <param name="attempts">The max number of attempts to perform before failing.</param>
        /// <param name="act">The operation to run.</param>
        /// <returns>True if we succeeded to perform the transaction within the given
        /// number of attempts.</returns>
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

        /// <summary>
        /// Runs a consistent distributed transaction. The lambda is executed in a Shielded
        /// transaction. Makes only one attempt.
        /// WARNING: Does not nest! Awaiting on this inside a running transaction might switch
        /// you to another thread, and Shielded does not support that.
        /// </summary>
        /// <param name="func">The operation to run.</param>
        /// <returns>True if we succeeded to perform the transaction, and if so, also returns
        /// the result of the lambda.</returns>
        public static Task<(bool Success, T Value)> Consistent<T>(Func<T> func)
        {
            return Consistent(1, func);
        }

        /// <summary>
        /// Runs a consistent distributed transaction. The lambda is executed in a Shielded
        /// transaction.
        /// WARNING: Does not nest! Awaiting on this inside a running transaction might switch
        /// you to another thread, and Shielded does not support that.
        /// </summary>
        /// <param name="attempts">The max number of attempts to perform before failing.</param>
        /// <param name="func">The operation to run.</param>
        /// <returns>True if we succeeded to perform the transaction within the given
        /// number of attempts, and if so, also returns the result of the lambda.</returns>
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

        /// <summary>
        /// To be used by <see cref="IBackend"/> implementors, when they wish to enlist
        /// in the current transaction. Throws if we're not in one.
        /// </summary>
        /// <param name="backend">The backend to enlist.</param>
        public static void EnlistBackend(IBackend backend)
        {
            AssertIsRunning();
            _current.Value.Backends.Add(backend);
        }
    }
}
