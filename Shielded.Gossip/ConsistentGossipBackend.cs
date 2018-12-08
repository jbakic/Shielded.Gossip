using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    /// <summary>
    /// A backend based on <see cref="GossipBackend"/>, extending it with support for
    /// consistent transactions. If used in ordinary <see cref="Shield"/> transactions, it
    /// works the same as the GossipBackend - eventually consistent. To run consistent
    /// transactions use <see cref="RunConsistent(Action, int)"/> or <see cref="Prepare(Action, int)"/>.
    /// </summary>
    /// <remarks><para>The transactions are very simple - for every Set operation you perform in your
    /// transaction lambda, we will make a transaction item only if it affects the storage.
    /// The result of such a Set operation will be Greater or Conflict. We send this result too, as
    /// the expected result. If Conflict is expected, other servers will accept this Set regardless
    /// of their local result. But if Greater is expected, then they will check if your new value is
    /// also Greater on their local DBs, and fail the transaction if not. This is all that a transaction
    /// checks!</para>
    /// 
    /// <para>If eventually consistent transactions are changing the same fields in parallel, then the
    /// consistent transaction, if successful, guarantees only that on every server at some point
    /// of time in their change history this transaction checked out OK. This is important to
    /// note, because eventual changes get written in immediately, maybe before some transaction has
    /// fully completed, and can overwrite her effects before they even become visible.</para></remarks>
    public class ConsistentGossipBackend : IGossipBackend, IDisposable
    {
        private readonly GossipBackend _wrapped;

        public ITransport Transport => _wrapped.Transport;
        public GossipConfiguration Configuration => _wrapped.Configuration;
        public ShieldedLocal<string> DirectMailRestriction => _wrapped.DirectMailRestriction;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transport">The message transport to use.</param>
        /// <param name="configuration">The configuration.</param>
        public ConsistentGossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            _wrapped = new GossipBackend(transport, configuration);
            Shield.InTransaction(() =>
                _wrapped.Changed.Subscribe(_wrapped_Changed));
        }

        /// <summary>
        /// Tries to read the value under the given key. The type of the value must be a CRDT.
        /// </summary>
        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            key = WrapPublicKey(key);
            return TryGetInternal(key, out item);
        }

        private bool TryGetInternal<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            if (!IsInConsistentTransaction)
                return _wrapped.TryGet(key, out item);
            item = default;
            var local = _state.GetValueOrDefault()?.Changes;
            if (local != null && local.TryGetValue(key, out TransactionItem i))
            {
                item = (TItem)i.Value;
                return true;
            }
            return _wrapped.TryGet(key, out item);
        }

        private string WrapPublicKey(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException();
            return PublicPfx + key;
        }

        private string WrapInternalKey(string prefix, string key)
        {
            return prefix + key;
        }

        private const string PublicPfx = "|";
        private const string TransactionPfx = "transaction|";

        /// <summary>
        /// Sets the given value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value. In a consistent
        /// transaction, only the calls which return Greater or Conflict, that actually affect the
        /// storage, get transmitted to other servers. If the result was Greater, we will insist
        /// that it's Greater on all servers.
        /// </summary>
        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem>
        {
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.Set(key, item);
            return SetInternal(key, item);
        }

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            if (val == null)
                throw new ArgumentNullException("val");
            Enlist();
            var cmp = VectorRelationship.Greater;
            if (TryGetInternal(key, out TItem oldVal))
            {
                cmp = val.VectorCompare(oldVal);
                if (cmp == VectorRelationship.Less || cmp == VectorRelationship.Equal)
                    return cmp;
                val = oldVal.MergeWith(val);
            }

            var local = _state.Value.Changes;
            // cmp and Expected can both be either Greater or Conflict, and Greater & Conflict == Greater.
            var expected = local.TryGetValue(key, out var oldItem) ? oldItem.Expected & cmp : cmp;
            local[key] = new TransactionItem { Key = key, Value = val, Expected = expected };

            OnChanged(key, oldVal, val);
            return cmp;
        }

        private struct PrepareResult
        {
            public readonly bool Success;
            public readonly Task WaitBeforeRetry;

            public PrepareResult(bool success, Task wait = null)
            {
                Success = success;
                WaitBeforeRetry = wait;
            }
        }

        private class BackendState
        {
            public readonly ConsistentGossipBackend Self;
            public readonly string Initiator;
            public readonly string TransactionId;
            public readonly Dictionary<string, TransactionItem> Changes;
            public readonly TaskCompletionSource<PrepareResult> PrepareCompleter = new TaskCompletionSource<PrepareResult>();
            public readonly TaskCompletionSource<object> Committer = new TaskCompletionSource<object>();

            public BackendState(string id, string initiator, ConsistentGossipBackend self, IEnumerable<TransactionItem> changes = null)
            {
                Self = self;
                Initiator = initiator;
                TransactionId = id;
                Changes = changes?.ToDictionary(ti => ti.Key) ?? new Dictionary<string, TransactionItem>();
            }

            public void Complete(bool res)
            {
                if (!Shield.InTransaction(() => Self._transactions.Remove(TransactionId)))
                    return;
                PrepareCompleter.TrySetResult(new PrepareResult(res));
                Self.UnlockFields(this);
                Committer.TrySetResult(null);
            }
        }

        private readonly ShieldedLocal<BackendState> _state = new ShieldedLocal<BackendState>();

        private void Enlist()
        {
            if (!_state.HasValue)
            {
                // a hack to keep the transaction open.
                new Shielded<int>().Value = 1;
                _state.Value = new BackendState(WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString()), Transport.OwnId, this);
            }
        }

        private void OnChanged(string key, object oldVal, object newVal)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired during any change, allowing the change to be cancelled.
        /// </summary>
        public readonly ShieldedEvent<ChangedEventArgs> Changed = new ShieldedEvent<ChangedEventArgs>();

        private readonly ShieldedDictNc<string, BackendState> _transactions = new ShieldedDictNc<string, BackendState>();
        private readonly ShieldedDictNc<string, BackendState> _fieldBlockers = new ShieldedDictNc<string, BackendState>();

        private readonly ShieldedLocal<bool> _isInConsistentTransaction = new ShieldedLocal<bool>();

        /// <summary>
        /// True if we're in a consistent transaction.
        /// </summary>
        public bool IsInConsistentTransaction => Shield.IsInTransaction && _isInConsistentTransaction.GetValueOrDefault();

        /// <summary>
        /// Prepares a distributed consistent transaction. The task will return a continuation with
        /// which you may commit or rollback the transaction when you wish. If the task returns null,
        /// then we failed to prepare the transaction in the given number of attempts. May only be
        /// called outside of transactions!
        /// </summary>
        /// <param name="trans">The lambda to run as a distributed transaction.</param>
        /// <param name="attempts">The number of attempts to make.</param>
        /// <returns>Result of the task is a continuation for later committing/rolling back, or null if
        /// preparation failed.</returns>
        public async Task<CommitContinuation> Prepare(Action trans, int attempts = 1)
        {
            if (trans == null)
                throw new ArgumentNullException("trans");
            if (attempts <= 0)
                throw new ArgumentOutOfRangeException();
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("Prepare cannot be called within a transaction.");

            CommitContinuation cont = null;
            try
            {
                while (attempts --> 0)
                {
                    BackendState ourState = null;
                    cont = Shield.RunToCommit(Timeout.Infinite, () =>
                    {
                        _isInConsistentTransaction.Value = true;
                        trans();

                        ourState = _state.GetValueOrDefault();
                        if (ourState != null)
                            Shield.SideEffect(() => Commit(ourState), () => ourState.Complete(false));
                    });

                    if (ourState == null)
                        return cont;
                    var transaction = new TransactionInfo
                    {
                        Initiator = Transport.OwnId,
                        Items = ourState.Changes.Values.ToArray(),
                        State = new TransactionVector(
                            new[] { new VectorItem<TransactionState>(Transport.OwnId, TransactionState.Prepared) }
                            .Concat(Transport.Servers.Select(s => new VectorItem<TransactionState>(s, TransactionState.None)))
                            .ToArray()),
                    };
                    var id = ourState.TransactionId;
                    Shield.InTransaction(() => _transactions.Add(id, ourState));
                    async Task<PrepareResult> PreparationProcess()
                    {
                        var prepare = await Task.WhenAny(PrepareInternal(id, ourState, transaction), ourState.PrepareCompleter.Task);
                        if (!prepare.Result.Success)
                            return prepare.Result;
                        Shield.InTransaction(() =>
                        {
                            if (_transactions.ContainsKey(id))
                                _wrapped.Set(id, transaction);
                        });
                        return await ourState.PrepareCompleter.Task;
                    }
                    var prepTask = PreparationProcess();
                    var resTask = await Task.WhenAny(prepTask, Task.Delay(GetNewTimeout()));
                    if (resTask == prepTask && prepTask.Result.Success)
                        return cont;

                    cont.Rollback();
                    ourState.Complete(false);
                    SetFail(ourState.TransactionId);
                    if (resTask == prepTask && prepTask.Result.WaitBeforeRetry != null)
                        await prepTask.Result.WaitBeforeRetry;
                }
                return null;
            }
            catch
            {
                if (cont != null && !cont.Completed)
                    cont.TryRollback();
                throw;
            }
        }

        /// <summary>
        /// Runs a distributed consistent transaction. May be called from within another consistent
        /// transaction, but not from a non-consistent one.
        /// </summary>
        /// <param name="trans">The lambda to run as a distributed transaction.</param>
        /// <param name="attempts">The number of attempts to make.</param>
        /// <returns>Result indicates if we succeeded in the given number of attempts.</returns>
        public async Task<bool> RunConsistent(Action trans, int attempts = 1)
        {
            if (IsInConsistentTransaction)
            {
                trans();
                return true;
            }

            using (var cont = await Prepare(trans, attempts))
            {
                if (cont == null)
                    return false;
                cont.TryCommit();
                return true;
            }
        }

        /// <summary>
        /// Runs a distributed consistent transaction. May be called from within another consistent
        /// transaction, but not from a non-consistent one.
        /// </summary>
        /// <param name="trans">The lambda to run as a distributed transaction.</param>
        /// <param name="attempts">The number of attempts to make.</param>
        /// <returns>Result indicates if we succeeded in the given number of attempts, and returns
        /// the result that the lambda returned.</returns>
        public async Task<(bool Success, T Value)> RunConsistent<T>(Func<T> trans, int attempts = 1)
        {
            T res = default;
            var success = await RunConsistent(() => { res = trans(); }, attempts);
            return (success, res);
        }

        int GetNewTimeout()
        {
            var rnd = new Random();
            return rnd.Next(Configuration.ConsistentPrepareTimeoutRange.Min, Configuration.ConsistentPrepareTimeoutRange.Max);
        }

        async Task<PrepareResult> PrepareInternal(string id, BackendState ourState, TransactionInfo newInfo = null)
        {
            var lockTask = LockFields(ourState);
            if (newInfo != null)
            {
                var lockRes = await lockTask;
                return lockRes.Success ? new PrepareResult(Check(id, newInfo), null) : lockRes;
            }

            var resTask = await Task.WhenAny(lockTask, Task.Delay(Configuration.ConsistentPrepareTimeoutRange.Max));
            if (resTask != lockTask)
                return new PrepareResult(false, null);
            return lockTask.Result.Success ? new PrepareResult(Check(id), null) : lockTask.Result;
        }

        private async Task<PrepareResult> LockFields(BackendState ourState)
        {
            var prepareTask = ourState.PrepareCompleter.Task;
            var keys = ourState.Changes.Keys;
            bool success = false;
            try
            {
                while (true)
                {
                    Task waitFor = Shield.InTransaction(() =>
                    {
                        var tasks = keys.Select(key =>
                        {
                            if (!_fieldBlockers.TryGetValue(key, out var someState))
                                return (Task)null;
                            if (IsHigherPrio(ourState, someState))
                                Shield.SideEffect(() =>
                                    someState.PrepareCompleter.TrySetResult(new PrepareResult(false, ourState.Committer.Task)));
                            return someState.Committer.Task;
                        }).ToArray();

                        if (tasks.Any(t => t != null))
                            return Task.WhenAll(tasks.Where(t => t != null).Select(t => t));

                        foreach (var key in keys)
                            _fieldBlockers[key] = ourState;
                        return null;
                    });
                    if (prepareTask.IsCompleted)
                        return prepareTask.Result;
                    if (waitFor == null)
                    {
                        success = true;
                        return new PrepareResult(true);
                    }

                    if (await Task.WhenAny(prepareTask, waitFor) == prepareTask)
                        return prepareTask.Result;
                }
            }
            finally
            {
                if (!success)
                    ourState.Complete(false);
            }
        }

        private bool IsHigherPrio(BackendState left, BackendState right)
        {
            return StringComparer.InvariantCultureIgnoreCase.Compare(left.Initiator, right.Initiator) < 0;
        }

        private void UnlockFields(BackendState ourState)
        {
            Shield.InTransaction(() =>
            {
                foreach (var key in ourState.Changes.Keys)
                    if (_fieldBlockers.TryGetValue(key, out BackendState state) && state == ourState)
                        _fieldBlockers.Remove(key);
            });
        }

        private readonly ApplyMethods _compareMethods = new ApplyMethods(
            typeof(ConsistentGossipBackend).GetMethod("CheckOne", BindingFlags.NonPublic | BindingFlags.Instance));

        private VectorRelationship CheckOne<TItem>(string key, TItem item) where TItem : IMergeable<TItem>
        {
            if (!_wrapped.TryGet(key, out TItem current))
                return VectorRelationship.Greater;
            return item.VectorCompare(current);
        }

        private bool Check(string id, TransactionInfo newInfo = null)
        {
            try
            {
                return Shield.InTransaction(() =>
                {
                    var current = newInfo;
                    if (current == null && !_wrapped.TryGet(id, out current))
                        return false;
                    foreach (var item in current.Items)
                    {
                        if (item.Expected == VectorRelationship.Conflict)
                            continue;
                        var obj = item.Value;
                        var comparer = _compareMethods.Get(this, obj.GetType());
                        if (item.Expected != comparer(item.Key, obj))
                            return false;
                    }
                    return true;
                });
            }
            catch
            {
                return false;
            }
        }

        private void _wrapped_Changed(object sender, ChangedEventArgs e)
        {
            if (e.Key.StartsWith(TransactionPfx))
            {
                OnTransactionChanged(e.Key, (TransactionInfo)e.NewValue);
            }
            else if (e.Key.StartsWith(PublicPfx))
            {
                OnChanged(e.Key.Substring(PublicPfx.Length), e.OldValue, e.NewValue);
            }
        }

        private void OnTransactionChanged(string id, TransactionInfo newVal)
        {
            if (!newVal.State.Items.Any(i => StringComparer.InvariantCultureIgnoreCase.Equals(i.ServerId, Transport.OwnId)))
                return;
            if (newVal.State[Transport.OwnId] != TransactionState.None || newVal.State.IsDone || _transactions.ContainsKey(id))
            {
                OnStateChange(id, newVal);
                return;
            }
            if (StringComparer.InvariantCultureIgnoreCase.Equals(newVal.Initiator, Transport.OwnId))
            {
                Shield.SideEffect(() => SetFail(id));
                return;
            }
            var ourState = new BackendState(id, newVal.Initiator, this, newVal.Items);
            _transactions.Add(id, ourState);
            Shield.SideEffect(async () =>
            {
                try
                {
                    if (!(await Task.WhenAny(PrepareInternal(id, ourState), ourState.PrepareCompleter.Task)).Result.Success)
                    {
                        SetRejected(id);
                        ourState.Complete(false);
                        return;
                    }
                    if (!SetPrepared(id))
                    {
                        ourState.Complete(false);
                        return;
                    }
                    ourState.PrepareCompleter.TrySetResult(new PrepareResult(true));
                }
                catch
                {
                    SetRejected(id);
                    ourState.Complete(false);
                }
            });
        }

        private bool SetPrepared(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.State.IsDone)
                return false;
            _wrapped.DirectMailRestriction.Value = current.Initiator;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Prepared));
            return true;
        });

        private bool SetRejected(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.State.IsDone)
                return false;
            _wrapped.DirectMailRestriction.Value = current.Initiator;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Rejected));
            return true;
        });

        private void SetFail(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return;
            if (!StringComparer.InvariantCultureIgnoreCase.Equals(current.Initiator, Transport.OwnId))
                _wrapped.DirectMailRestriction.Value = null;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Fail));
        });

        private bool ApplyAndSetSuccess(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return false;
            if (!StringComparer.InvariantCultureIgnoreCase.Equals(current.Initiator, Transport.OwnId))
                _wrapped.DirectMailRestriction.Value = null;
            Apply(current);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Success));
            return true;
        });

        private void OnStateChange(string id, TransactionInfo current)
        {
            if ((current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return;
            _transactions.TryGetValue(id, out var ourState);
            if (current.State.IsFail || current.State.IsRejected)
            {
                Shield.SideEffect(() =>
                {
                    SetFail(id);
                    if (ourState != null)
                        ourState.Complete(false);
                });
            }
            else if (current.State.IsSuccess)
            {
                Shield.SideEffect(() =>
                {
                    ApplyAndSetSuccess(id);
                    if (ourState != null)
                        ourState.Complete(true);
                });
            }
            else if (current.State.IsPrepared &&
                StringComparer.InvariantCultureIgnoreCase.Equals(current.Initiator, Transport.OwnId))
            {
                if (ourState != null)
                    Shield.SideEffect(() =>
                        ourState.PrepareCompleter.TrySetResult(new PrepareResult(true)));
                else
                    Shield.SideEffect(() => SetFail(id));
            }
        }

        private void Apply(TransactionInfo current)
        {
            _wrapped.ApplyItems(current.Items);
        }

        private void Commit(BackendState ourState) => Shield.InTransaction(() =>
        {
            var id = ourState.TransactionId;
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                throw new ApplicationException("Critical error - unexpected commit failure.");
            Apply(current);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Success));
            Shield.SideEffect(() => ourState.Complete(true));
        });

        public void Dispose()
        {
            _wrapped.Dispose();
        }
    }
}
