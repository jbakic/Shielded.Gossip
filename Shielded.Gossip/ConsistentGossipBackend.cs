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
    /// <remarks>The consistent transactions make a note of all reads and writes, and make
    /// sure that your transaction is fully consistent, even when you only do reads. The
    /// check involves making sure that the versions of all read data are Greater or Equal
    /// to the versions available on other servers, and that all you written versions are
    /// Greater than the versions on other servers.</remarks>
    public class ConsistentGossipBackend : IGossipBackend, IDisposable
    {
        private readonly GossipBackend _wrapped;

        public ITransport Transport => _wrapped.Transport;
        public GossipConfiguration Configuration => _wrapped.Configuration;

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
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            key = WrapPublicKey(key);
            return TryGetInternal(key, out item);
        }

        private bool TryGetInternal<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            if (!IsInConsistentTransaction)
                return _wrapped.TryGet(key, out item);
            var local = _currentState.Value;
            if (local.TryGetValue(key, out MessageItem i))
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
            => Shield.InTransaction(() =>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            if (item == null)
                throw new ArgumentNullException("val");
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.Set(key, item);
            return SetInternal(key, item);
        });

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem>
        {
            var cmp = VectorRelationship.Greater;
            if (TryGetInternal(key, out TItem oldVal))
            {
                cmp = val.VectorCompare(oldVal);
                if (cmp == VectorRelationship.Less || cmp == VectorRelationship.Equal)
                    return cmp;
                val = oldVal.MergeWith(val);
                if (val == null)
                    throw new ApplicationException("IMergeable.MergeWith should not return null.");
            }

            var local = _currentState.Value;
            local[key] = new MessageItem { Key = key, Value = val };

            OnChanged(key, oldVal, val);
            return cmp;
        }

        /// <summary>
        /// A non-update, which ensures that when your local transaction is transmitted to other servers, this
        /// field will be transmitted as well, even if you did not change its value.
        /// </summary>
        /// <param name="key">The key to touch.</param>
        public void Touch(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            key = WrapPublicKey(key);
            if (IsInConsistentTransaction)
                // we just need to read the field, because a consistent transaction transmits reads too.
                _wrapped.GetItem(key);
            else
                _wrapped.Touch(key);
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
            public readonly string[] AllKeys;
            public readonly TaskCompletionSource<PrepareResult> PrepareCompleter = new TaskCompletionSource<PrepareResult>();
            public readonly TaskCompletionSource<object> Committer = new TaskCompletionSource<object>();

            public BackendState(string id, string initiator, ConsistentGossipBackend self, string[] allKeys)
            {
                Self = self;
                Initiator = initiator;
                TransactionId = id;
                AllKeys = allKeys;
            }

            public void Complete(bool res) => Shield.InTransaction(() =>
            {
                if (!Self._transactions.Remove(TransactionId))
                    return;
                Self.UnlockFields(this);
                Shield.SideEffect(() =>
                {
                    PrepareCompleter.TrySetResult(new PrepareResult(res));
                    Committer.TrySetResult(null);
                });
            });
        }

        private readonly ShieldedLocal<Dictionary<string, MessageItem>> _currentState = new ShieldedLocal<Dictionary<string, MessageItem>>();

        private void OnChanged(string key, object oldVal, object newVal)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired after any key changes.
        /// </summary>
        public readonly ShieldedEvent<ChangedEventArgs> Changed = new ShieldedEvent<ChangedEventArgs>();

        private readonly ShieldedDictNc<string, BackendState> _transactions = new ShieldedDictNc<string, BackendState>();
        private readonly ShieldedDictNc<string, BackendState> _fieldBlockers = new ShieldedDictNc<string, BackendState>();

        /// <summary>
        /// True if we're in a consistent transaction.
        /// </summary>
        public bool IsInConsistentTransaction => Shield.IsInTransaction && _currentState.HasValue;

        /// <summary>
        /// Prepares a distributed consistent transaction. The task will return a continuation with
        /// which you may commit or rollback the transaction when you wish. If the task returns null,
        /// then we failed to prepare the transaction in the given number of attempts. May only be
        /// called outside of transactions!
        /// </summary>
        /// <param name="trans">The lambda to run as a distributed transaction.</param>
        /// <param name="attempts">The number of attempts to make, default 10.</param>
        /// <returns>Result of the task is a continuation for later committing/rolling back, or null if
        /// preparation failed.</returns>
        public async Task<CommitContinuation> Prepare(Action trans, int attempts = 10)
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
                    TransactionInfo transaction = null;
                    cont = Shield.RunToCommit(Timeout.Infinite, () =>
                    {
                        var ourChanges = _currentState.Value = new Dictionary<string, MessageItem>();
                        trans();

                        var ourReads = _wrapped.Reads;
                        if (ourChanges.Any() || ourReads.Any())
                        {
                            transaction = new TransactionInfo
                            {
                                Initiator = Transport.OwnId,
                                Reads = ourReads
                                    .Where(key => !ourChanges.ContainsKey(key))
                                    .Select(key => _wrapped.GetItem(key) ?? new MessageItem { Key = key })
                                    .ToArray(),
                                Changes = ourChanges.Values.ToArray(),
                                State = new TransactionVector(
                                    new[] { new VectorItem<TransactionState>(Transport.OwnId, TransactionState.Prepared) }
                                    .Concat(Transport.Servers.Select(s => new VectorItem<TransactionState>(s, TransactionState.None)))
                                    .ToArray()),
                            };
                            ourState = new BackendState(WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString()),
                                Transport.OwnId, this, transaction.AllKeys.ToArray());

                            Shield.SideEffect(() => Commit(ourState), () => Fail(ourState));
                        }
                    });

                    if (ourState == null)
                        return cont;
                    var id = ourState.TransactionId;
                    Shield.InTransaction(() => _transactions.Add(id, ourState));
                    async Task<PrepareResult> PreparationProcess()
                    {
                        var prepare = await PrepareInternal(ourState, transaction, true).ConfigureAwait(false);
                        if (!prepare.Success)
                            return prepare;
                        Shield.InTransaction(() =>
                        {
                            if (_transactions.ContainsKey(id))
                                _wrapped.Set(id, transaction);
                        });
                        return await ourState.PrepareCompleter.Task.ConfigureAwait(false);
                    }
                    var prepTask = PreparationProcess();
                    var resTask = await Task.WhenAny(prepTask, Task.Delay(GetNewTimeout()));
                    if (resTask == prepTask && prepTask.Result.Success)
                        return cont;

                    cont.Rollback();
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
        /// <param name="attempts">The number of attempts to make, default 10. If nested in another consistent
        /// transaction, this argument is ignored.</param>
        /// <returns>Result indicates if we succeeded in the given number of attempts.</returns>
        public async Task<bool> RunConsistent(Action trans, int attempts = 10)
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
            return (success, success ? res : default);
        }

        int GetNewTimeout()
        {
            var rnd = new Random();
            return rnd.Next(Configuration.ConsistentPrepareTimeoutRange.Min, Configuration.ConsistentPrepareTimeoutRange.Max);
        }

        async Task<PrepareResult> PrepareInternal(BackendState ourState, TransactionInfo transaction, bool initiatedLocally)
        {
            var lockRes = await LockFields(ourState);
            return lockRes.Success ? new PrepareResult(Check(ourState.TransactionId, transaction, initiatedLocally), null) : lockRes;
        }

        private async Task<PrepareResult> LockFields(BackendState ourState)
        {
            var prepareTask = ourState.PrepareCompleter.Task;
            var keys = ourState.AllKeys;
            while (true)
            {
                Task waitFor = Shield.InTransaction(() =>
                {
                    if (!_transactions.ContainsKey(ourState.TransactionId))
                        return prepareTask;
                    var tasks = keys.Select(key =>
                    {
                        if (!_fieldBlockers.TryGetValue(key, out var someState))
                            return (Task)null;
                        if (IsHigherPrio(ourState, someState))
                            Shield.SideEffect(() =>
                                someState.PrepareCompleter.TrySetResult(new PrepareResult(false, ourState.Committer.Task)));
                        return someState.Committer.Task;
                    }).Where(t => t != null).ToArray();

                    if (tasks.Any())
                        return Task.WhenAll(tasks);

                    foreach (var key in keys)
                        _fieldBlockers[key] = ourState;
                    return null;
                });
                if (waitFor == null)
                    return new PrepareResult(true);
                if (waitFor == prepareTask)
                    return new PrepareResult(false);

                if (await Task.WhenAny(prepareTask, waitFor) == prepareTask)
                    return prepareTask.Result;
            }
        }

        private bool IsHigherPrio(BackendState left, BackendState right)
        {
            return StringComparer.InvariantCultureIgnoreCase.Compare(left.Initiator, right.Initiator) < 0;
        }

        private void UnlockFields(BackendState ourState) => Shield.InTransaction(() =>
        {
            foreach (var key in ourState.AllKeys)
                if (_fieldBlockers.TryGetValue(key, out BackendState state) && state == ourState)
                    _fieldBlockers.Remove(key);
        });

        private readonly ApplyMethods _compareMethods = new ApplyMethods(
            typeof(ConsistentGossipBackend).GetMethod("CheckOne", BindingFlags.NonPublic | BindingFlags.Instance));

        private VectorRelationship CheckOne<TItem>(string key, TItem item) where TItem : IMergeable<TItem>
        {
            if (!_wrapped.TryGet(key, out TItem current))
                return VectorRelationship.Greater;
            return item.VectorCompare(current);
        }

        private bool Check(string id, TransactionInfo transaction, bool initiatedLocally) => Shield.InTransaction(() =>
        {
            bool result = true;

            if (transaction.Reads != null)
                foreach (var read in transaction.Reads)
                {
                    var obj = read.Value;
                    if (obj == null)
                    {
                        if (_wrapped.GetItem(read.Key) == null)
                            continue;
                        if (initiatedLocally)
                            return false;
                        _wrapped.Touch(read.Key);
                        result = false;
                    }
                    else
                    {
                        var comparer = _compareMethods.Get(this, obj.GetType());
                        // what you read must be Greater or Equal to what we have.
                        if ((comparer(read.Key, obj) | VectorRelationship.Greater) != VectorRelationship.Greater)
                        {
                            if (initiatedLocally)
                                return false;
                            _wrapped.Touch(read.Key);
                            result = false;
                        }
                    }
                }

            if (transaction.Changes != null)
                foreach (var change in transaction.Changes)
                {
                    var obj = change.Value;
                    var comparer = _compareMethods.Get(this, obj.GetType());
                    if (comparer(change.Key, obj) != VectorRelationship.Greater)
                    {
                        if (initiatedLocally)
                            return false;
                        _wrapped.Touch(change.Key);
                        result = false;
                    }
                }

            if (!initiatedLocally)
            {
                if (result)
                    SetPrepared(id);
                else
                    SetRejected(id);
            }
            return result;
        });

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
            {
                if (newVal.State.IsSuccess)
                    Apply(newVal);
                return;
            }
            if (newVal.State[Transport.OwnId] != TransactionState.None || newVal.State.IsDone || _transactions.ContainsKey(id))
            {
                OnStateChange(id, newVal);
                return;
            }
            if (StringComparer.InvariantCultureIgnoreCase.Equals(newVal.Initiator, Transport.OwnId))
            {
                SetRejected(id);
                return;
            }
            var ourState = new BackendState(id, newVal.Initiator, this, newVal.AllKeys.ToArray());
            _transactions.Add(id, ourState);
            Shield.SideEffect(async () =>
            {
                try
                {
                    var prepareTask = PrepareInternal(ourState, newVal, false);
                    if (await Task.WhenAny(
                            prepareTask,
                            ourState.PrepareCompleter.Task,
                            Task.Delay(Configuration.ConsistentPrepareTimeoutRange.Max)) != prepareTask ||
                        !prepareTask.Result.Success)
                    {
                        SetRejected(id);
                    }
                }
                catch
                {
                    SetRejected(id);
                }
            });
        }

        private bool SetPrepared(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.State.IsDone)
                return false;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Prepared));
            return true;
        });

        private bool SetRejected(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.State.IsDone)
                return false;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Rejected));
            return true;
        });

        private void SetFail(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Fail));
        });

        private bool ApplyAndSetSuccess(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return false;
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
                SetFail(id);
                if (ourState != null)
                    ourState.Complete(false);
            }
            else if (current.State.IsSuccess)
            {
                ApplyAndSetSuccess(id);
                if (ourState != null)
                    ourState.Complete(true);
            }
            else if (current.State.IsPrepared &&
                StringComparer.InvariantCultureIgnoreCase.Equals(current.Initiator, Transport.OwnId))
            {
                if (ourState != null)
                    Shield.SideEffect(() =>
                        ourState.PrepareCompleter.TrySetResult(new PrepareResult(true)));
                else
                    SetFail(id);
            }
        }

        private void Apply(TransactionInfo current)
        {
            _wrapped.ApplyItems(
                (current.Reads ?? Enumerable.Empty<MessageItem>())
                .Concat(current.Changes ?? Enumerable.Empty<MessageItem>())
                .ToArray(), false);
        }

        private void Commit(BackendState ourState) => Shield.InTransaction(() =>
        {
            var id = ourState.TransactionId;
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                throw new ApplicationException("Critical error - unexpected commit failure.");
            Apply(current);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Success));
            ourState.Complete(true);
        });

        private void Fail(BackendState ourState) => Shield.InTransaction(() =>
        {
            SetFail(ourState.TransactionId);
            ourState.Complete(false);
        });

        public void Dispose()
        {
            _wrapped.Dispose();
        }
    }
}
