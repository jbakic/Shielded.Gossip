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
        /// Event raised when an unexpected error occurs on one of the background tasks of the backend.
        /// </summary>
        public event EventHandler<GossipBackendException> Error
        {
            add { _wrapped.Error += value; }
            remove { _wrapped.Error -= value; }
        }

        /// <summary>
        /// Yields the IDs of servers that decide consistent transactions. If set to null, the backend
        /// will use all servers visible to the transport and himself. This will be enumerated in every
        /// transaction, so it may, if you wish, introduce further reads/writes into each transaction...
        /// </summary>
        public IEnumerable<string> TransactionParticipants
        {
            get => _transactionParticipants.Value;
            set => Shield.InTransaction(() => _transactionParticipants.Value = value);
        }
        private readonly Shielded<IEnumerable<string>> _transactionParticipants = new Shielded<IEnumerable<string>>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transport">The message transport to use.</param>
        /// <param name="configuration">The configuration.</param>
        public ConsistentGossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            _compareMethods = new ApplyMethods(this,
                typeof(ConsistentGossipBackend).GetMethod("CheckOne", BindingFlags.NonPublic | BindingFlags.Instance));
            _wrapped = new GossipBackend(transport, configuration, this);
            Shield.InTransaction(() =>
                _wrapped.Changed.Subscribe(_wrapped_Changed));
        }

        /// <summary>
        /// Returns true if the backend contains a (non-deleted and non-expired) value under the key.
        /// </summary>
        public bool ContainsKey(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.ContainsKey(key);
            var local = _currentState.Value;
            if (!local.TryGetValue(key, out MessageItem i))
                return _wrapped.ContainsKey(key);
            return !(i.Deleted || i.Expired || i.ExpiresInMs <= 0);
        }

        /// <summary>
        /// Returns true if the backend contains a value under the key, including any expired or deleted value
        /// that still lingers.
        /// </summary>
        public bool ContainsKeyWithInfo(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.ContainsKeyWithInfo(key);
            var local = _currentState.Value;
            return local.TryGetValue(key, out MessageItem _) || _wrapped.ContainsKeyWithInfo(key);
        }

        /// <summary>
        /// Gets all (non-deleted and non-expired) keys contained in the backend. Throws if used in a consistent
        /// transaction.
        /// </summary>
        public ICollection<string> Keys =>
            IsInConsistentTransaction ? throw new NotSupportedException("Accessing all keys in a consistent transaction is not supported.")
            : _wrapped.Keys;

        /// <summary>
        /// Gets all keys contained in the backend, including deleted and expired keys that still linger. Throws
        /// if used in a consistent transaction.
        /// </summary>
        public ICollection<string> KeysWithInfo =>
            IsInConsistentTransaction ? throw new NotSupportedException("Accessing all keys in a consistent transaction is not supported.")
            : _wrapped.KeysWithInfo;

        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            return TryGetInternal(key, out item);
        }

        private bool TryGetInternal<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            if (!IsInConsistentTransaction)
                return _wrapped.TryGet(key, out item);
            var local = _currentState.Value;
            if (!local.TryGetValue(key, out MessageItem i))
                return _wrapped.TryGet(key, out item);
            if (i.Deleted || i.Expired || i.ExpiresInMs <= 0)
            {
                item = default;
                return false;
            }
            item = (TItem)i.Value;
            return true;
        }

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        public FieldInfo<TItem> TryGetWithInfo<TItem>(string key) where TItem : IMergeable<TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.TryGetWithInfo<TItem>(key);
            var local = _currentState.Value;
            if (!local.TryGetValue(key, out var item))
                return _wrapped.TryGetWithInfo<TItem>(key);
            return new FieldInfo<TItem>(item);
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
        /// Set a value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value.
        /// </summary>
        /// <param name="expireInMs">If given, the item will expire and be removed from the storage in
        /// this many milliseconds. If not null, must be > 0.</param>
        public VectorRelationship Set<TItem>(string key, TItem item, int? expireInMs = null) where TItem : IMergeable<TItem>
            => Shield.InTransaction(() =>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            if (expireInMs <= 0)
                throw new ArgumentOutOfRangeException(nameof(expireInMs));
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.Set(key, item, expireInMs);
            return SetInternal(key, item, expireInMs);
        });

        private VectorRelationship SetInternal<TItem>(string key, TItem val, int? expireInMs = null) where TItem : IMergeable<TItem>
        {
            var local = _currentState.Value;
            var oldValue = local.TryGetValue(key, out var oldItem)
                ? new FieldInfo<TItem>(oldItem)
                : _wrapped.TryGetWithInfo<TItem>(key);
            if (oldValue != null)
            {
                var (mergedValue, cmp) = new FieldInfo<TItem>(val, expireInMs)
                    .MergeWith(oldValue, Configuration.ExpiryComparePrecision);
                if (cmp == ComplexRelationship.Less || cmp == ComplexRelationship.Equal || cmp == ComplexRelationship.EqualButLess)
                    return cmp.GetValueRelationship();
                local[key] = new MessageItem
                {
                    Key = key,
                    Value = mergedValue.Value,
                    Deleted = mergedValue.Deleted,
                    Expired = mergedValue.Expired,
                    ExpiresInMs = mergedValue.ExpiresInMs,
                };
                if (cmp.GetValueRelationship() != VectorRelationship.Equal)
                    OnChanged(key, oldValue.Value, mergedValue.Value, mergedValue.Deleted);
                return cmp.GetValueRelationship();
            }
            else
            {
                if (val is IDeletable del && del.CanDelete)
                    return VectorRelationship.Equal;
                local[key] = new MessageItem
                {
                    Key = key,
                    Value = val,
                    ExpiresInMs = expireInMs,
                };
                OnChanged(key, null, val, false);
                return VectorRelationship.Greater;
            }
        }

        /// <summary>
        /// A non-update, which ensures that when your local transaction is transmitted to other servers, this
        /// field will be transmitted as well, even if you did not change its value.
        /// </summary>
        /// <param name="key">The key to touch.</param>
        public void Touch(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            if (IsInConsistentTransaction)
                // we just need to read the field, because a consistent transaction transmits reads too.
                _wrapped.GetItem(key);
            else
                _wrapped.Touch(key);
        }

        /// <summary>
        /// Remove the given key from the storage.
        /// </summary>
        public bool Remove(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            if (IsInConsistentTransaction)
                return RemoveInternal(key);
            else
                return _wrapped.Remove(key);
        }

        private bool RemoveInternal(string key)
        {
            var local = _currentState.Value;
            if (!local.TryGetValue(key, out var oldItem) && (oldItem = _wrapped.GetActiveItem(key)) == null)
                return false;
            if (oldItem.Deleted)
                return false;

            local[key] = new MessageItem
            {
                Key = key,
                Data = oldItem.Data,
                Deleted = true,
            };
            if (oldItem.Expired || oldItem.ExpiresInMs <= 0)
                return false;
            var val = oldItem.Value;
            OnChanged(key, val, val, true);
            return true;
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

        private void OnChanged(string key, object oldVal, object newVal, bool deleted)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal, deleted);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired after any key changes. Does not fire within consistent transactions, but only when
        /// and if they commit their changes.
        /// </summary>
        public ShieldedEvent<ChangedEventArgs> Changed { get; } = new ShieldedEvent<ChangedEventArgs>();

        /// <summary>
        /// Fired when accessing a key that has no value or has expired. Handlers can specify a value to use,
        /// which will be saved in the backend and returned to the original reader.
        /// </summary>
        public ShieldedEvent<KeyMissingEventArgs> KeyMissing => _wrapped.KeyMissing;

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
                throw new ArgumentNullException(nameof(trans));
            if (attempts <= 0)
                throw new ArgumentOutOfRangeException(nameof(attempts));
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

                        if (ourChanges.Any() || _wrapped.Reads.Any())
                        {
                            var transParticipants = TransactionParticipants?.ToArray() ?? Transport.Servers;
                            var exceptMe = transParticipants.Where(s => !StringComparer.InvariantCultureIgnoreCase.Equals(s, Transport.OwnId));
                            var reads = _wrapped.Reads.ToArray();
                            transaction = new TransactionInfo
                            {
                                Initiator = Transport.OwnId,
                                InitiatorVotes = TransactionParticipants == null ||
                                    transParticipants.Contains(Transport.OwnId, StringComparer.InvariantCultureIgnoreCase),
                                Reads = reads
                                    .Select(key => _wrapped.GetItem(key) ?? new MessageItem { Key = key })
                                    .ToArray(),
                                Changes = ourChanges.Values.ToArray(),
                                State = new TransactionVector(
                                    new[] { new VectorItem<TransactionState>(Transport.OwnId, TransactionState.Prepared) }
                                    .Concat(exceptMe.Select(s => new VectorItem<TransactionState>(s, TransactionState.None)))
                                    .ToArray()),
                            };
                            ourState = new BackendState(WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString()),
                                Transport.OwnId, this, reads);

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
                    var result = await prepTask.WithTimeout(GetNewTimeout());
                    if (result.Success)
                        return cont;

                    cont.Rollback();
                    if (result.WaitBeforeRetry != null)
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
        /// <param name="attempts">The number of attempts to make, default 10. If nested in another consistent
        /// transaction, this argument is ignored.</param>
        /// <returns>Result indicates if we succeeded in the given number of attempts, and returns
        /// the result that the lambda returned.</returns>
        public async Task<(bool Success, T Value)> RunConsistent<T>(Func<T> trans, int attempts = 10)
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

        private readonly ApplyMethods _compareMethods;

        private ComplexRelationship CheckOne<TItem>(string key, FieldInfo<TItem> value)
            where TItem : IMergeable<TItem>
        {
            var curr = _wrapped.TryGetWithInfo<TItem>(key);
            if (curr == null)
                return value.Deleted || value.Expired ? ComplexRelationship.Equal : ComplexRelationship.Greater;
            return value.VectorCompare(curr, Configuration.ExpiryComparePrecision);
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
                        if (_wrapped.GetActiveItem(read.Key) == null)
                            continue;
                        if (initiatedLocally)
                            return false;
                        _wrapped.Touch(read.Key);
                        result = false;
                    }
                    else
                    {
                        var comparer = _compareMethods.Get(obj.GetType());
                        // what you read must be Greater or Equal to what we have.
                        if ((comparer(read.Key, obj).GetValueRelationship() | VectorRelationship.Greater) != VectorRelationship.Greater)
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
                    var comparer = _compareMethods.Get(obj.GetType());
                    // the same for writes - because e.g. if you add and remove a key that did not exist, your write's effect on
                    // other servers will be Equal. this is also OK.
                    if ((comparer(change.Key, obj, change.Deleted, false, change.ExpiresInMs)
                        .GetValueRelationship() | VectorRelationship.Greater) != VectorRelationship.Greater)
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
                OnChanged(e.Key.Substring(PublicPfx.Length), e.OldValue, e.NewValue, e.Deleted);
            }
        }

        private void OnTransactionChanged(string id, TransactionInfo newVal)
        {
            if (!newVal.State.Any(i => StringComparer.InvariantCultureIgnoreCase.Equals(i.ServerId, Transport.OwnId)))
            {
                if (newVal.IsSuccess)
                    Apply(newVal);
                return;
            }
            if (newVal.State[Transport.OwnId] != TransactionState.None || newVal.IsDone || _transactions.ContainsKey(id))
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
                    var prepareTask = PrepareInternal(ourState, newVal, false).WithTimeout(Configuration.ConsistentPrepareTimeoutRange.Max);
                    if (await Task.WhenAny(prepareTask, ourState.PrepareCompleter.Task) != prepareTask ||
                        !prepareTask.Result.Success)
                    {
                        SetRejected(id);
                    }
                }
                catch (Exception ex)
                {
                    SetRejected(id);
                    _wrapped.RaiseError(new GossipBackendException("Unexpected error while preparing external transaction.", ex));
                }
            });
        }

        private bool SetPrepared(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.IsDone)
                return false;
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Prepared));
            return true;
        });

        private bool SetRejected(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.IsDone)
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
            if (current.IsFail || current.IsRejected)
            {
                SetFail(id);
                if (ourState != null)
                    ourState.Complete(false);
            }
            else if (current.IsSuccess)
            {
                ApplyAndSetSuccess(id);
                if (ourState != null)
                    ourState.Complete(true);
            }
            else if (current.IsPrepared &&
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
