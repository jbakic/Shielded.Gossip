using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly ApplyMethods _compareMethods;
        private readonly ILogger _logger;

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
        /// <param name="logger">The logger to use.</param>
        public ConsistentGossipBackend(ITransport transport, GossipConfiguration configuration, ILogger logger = null)
        {
            _compareMethods = new ApplyMethods((key, fi) => CheckOne(key, fi));
            _logger = logger ?? NullLogger.Instance;
            _wrapped = new GossipBackend(transport, configuration, this, _logger);
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
            var res = TryGet(key, out var obj);
            item = res ? (TItem)obj : default;
            return res;
        }

        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        public bool TryGet(string key, out object item)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            return TryGetInternal(key, out item);
        }


        private bool TryGetInternal(string key, out object item)
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
            item = i.Value;
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

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        public FieldInfo TryGetWithInfo(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            key = WrapPublicKey(key);
            if (!IsInConsistentTransaction)
                return _wrapped.TryGetWithInfo(key);
            var local = _currentState.Value;
            if (!local.TryGetValue(key, out var item))
                return _wrapped.TryGetWithInfo(key);
            return new FieldInfo(item);
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
            public readonly TaskCompletionSource<PrepareResult> PrepareCompleter = new TaskCompletionSource<PrepareResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource<object> Committer = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

            public BackendState(string id, string initiator, ConsistentGossipBackend self, string[] allKeys)
            {
                Self = self;
                Initiator = initiator;
                TransactionId = id;
                AllKeys = allKeys;
            }

            public void Complete() => Shield.InTransaction(() =>
            {
                if (!Self._transactions.Remove(TransactionId))
                    return;
                Self._logger.LogDebug("Completing backend state of {TransactionId}", TransactionId);
                Self.UnlockFields(this);
                Shield.SideEffect(() =>
                {
                    PrepareCompleter.TrySetResult(new PrepareResult(false));
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
        /// <param name="runTransOnCapturedContext">Whether to capture the current synchronization context and
        /// always run your lambda in that context.</param>
        /// <returns>Result of the task is a continuation for later committing/rolling back, or null if
        /// preparation failed.</returns>
        public async Task<CommitContinuation> Prepare(Action trans, int attempts = 10, bool runTransOnCapturedContext = true)
        {
            if (trans == null)
                throw new ArgumentNullException(nameof(trans));
            if (attempts <= 0)
                throw new ArgumentOutOfRangeException(nameof(attempts));
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("Prepare cannot be called within a transaction.");

            using (_logger.BeginScope("Consistent prepare ID {PrepareId}", Guid.NewGuid()))
            {
                CommitContinuation cont = null;
                try
                {
                    while (attempts --> 0)
                    {
                        _logger.LogDebug("Preparing a consistent transaction, attempts left: {Attempts}", attempts + 1);
                        BackendState ourState = null;
                        TransactionInfo transaction = null;
                        cont = Shield.RunToCommit(Timeout.Infinite, () =>
                        {
                            ourState = null;
                            transaction = null;
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
                        {
                            _logger.LogDebug("Transaction touched no backend fields, exiting with trivial success.");
                            return cont;
                        }
                        var id = ourState.TransactionId;
                        using (_logger.BeginScope("Local {TransactionId}", id))
                        {
                            _logger.LogDebug("Participating servers: {TransactionServers}; initiator votes: {InitiatorVotes}",
                                transaction.State.Select(s => s.ServerId).ToArray(), transaction.InitiatorVotes);
                            Shield.InTransaction(() => _transactions.Add(id, ourState));
                            async Task<PrepareResult> PreparationProcess()
                            {
                                var prepare = await PrepareInternal(ourState, transaction, true).ConfigureAwait(false);
                                if (!prepare.Success)
                                {
                                    _logger.LogDebug("Failed to prepare transaction locally.");
                                    return prepare;
                                }
                                Shield.InTransaction(() =>
                                {
                                    if (_transactions.ContainsKey(id))
                                        _wrapped.Set(id, transaction);
                                });
                                _logger.LogDebug("Local prepare succeeded, awaiting other servers' votes.");
                                return await ourState.PrepareCompleter.Task.ConfigureAwait(false);
                            }
                            var result = await PreparationProcess().WithTimeout(GetNewTimeout()).ConfigureAwait(runTransOnCapturedContext);
                            if (result.Success)
                            {
                                _logger.LogDebug("Successfully reached consensus with other servers.");
                                return cont;
                            }

                            _logger.LogDebug("Failed to prepare transaction, rolling back.");
                            cont.Rollback();

                            if (result.WaitBeforeRetry != null && attempts >= 1)
                            {
                                _logger.LogDebug("Awaiting before retry.");
                                await result.WaitBeforeRetry.ConfigureAwait(runTransOnCapturedContext);
                            }
                        }
                    }
                    _logger.LogWarning("Failed to prepare transaction in given number of attempts.");
                    return null;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Prepare interrupted by exception.");
                    if (cont != null && !cont.Completed)
                        cont.TryRollback();
                    throw;
                }
            }
        }

        /// <summary>
        /// Runs a distributed consistent transaction. May be called from within another consistent
        /// transaction, but not from a non-consistent one.
        /// </summary>
        /// <param name="trans">The lambda to run as a distributed transaction.</param>
        /// <param name="attempts">The number of attempts to make, default 10. If nested in another consistent
        /// transaction, this argument is ignored.</param>
        /// <param name="runTransOnCapturedContext">Whether to capture the current synchronization context and
        /// always run your lambda in that context.</param>
        /// <returns>Result indicates if we succeeded in the given number of attempts.</returns>
        public async Task<bool> RunConsistent(Action trans, int attempts = 10, bool runTransOnCapturedContext = true)
        {
            if (IsInConsistentTransaction)
            {
                trans();
                return true;
            }

            using (var cont = await Prepare(trans, attempts, runTransOnCapturedContext).ConfigureAwait(runTransOnCapturedContext))
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
        /// <param name="runTransOnCapturedContext">Whether to capture the current synchronization context and
        /// always run your lambda in that context.</param>
        /// <returns>Result indicates if we succeeded in the given number of attempts, and returns
        /// the result that the lambda returned.</returns>
        public async Task<(bool Success, T Value)> RunConsistent<T>(Func<T> trans, int attempts = 10, bool runTransOnCapturedContext = true)
        {
            T res = default;
            var success = await RunConsistent(() => { res = trans(); }, attempts, runTransOnCapturedContext).ConfigureAwait(runTransOnCapturedContext);
            return (success, success ? res : default);
        }

        int GetNewTimeout()
        {
            var rnd = new Random();
            return rnd.Next(Configuration.ConsistentPrepareTimeoutRange.Min, Configuration.ConsistentPrepareTimeoutRange.Max);
        }

        async Task<PrepareResult> PrepareInternal(BackendState ourState, TransactionInfo transaction, bool initiatedLocally)
        {
            var lockRes = await LockFields(ourState).ConfigureAwait(false);
            return lockRes.Success ? new PrepareResult(Check(ourState, transaction, initiatedLocally), null) : lockRes;
        }

        private async Task<PrepareResult> LockFields(BackendState ourState)
        {
            var prepareTask = ourState.PrepareCompleter.Task;
            var keys = ourState.AllKeys;
            while (true)
            {
                Task[] waitFor = Shield.InTransaction(() =>
                {
                    if (!_transactions.TryGetValue(ourState.TransactionId, out var currState) || currState != ourState)
                        return new Task[0];
                    var tasks = keys.Select(key =>
                    {
                        if (!_fieldBlockers.TryGetValue(key, out var someState))
                            return (Task)null;
                        if (IsHigherPrio(ourState, someState))
                            Shield.SideEffect(() =>
                            {
                                _logger.LogDebug("Lock-conflict over key {Key}, trying to interrupt conflicting lower prio {OtherTransactionId}",
                                    key, someState.TransactionId);
                                someState.PrepareCompleter.TrySetResult(new PrepareResult(false, ourState.Committer.Task));
                            });
                        else
                            Shield.SideEffect(() =>
                                _logger.LogDebug("Lock-conflict over key {Key} with {OtherTransactionId}", key, someState.TransactionId));
                        return someState.Committer.Task;
                    }).Where(t => t != null).Distinct().ToArray();

                    if (tasks.Length > 0)
                        return tasks;

                    foreach (var key in keys)
                        _fieldBlockers[key] = ourState;
                    return null;
                });
                if (waitFor == null)
                {
                    _logger.LogDebug("Successfully locked the fields.");
                    return new PrepareResult(true);
                }
                if (waitFor.Length == 0)
                {
                    _logger.LogDebug("Transaction aborted while waiting for field locks.");
                    return new PrepareResult(false);
                }

                _logger.LogDebug("Awaiting for field locks to be released.");
                if (await Task.WhenAny(prepareTask, Task.WhenAll(waitFor)).ConfigureAwait(false) == prepareTask)
                    return await prepareTask;
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

        private ComplexRelationship CheckOne<TItem>(string key, FieldInfo<TItem> value)
            where TItem : IMergeable<TItem>
        {
            var curr = _wrapped.TryGetWithInfo<TItem>(key);
            return curr == null ? ComplexRelationship.Greater : value.VectorCompare(curr, Configuration.ExpiryComparePrecision);
        }

        private bool Check(BackendState ourState, TransactionInfo transaction, bool initiatedLocally) => Shield.InTransaction(() =>
        {
            var id = ourState.TransactionId;
            if (!_transactions.TryGetValue(id, out var currState) || currState != ourState)
                return false;
            bool result = true;
            _logger.LogDebug("Checking transaction consistency.");

            if (transaction.Reads != null)
                foreach (var read in transaction.Reads)
                {
                    var obj = read.Value;
                    if (obj == null)
                    {
                        if (_wrapped.GetActiveItem(read.Key) == null)
                            continue;
                        _logger.LogDebug("Read not up to date, key {ItemKey}", read.Key);
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
                            _logger.LogDebug("Read not up to date, key {ItemKey}", read.Key);
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
                    // writes must be Greater than what we have, or EqualButGreater, if you just extended the expiry
                    var cmp = comparer(change.Key, obj, change.Deleted, false, change.ExpiresInMs);
                    if (cmp != ComplexRelationship.Greater && cmp != ComplexRelationship.EqualButGreater)
                    {
                        _logger.LogDebug("Written version not greater than old value, key {ItemKey}", change.Key);
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
                    Apply(id, newVal);
                return;
            }
            if (newVal.State[Transport.OwnId] != TransactionState.None || newVal.IsDone || _transactions.ContainsKey(id))
            {
                OnStateChange(id, newVal);
                return;
            }
            if (StringComparer.InvariantCultureIgnoreCase.Equals(newVal.Initiator, Transport.OwnId))
            {
                throw new ApplicationException("Invalid transaction info - local server is Initiator, but his state is None.");
            }
            var ourState = new BackendState(id, newVal.Initiator, this, newVal.AllKeys.ToArray());
            _transactions.Add(id, ourState);
            Shield.SideEffect(async () =>
            {
                using (_logger.BeginScope("External {TransactionId}", id))
                {
                    try
                    {
                        _logger.LogDebug("Preparing external transaction");
                        var result = await PrepareInternal(ourState, newVal, false)
                            .WithTimeout(Configuration.ConsistentPrepareTimeoutRange.Max).ConfigureAwait(false);
                        if (result.Success)
                        {
                            _logger.LogDebug("Successfully prepared. Waiting for initiator command.");
                        }
                        else
                        {
                            _logger.LogDebug("Preparation failed.");
                            SetRejected(id);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error while preparing external transaction.");
                        SetRejected(id);
                        _wrapped.RaiseError(new GossipBackendException("Unexpected error while preparing external transaction.", ex));
                    }
                }
            });
        }

        private bool SetPrepared(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.IsDone)
                return false;
            _logger.LogDebug("Changing state to Prepared on {TransactionId}", id);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Prepared));
            return true;
        });

        private bool SetRejected(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                current.State[Transport.OwnId] != TransactionState.None ||
                current.IsDone)
                return false;
            _logger.LogDebug("Changing state to Rejected on {TransactionId}", id);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Rejected));
            return true;
        });

        private void SetFail(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return;
            _logger.LogDebug("Changing state to Fail on {TransactionId}", id);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Fail));
        });

        private bool SetSuccess(string id) => Shield.InTransaction(() =>
        {
            if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                return false;
            _logger.LogDebug("Changing state to Success on {TransactionId}", id);
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
                _logger.LogDebug("{TransactionId} has been rejected or failed.", id);
                SetFail(id);
                if (ourState != null)
                    ourState.Complete();
            }
            else if (current.IsSuccess)
            {
                _logger.LogDebug("{TransactionId} has succeeded.", id);
                Apply(id, current);
                SetSuccess(id);
                if (ourState != null)
                    ourState.Complete();
            }
            else if (current.IsPrepared &&
                StringComparer.InvariantCultureIgnoreCase.Equals(current.Initiator, Transport.OwnId))
            {
                if (ourState != null)
                {
                    _logger.LogDebug("Local {TransactionId} has been prepared by other servers.", id);
                    Shield.SideEffect(() =>
                        ourState.PrepareCompleter.TrySetResult(new PrepareResult(true)));
                }
                else
                {
                    // in case this transaction previously succeeded, but some other servers never got the news,
                    // and were holding in Prepared - we send the latest state of these keys, to make sure they
                    // are up-to-date when they release their field locks.
                    _logger.LogWarning("Local {TransactionId} has been prepared by other servers, but is unknown locally! Failing it.", id);
                    foreach (var key in current.AllKeys)
                        _wrapped.Touch(key);
                    SetFail(id);
                }
            }
        }

        private void Apply(string id, TransactionInfo current)
        {
            _logger.LogDebug("Applying {TransactionId}", id);
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
            {
                _logger.LogCritical("Unexpected commit failure of {TransactionId}.", ourState.TransactionId);
                throw new ApplicationException("Critical error - unexpected commit failure.");
            }
            Apply(ourState.TransactionId, current);
            _logger.LogDebug("Changing state to Success on {TransactionId}", id);
            _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Success));
            ourState.Complete();
        });

        private void Fail(BackendState ourState) => Shield.InTransaction(() =>
        {
            SetFail(ourState.TransactionId);
            ourState.Complete();
        });

        public void Dispose()
        {
            _wrapped.Dispose();
        }
    }
}
