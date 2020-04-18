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
        private readonly ApplyMethods _checkOneMethods;
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
            _checkOneMethods = new ApplyMethods((key, fi) => CheckOne(key, fi));
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
                //if (cmp.GetValueRelationship() != VectorRelationship.Equal)
                //    OnChanged(key, oldValue.Value, mergedValue.Value, mergedValue.Deleted);
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
                //OnChanged(key, null, val, false);
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
            //OnChanged(key, val, val, true);
            return true;
        }

        private void OnChanged(string key, object oldVal, object newVal, bool deleted)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal, deleted);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired after any key changes. Does not fire within consistent transactions, but only when
        /// and if they commit their changes. Please note that it also fires during processing of
        /// incoming gossip messages, so, unless you really need to, don't do anything slow here.
        /// </summary>
        public ShieldedEvent<ChangedEventArgs> Changed { get; } = new ShieldedEvent<ChangedEventArgs>();

        /// <summary>
        /// An enumerable of keys read or written into by the current transaction. Includes
        /// keys that did not have a value.
        /// </summary>
        public IEnumerable<string> Reads => _wrapped.Reads;

        /// <summary>
        /// An enumerable of keys written into by the current transaction.
        /// </summary>
        public IEnumerable<string> Changes => IsInConsistentTransaction ? _currentState.Value.Keys : _wrapped.Changes;

        private readonly ShieldedLocal<Dictionary<string, MessageItem>> _currentState = new ShieldedLocal<Dictionary<string, MessageItem>>();

        /// <summary>
        /// True if we're in a consistent transaction.
        /// </summary>
        public bool IsInConsistentTransaction => Shield.IsInTransaction && _currentState.HasValue;

        private struct PrepareResult
        {
            public readonly bool Success;
            public readonly Task WaitBeforeRetry;
            public readonly Task<bool> CompletionTask;

            private PrepareResult(bool success, Task wait, Task<bool> completionTask)
            {
                Success = success;
                WaitBeforeRetry = wait;
                CompletionTask = completionTask;
            }

            public static PrepareResult Failed(Task wait = null) => new PrepareResult(false, wait, null);
            public static PrepareResult Ok(Task<bool> completionTask) => new PrepareResult(true, null, completionTask);
        }

        private (TransactionInfo, CommitContinuation) BufferTransaction(Action trans)
        {
            TransactionInfo transaction = null;
            var cont = Shield.RunToCommit(Timeout.Infinite, () =>
            {
                transaction = null;
                var ourChanges = _currentState.Value = new Dictionary<string, MessageItem>();
                trans();
                if (!_wrapped.Reads.Any())
                    return;
                transaction = new TransactionInfo
                {
                    Initiator = Transport.OwnId,
                    Reads = _wrapped.Reads
                        .Select(key => _wrapped.GetItem(key) ?? new MessageItem { Key = key })
                        .ToArray(),
                    Changes = ourChanges.Values.ToArray(),
                    State = new TransactionVector(
                        (TransactionParticipants?.ToArray() ?? new[] { Transport.OwnId }.Concat(Transport.Servers))
                        .Select(s => new VectorItem<TransactionState>(s, TransactionState.None))
                        .ToArray()),
                };
            });
            return (transaction, cont);
        }

        /// <summary>
        /// Runs a distributed consistent transaction. May be called from within another consistent
        /// transaction, but not from a non-consistent one.
        /// </summary>
        /// <param name="trans">The lambda to run as a distributed transaction.</param>
        /// <param name="attempts">The number of attempts to make, default 10.</param>
        /// <param name="runTransOnCapturedContext">Whether to capture the current synchronization context and
        /// always run your lambda in that context.</param>
        /// <returns>Eventually a bool indicating whether the transaction succeeded.</returns>
        public async Task<bool> RunConsistent(Action trans, int attempts = 10, bool runTransOnCapturedContext = true)
        {
            if (trans == null)
                throw new ArgumentNullException(nameof(trans));
            if (attempts <= 0)
                throw new ArgumentOutOfRangeException(nameof(attempts));

            if (IsInConsistentTransaction)
            {
                trans();
                return true;
            }
            if (Shield.IsInTransaction)
                throw new InvalidOperationException("RunConsistent cannot be called within non-consistent transactions.");

            using (_logger.BeginScope("RunConsistent call ID {PrepareId}", Guid.NewGuid()))
            {
                CommitContinuation cont = null;
                try
                {
                    while (attempts --> 0)
                    {
                        _logger.LogDebug("Attempting a consistent transaction, attempts left: {Attempts}", attempts + 1);
                        TransactionInfo transaction;
                        (transaction, cont) = BufferTransaction(trans);
                        if (transaction == null)
                        {
                            _logger.LogDebug("Transaction touched no backend fields, exiting with trivial success.");
                            return cont.TryCommit();
                        }
                        var id = WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString());
                        Shield.InTransaction(() => _ballotCounter.Modify((ref long c) => transaction.BallotNumber = unchecked(++c)));

                        using (_logger.BeginScope("Local {TransactionId}", id))
                        {
                            _logger.LogDebug("Participating servers: {TransactionServers}", string.Join(",", transaction.State.Select(s => s.ServerId)));
                            var prepare = PrepareLocal(id, transaction);
                            if (!prepare.Success)
                            {
                                _logger.LogDebug("Failed to prepare transaction locally, rolling back.");
                                cont.Rollback();
                                if (prepare.WaitBeforeRetry != null && attempts > 0)
                                {
                                    _logger.LogDebug("Awaiting before retry.");
                                    await prepare.WaitBeforeRetry.ConfigureAwait(runTransOnCapturedContext);
                                }
                            }
                            else if (await prepare.CompletionTask.ConfigureAwait(runTransOnCapturedContext))
                            {
                                _logger.LogDebug("Successfully reached consensus.");
                                // TODO: what if the caller wants to cancel us? this continuation will stay blocked...
                                cont.Commit();
                                return true;
                            }
                            else
                            {
                                _logger.LogDebug("Failed to reach consensus, rolling back.");
                                cont.Rollback();
                            }
                        }
                    }
                    _logger.LogWarning("Failed to prepare transaction in given number of attempts.");
                    return false;
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "RunConsistent interrupted by exception.");
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
        /// <returns>Result indicates if we succeeded in the given number of attempts, and returns
        /// the result that the lambda returned.</returns>
        public async Task<(bool Success, T Value)> RunConsistent<T>(Func<T> trans, int attempts = 10, bool runTransOnCapturedContext = true)
        {
            T res = default;
            var success = await RunConsistent(() => { res = trans(); }, attempts, runTransOnCapturedContext).ConfigureAwait(runTransOnCapturedContext);
            return (success, success ? res : default);
        }

        private class TransactionMeta : IComparable<TransactionMeta>
        {
            public readonly string Id;
            public readonly string Initiator;
            public readonly long BallotNumber;

            public TransactionMeta(string id, TransactionInfo transaction)
            {
                Id = id;
                Initiator = transaction.Initiator;
                BallotNumber = transaction.BallotNumber;
            }

            public int CompareTo(TransactionMeta other)
            {
                return TransactionInfo.ComparePriority(Initiator, BallotNumber, other.Initiator, other.BallotNumber);
            }
        }

        private readonly ShieldedDictNc<string, TaskCompletionSource<bool>> _activeTransactions = new ShieldedDictNc<string, TaskCompletionSource<bool>>();
        private readonly ShieldedDictNc<string, TransactionMeta[]> _fieldHolders = new ShieldedDictNc<string, TransactionMeta[]>();
        private readonly Shielded<long> _ballotCounter = new Shielded<long>();

        private PrepareResult PrepareLocal(string id, TransactionInfo transaction) => Shield.InTransaction(() =>
        {
            var problems = CheckFieldsAreFree(transaction.AllKeys);
            if (problems.Any())
            {
                _logger.LogDebug("Local transaction failed to get exclusive access to fields.");
                return PrepareResult.Failed(Task.WhenAll(problems));
            }
            if (!Check(transaction, true))
                return PrepareResult.Failed();
            var metaArr = new[] { new TransactionMeta(id, transaction) };
            foreach (var key in transaction.AllKeys)
                _fieldHolders[key] = metaArr;
            var completer = _activeTransactions[id] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (transaction.State.HasServer(Transport.OwnId))
            {
                _logger.LogDebug("Voting Promised on {TransactionId}", id);
                transaction = transaction.WithState(Transport.OwnId, TransactionState.Promised);
            }
            _wrapped.Set(id, transaction);
            return PrepareResult.Ok(completer.Task);
        });

        private Task[] CheckFieldsAreFree(IEnumerable<string> keys) => Shield.InTransaction(() =>
        {
            return keys
                .SelectMany(key => _fieldHolders.TryGetValue(key, out var others) ? others.Select(m => m.Id) : Enumerable.Empty<string>())
                .Distinct()
                .Select(otherId => _activeTransactions[otherId].Task)
                .ToArray();
        });

        private bool PrepareExternal(string id, TransactionInfo transaction) => Shield.InTransaction(() =>
        {
            if (!Check(transaction, false))
            {
                _logger.LogDebug("Voting Rejected and in the future ignoring {TransactionId}", id);
                _wrapped.Set(id, transaction.WithState(Transport.OwnId, TransactionState.Rejected));
                return false;
            }
            var metaArr = new[] { new TransactionMeta(id, transaction) };
            foreach (var key in transaction.AllKeys)
            {
                if (!_fieldHolders.TryGetValue(key, out var others))
                {
                    _fieldHolders[key] = metaArr;
                }
                else
                {
                    var newIds = metaArr.Concat(others).OrderBy(m => m).ToArray();
                    _fieldHolders[key] = newIds;
                }
            }
            _activeTransactions[id] = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            return TrySetPromised(id, transaction);
        });

        private ComplexRelationship CheckOne<TItem>(string key, FieldInfo<TItem> value)
            where TItem : IMergeable<TItem>
        {
            var curr = _wrapped.TryGetWithInfo<TItem>(key);
            return curr == null ? ComplexRelationship.Greater : value.VectorCompare(curr, Configuration.ExpiryComparePrecision);
        }

        private bool Check(TransactionInfo transaction, bool quickCheck) => Shield.InTransaction(() =>
        {
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
                        if (quickCheck)
                            return false;
                        _wrapped.Touch(read.Key);
                        result = false;
                    }
                    else
                    {
                        var comparer = _checkOneMethods.Get(obj.GetType());
                        // what you read must be Greater or Equal to what we have.
                        if ((comparer(read.Key, obj).GetValueRelationship() | VectorRelationship.Greater) != VectorRelationship.Greater)
                        {
                            _logger.LogDebug("Read not up to date, key {ItemKey}", read.Key);
                            if (quickCheck)
                                return false;
                            _wrapped.Touch(read.Key);
                            result = false;
                        }
                    }
                }

            return result;
        });

        private void _wrapped_Changed(object sender, ChangedEventArgs e)
        {
            if (e.Key.StartsWith(PublicPfx))
            {
                OnChanged(e.Key.Substring(PublicPfx.Length), e.OldValue, e.NewValue, e.Deleted);
            }
            else
            {
                OnTransactionChanged(e.Key, (TransactionInfo)e.NewValue);
            }
        }

        private void OnTransactionChanged(string id, TransactionInfo newVal)
        {
            if (BallotComparer.Compare(newVal.BallotNumber, _ballotCounter) < 0)
                _ballotCounter.Value = newVal.BallotNumber;

            if (!newVal.State.HasServer(Transport.OwnId))
            {
                if (newVal.IsSuccessful)
                    Apply(id, newVal);
                return;
            }
            if (newVal.State[Transport.OwnId] != TransactionState.None || _activeTransactions.ContainsKey(id) || newVal.IsDone)
            {
                OnStateChange(id, newVal);
                return;
            }
            if (StringComparer.InvariantCultureIgnoreCase.Equals(newVal.Initiator, Transport.OwnId))
            {
                throw new ApplicationException("Invalid transaction info - local server is Initiator, but his state is None.");
            }

            using (_logger.BeginScope("External {TransactionId}", id))
            {
                _logger.LogDebug("Preparing external transaction");
                var result = PrepareExternal(id, newVal);
            }
        }

        private void OnStateChange(string id, TransactionInfo transaction)
        {
            if (!_activeTransactions.ContainsKey(id))
                return;
            if (transaction.IsRejected)
            {
                _logger.LogDebug("{TransactionId} has been rejected or failed.", id);
                Complete(id, transaction);
            }
            else if (transaction.IsSuccessful)
            {
                _logger.LogDebug("{TransactionId} has succeeded.", id);
                Apply(id, transaction);
                Complete(id, transaction);
            }
            else if (transaction.IsPromised && transaction.State[Transport.OwnId] == TransactionState.Promised)
            {
                _logger.LogDebug("{TransactionId} has reached promise majority.", id);
                TrySetAccepted(id, transaction);
            }
        }

        private void Complete(string id, TransactionInfo transaction) => Shield.InTransaction(() =>
        {
            if (!_activeTransactions.TryGetValue(id, out var tcs))
                return;
            bool success = transaction.IsSuccessful;
            _logger.LogDebug("Completing {TransactionId}, successful: {Successful}", id, success);
            Shield.SideEffect(() => tcs.TrySetResult(success));
            RemoveFromHolders(id, transaction);
            _activeTransactions.Remove(id);

            if (success)
            {
                // rejections due to this commit we must do now, in this Shielded transaction, to prevent any later Changed event handlers
                // that come in this same transaction from mistakenly progressing on transactions that should be rejected.
                foreach (var otherId in GetTransactionsByKeysSorted(transaction.AllKeys))
                {
                    if (!_wrapped.TryGet(otherId, out TransactionInfo other))
                        continue;
                    var ourOtherState = other.State[Transport.OwnId];
                    // we may not change Accepted vote to Rejected. yes, probably the "other" will never be committed. but there is a
                    // chance that the other servers have already committed "other", and the current "transaction" is in fact a consistent
                    // follow-up of it! in that case, even though "other" does not check out any more, it will in fact soon be successful.
                    if (ourOtherState == TransactionState.Rejected || ourOtherState == TransactionState.Accepted)
                        continue;
                    if (!Check(other, quickCheck: true))
                        // note that this may recurse into Complete, but only with success == false.
                        TrySetRejected(otherId, other);
                }
            }

            // moving competitors forward will be done in a side-effect, to avoid crazy recursions.
            PostCommitProgressCheck(transaction.AllKeys);
        });

        private static TransactionMeta[] _emptyMetaArr = new TransactionMeta[0];

        private IEnumerable<string> GetTransactionsByKeysSorted(IEnumerable<string> keys)
        {
            return keys
                .SelectMany(key => _fieldHolders.TryGetValue(key, out var h) ? h : _emptyMetaArr)
                .Distinct()
                .OrderBy(m => m)
                .Select(m => m.Id);
        }

        private ShieldedLocal<HashSet<string>> _fieldsToCheck = new ShieldedLocal<HashSet<string>>();

        private void PostCommitProgressCheck(IEnumerable<string> keys)
        {
            var fieldsToCheck = _fieldsToCheck.GetValueOrDefault();
            if (fieldsToCheck == null)
                fieldsToCheck = CreateProgressCheckSubscription();
            foreach (var key in keys)
                fieldsToCheck.Add(key);
        }

        private HashSet<string> CreateProgressCheckSubscription()
        {
            HashSet<string> fieldsToCheck = _fieldsToCheck.Value = new HashSet<string>();
            Shield.SideEffect(() => Shield.InTransaction(() =>
            {
                foreach (var otherId in GetTransactionsByKeysSorted(fieldsToCheck))
                {
                    if (!_wrapped.TryGet(otherId, out TransactionInfo other))
                        continue;
                    var ourOtherState = other.State[Transport.OwnId];
                    if (ourOtherState == TransactionState.Rejected || ourOtherState == TransactionState.Accepted)
                        continue;
                    if (other.IsPromised)
                        TrySetAccepted(otherId, other);
                    else if (ourOtherState == TransactionState.None)
                        TrySetPromised(otherId, other);
                }
            }));
            return fieldsToCheck;
        }

        private void RemoveFromHolders(string id, TransactionInfo transaction)
        {
            foreach (var key in transaction.AllKeys)
            {
                var holders = _fieldHolders[key];
                if (holders.Length == 1)
                    _fieldHolders.Remove(key);
                else
                    _fieldHolders[key] = holders.Where(other => other.Id != id).ToArray();
            }
        }

        private IEnumerable<(string Id, TransactionInfo Transaction)> GetCompetitors(string id, TransactionInfo transaction)
        {
            return transaction.AllKeys
                .SelectMany(key => _fieldHolders.TryGetValue(key, out var h) ? h.Where(m => m.Id != id) : _emptyMetaArr)
                .Distinct()
                .Select(m => (Id: m.Id, Transaction: _wrapped.TryGet(m.Id, out TransactionInfo other) ? other : null))
                .Where(pair => pair.Transaction != null);
        }

        private bool CanMakeProgress(string id, TransactionInfo transaction)
        {
            var currHasAcceptedVotes = transaction.HasAcceptedVotes;
            foreach (var (otherId, other) in GetCompetitors(id, transaction))
            {
                var competitorState = other.State[Transport.OwnId];
                var beforeMe = other.ComparePriority(transaction) < 0;
                if (competitorState == TransactionState.Accepted && (beforeMe || !currHasAcceptedVotes))
                    return false;
                if (competitorState == TransactionState.Promised && beforeMe)
                    return false;
            }
            return true;
        }

        private bool TrySetPromised(string id, TransactionInfo transaction) => Shield.InTransaction(() =>
        {
            if (!transaction.State.HasServer(Transport.OwnId))
                throw new ApplicationException("Trying to set state on a transaction we're not a part of.");
            if (transaction.State.Count(s => s.Value == TransactionState.Promised) == transaction.State.Count / 2)
                return TrySetAccepted(id, transaction);
            if (!CanMakeProgress(id, transaction))
                return false;
            _logger.LogDebug("Changing state to Promised on {TransactionId}", id);
            _wrapped.Set(id, transaction.WithState(Transport.OwnId, TransactionState.Promised));
            return true;
        });

        private bool TrySetAccepted(string id, TransactionInfo transaction) => Shield.InTransaction(() =>
        {
            if (!transaction.State.HasServer(Transport.OwnId))
                throw new ApplicationException("Trying to set state on a transaction we're not a part of.");
            if (!CanMakeProgress(id, transaction))
                return false;
            _logger.LogDebug("Changing state to Accepted on {TransactionId}", id);
            _wrapped.Set(id, transaction.WithState(Transport.OwnId, TransactionState.Accepted));
            return true;
        });

        private bool TrySetRejected(string id, TransactionInfo transaction) => Shield.InTransaction(() =>
        {
            if (!transaction.State.HasServer(Transport.OwnId))
                throw new ApplicationException("Trying to set state on a transaction we're not a part of.");
            _logger.LogDebug("Changing state to Rejected on {TransactionId}", id);
            _wrapped.Set(id, transaction.WithState(Transport.OwnId, TransactionState.Rejected));
            return true;
        });

        private void Apply(string id, TransactionInfo current)
        {
            _logger.LogDebug("Applying {TransactionId}", id);
            _wrapped.ApplyItems(
                (current.Reads ?? Enumerable.Empty<MessageItem>())
                .Concat(current.Changes ?? Enumerable.Empty<MessageItem>())
                .ToArray(), false);
        }

        public void Dispose()
        {
            _wrapped.Dispose();
        }
    }
}
