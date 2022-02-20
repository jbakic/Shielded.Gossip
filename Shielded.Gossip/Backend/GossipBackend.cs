using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shielded.Gossip.Mergeables;
using Shielded.Gossip.Transport;
using Shielded.Gossip.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Backend
{
    /// <summary>
    /// A backend supporting a key/value store which is distributed using a causally consistent gossip protocol
    /// implementation. Use it in ordinary <see cref="Shield"/> transactions.
    /// Values should be CRDTs, implementing <see cref="IMergeableEx{T}"/>, or you can use the
    /// <see cref="Multiple{T}"/> and <see cref="VecVersioned{T}"/> wrappers to make them a CRDT.
    /// </summary>
    public class GossipBackend : IGossipBackend, IDisposable
    {
        internal class TransactionInfo
        {
            public Dictionary<string, StoredItem> Changes;
            public HashSet<string> Reads = new();
            public VersionHash HashEffect;
            public Dictionary<string, StoredDependency> Dependencies = new();
            public long FreshnessOffset;
            public Action<string> ExtraTracking;
            /// <summary>
            /// If set to true, direct mail will not happen.
            /// </summary>
            public bool DisableMailFx;
        }

        private readonly ShieldedDictNc<string, StoredItem> _local = new ShieldedDictNc<string, StoredItem>();
        private readonly ReverseTimeIndex _freshIndex;
        private readonly ShieldedLocal<TransactionInfo> _currTransaction = new ShieldedLocal<TransactionInfo>();
        private readonly ILogger _logger;

        private readonly Timer _gossipTimer;
        private readonly Timer _deletableTimer;

        public readonly ITransport Transport;
        public readonly GossipConfiguration Configuration;

        /// <summary>
        /// Event raised when an unexpected error occurs on one of the background tasks of the backend.
        /// </summary>
        public event EventHandler<GossipBackendException> Error;

        private readonly object _owner;

        internal void RaiseError(GossipBackendException ex) => Error?.Invoke(_owner, ex);

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transport">The message transport to use. The backend will dispose it when it gets disposed.</param>
        /// <param name="configuration">The configuration.</param>
        /// <param name="logger">The logger to use.</param>
        public GossipBackend(ITransport transport, GossipConfiguration configuration, ILogger logger = null)
            : this(transport, configuration, null, logger) { }

        internal GossipBackend(ITransport transport, GossipConfiguration configuration, object owner, ILogger logger)
        {
            _owner = owner ?? this;
            _logger = logger ?? NullLogger.Instance;
            Transport = transport ?? throw new ArgumentNullException(nameof(transport));
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _freshIndex = new ReverseTimeIndex(GetItem);
            _gossipTimer = new Timer(_ => SpreadRumors(), null, Configuration.GossipInterval, Configuration.GossipInterval);
            _deletableTimer = new Timer(GetDeletableTimerMethod(), null, Configuration.CleanUpInterval, Configuration.CleanUpInterval);
            Transport.MessageHandler = Transport_MessageHandler;
        }

        private TimerCallback GetDeletableTimerMethod()
        {
            var lockObj = new object();
            return _ =>
            {
                using (_logger.BeginScope("Deletable timer run {DeletableRunId}", Guid.NewGuid()))
                {
                    bool lockTaken = false;
                    try
                    {
                        Monitor.TryEnter(lockObj, ref lockTaken);
                        if (!lockTaken)
                        {
                            _logger.LogWarning("Previous deletable timer tick still running.");
                            return;
                        }
                        CleanUpFields();
                        CleanUpGossipStates();
                        _logger.LogDebug("Clean-up complete.");
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error on deletable timer task.");
                        RaiseError(new GossipBackendException("Unexpected error on deletable timer task.", ex));
                    }
                    finally
                    {
                        if (lockTaken)
                            Monitor.Exit(lockObj);
                    }
                }
            };
        }

        private void CleanUpFields()
        {
            _logger.LogDebug("Searching for items to clean up.");
            var currTickCount = Environment.TickCount;
            var toRemove = Shield.InTransaction(() =>
                _freshIndex
                    .Where(i => i.Item.RemovableSince.HasValue
                        ? unchecked(currTickCount - i.Item.RemovableSince.Value) > Configuration.RemovableItemLingerMs
                        : i.Item.ExpiresInMs <= 0)
                    .Select(i => i.Item)
                    .ToArray());
            _logger.LogDebug("Found {ToRemoveCount} items to clean up.", toRemove.Length);
            Shield.InTransaction(() =>
            {
                foreach (var item in toRemove)
                    if (_local.TryGetValue(item.Key, out var mi) && mi == item)
                    {
                        if (item.RemovableSince.HasValue)
                            _local.Remove(item.Key);
                        else
                            Expire(item);
                    }
            });
        }

        private void Expire(StoredItem item)
        {
            dynamic val = item.Value;
            var hash = GetHash(item.Key, val);
            var newItem = new StoredItem
            {
                Key = item.Key,
                Data = item.Data,
                Expired = true,
            };
            _local[item.Key] = newItem;
            EnlistWrite(item, newItem, val, hash);
        }

        private void CleanUpGossipStates()
        {
            _logger.LogDebug("Searching for stale gossip states to remove.");
            // why this? well, if a server gets removed from Transport.Servers, an old state could stay in the states
            // dictionary. if it stays for > 24.9 days, the HasTimedOut check would start overflowing...
            var staleStates = Shield.InTransaction(() => _gossipStates.Where(kvp => HasTimedOut(kvp.Value)).ToArray());
            _logger.LogDebug("Found {ToRemoveCount} stale states to remove.", staleStates.Length);
            Shield.InTransaction(() =>
            {
                foreach (var kvp in staleStates)
                    if (_gossipStates.TryGetValue(kvp.Key, out var current) && current == kvp.Value)
                        _gossipStates.Remove(kvp.Key);
            });
        }

        private void DoDirectMail(TransactionInfo trans)
        {
            if (trans.DisableMailFx || Configuration.DirectMail == DirectMailType.Off || trans.Changes == null || trans.Changes.Count == 0)
                return;

            DirectMail createPackage(TransactionInfo trans) =>
                new DirectMail
                {
                    From = Transport.OwnId,
                    Transactions = new[]
                    {
                        new CausalTransaction
                        {
                            Changes = trans.Changes.Values.Select(si => (MessageItem)si).ToArray(),
                            Dependencies = trans.Dependencies.Values.Where(d => !trans.Changes.ContainsKey(d.Key)).Select(d => new Dependency
                            {
                                Key = d.Key,
                                VersionData = d.VersionData,
                            }).ToArray(),
                        }
                    }
                };

            if (Configuration.DirectMail == DirectMailType.Always)
            {
                Transport.Broadcast(createPackage(trans));
            }
            else if (Configuration.DirectMail == DirectMailType.GossipSupressed)
            {
                var package = createPackage(trans);
                foreach (var server in Transport.Servers.Where(s => !IsGossipActive(s)))
                    Transport.Send(server, package, false);
            }
            else if (Configuration.DirectMail == DirectMailType.StartGossip)
            {
                foreach (var server in Transport.Servers)
                    StartGossip(server);
            }
        }

        private enum MessageType
        {
            Start,
            Reply,
            End
        }

        private class GossipState
        {
            public readonly int? LastReceivedMsgId;
            public readonly CausalTransaction[] WaitingStore;
            public readonly int LastSentMsgId;
            public readonly ReverseTimeIndex.Enumerator LastWindowStart;
            public readonly int LastPackageSize;
            public readonly MessageType LastSentMsgType;
            // used only when LastSentMsgType == MessageType.Start
            public readonly int? PreviousSentEndMsgId;

            public readonly int CreationTickCount = Environment.TickCount;

            public GossipState(int? lastReceivedMsgId, CausalTransaction[] waitingStore, int lastSentMsgId, ReverseTimeIndex.Enumerator lastWindowStart,
                int lastPackageSize, MessageType lastSentMsgType, int? previousSentEndMsgId = null)
            {
                LastReceivedMsgId = lastReceivedMsgId;
                WaitingStore = waitingStore;
                LastSentMsgId = lastSentMsgId;
                LastWindowStart = lastWindowStart;
                LastPackageSize = lastPackageSize;
                LastSentMsgType = lastSentMsgType;
                PreviousSentEndMsgId = previousSentEndMsgId;
            }
        }

        private ShieldedDictNc<string, GossipState> _gossipStates = new ShieldedDictNc<string, GossipState>(StringComparer.InvariantCultureIgnoreCase);

        private bool HasTimedOut(GossipState state) =>
            unchecked(TransactionalTickCount.Value - state.CreationTickCount) >= Configuration.AntiEntropyIdleTimeout;

        private bool IsGossipActive(string server) => Shield.InTransaction(() =>
        {
            if (!_gossipStates.TryGetValue(server, out var state) || state.LastSentMsgType == MessageType.End)
                return false;
            if (HasTimedOut(state))
            {
                _gossipStates.Remove(server);
                return false;
            }
            return true;
        });

        private void SpreadRumors()
        {
            using (_logger.BeginScope("Gossip timer run {GossipRunId}", Guid.NewGuid()))
            {
                try
                {
                    Shield.InTransaction(() =>
                    {
                        _logger.LogDebug("Searching for server to gossip with");
                        var servers = Transport.Servers;
                        if (servers == null || !servers.Any())
                        {
                            _logger.LogDebug("No other server known.");
                            return;
                        }
                        var limit = Configuration.AntiEntropyHuntingLimit;
                        var rand = new Random();
                        string server;
                        do
                        {
                            server = servers.Skip(rand.Next(servers.Count)).First();
                        }
                        while (!StartGossip(server) && --limit >= 0);
                        if (limit < 0)
                            _logger.LogDebug("No server found to gossip with.");
                    });
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error on gossip timer task.");
                    RaiseError(new GossipBackendException("Unexpected error on gossip timer task.", ex));
                }
            }
        }

        private bool StartGossip(string server) => Shield.InTransaction(() =>
        {
            if (IsGossipActive(server))
            {
                _logger.LogDebug("Already gossiping with server {ServerId}.", server);
                return false;
            }
            _logger.LogDebug("Starting gossip with server {ServerId}.", server);
            var lastReceivedId = _gossipStates.TryGetValue(server, out var oldState) ? oldState.LastReceivedMsgId : null;
            var toSend = GetPackage(Configuration.AntiEntropyInitialSize, default, null, null, null, out var newWindowStart);
            var msg = new GossipStart
            {
                From = Transport.OwnId,
                DatabaseHash = _freshIndex.DatabaseHash,
                Transactions = toSend.Length == 0 ? null : toSend,
                WindowStart = toSend.Length == 0 || newWindowStart.IsDone ? 0 : newWindowStart.Current.Freshness,
                // GetPackage always returns the latest committed transaction, so WindowEnd is always:
                WindowEnd = _freshIndex.LastFreshness,
                ReplyToId = lastReceivedId,
            };
            _gossipStates[server] = new GossipState(null, null, msg.MessageId, newWindowStart, Configuration.AntiEntropyInitialSize,
                MessageType.Start, oldState?.LastSentMsgType == MessageType.End ? (int?)oldState.LastSentMsgId : null);
            Shield.SideEffect(() =>
            {
                _logger.LogDebug("Sending GossipStart {ServerId}/{MessageId} to {TargetServerId} with {TransactionCount} transactions, window: {WindowStart} - {WindowEnd}",
                    msg.From, msg.MessageId, server, msg.Transactions?.Length ?? 0, msg.WindowStart, msg.WindowEnd);
                Transport.Send(server, msg, true);
            });
            return true;
        });

        private object Transport_MessageHandler(object msg)
        {
            switch (msg)
            {
                case DirectMail directMail:
                    _logger.LogDebug("Direct mail received from {ServerId} with {TransactionCount} transactions.",
                        directMail.From, directMail.Transactions?.Length ?? 0);
                    var (_, waitingStore) = ApplyTransactions(null, directMail.Transactions);
                    if (waitingStore != null && Configuration.DirectMailCausalityFailStartsGossip)
                    {
                        _logger.LogDebug("Direct mail received from {ServerId} has unmet dependencies. Starting gossip with that server.",
                            directMail.From, directMail.Transactions?.Length ?? 0);
                        StartGossip(directMail.From);
                    }
                    return null;

                case GossipMessage gossip:
                    using (_logger.BeginScope("{MessageType} {ServerId}/{MessageId}", gossip.GetType().Name, gossip.From, gossip.MessageId))
                    {
                        _logger.LogDebug("Gossip message received");
                        var pkg = gossip as NewGossip;
                        long? ignoreUpToFreshness = null;
                        HashSet<string> keysToIgnore = null;
                        CausalTransaction[] newWaitingStore = null;
                        if (pkg?.Transactions != null && gossip.DatabaseHash != _freshIndex.DatabaseHash)
                        {
                            Shield.InTransaction(() =>
                            {
                                // we must check LastSentMsgId of the gossip state, because we are only allowed to use the waitingStore if
                                // this is the correct reply. ApplyTransaction optimization where it does not re-compare dependencies of
                                // waitingStore transactions would otherwise be dangerous.
                                var waitingStore = _gossipStates.TryGetValue(gossip.From, out var gossipState) &&
                                    gossipState.LastSentMsgId == gossip.ReplyToId ? gossipState.WaitingStore : null;
                                (keysToIgnore, newWaitingStore) = ApplyTransactions(waitingStore, pkg.Transactions);
                                if (keysToIgnore != null)
                                {
                                    if (!_local.Changes.Any())
                                        ignoreUpToFreshness = _freshIndex.LastFreshness;
                                    else
                                        Shield.SyncSideEffect(() =>
                                        {
                                            // we need the max freshness in case the fields, for which ApplyTransactions claims are now equal
                                            // here as on the other server, change after this transaction.
                                            ignoreUpToFreshness = _freshIndex.LastFreshness;
                                        });
                                }
                            });
                        }
                        return GetReply(gossip, ignoreUpToFreshness, keysToIgnore, newWaitingStore);
                    }

                case KillGossip kill:
                    _logger.LogDebug("KillGossip message from {ServerId}, replying to our message ID {ReplyToId}", kill.From, kill.ReplyToId);
                    Shield.InTransaction(() =>
                    {
                        if (_gossipStates.TryGetValue(kill.From, out var state) && state.LastSentMsgId == kill.ReplyToId)
                            _gossipStates.Remove(kill.From);
                    });
                    return null;

                default:
                    var msgType = msg.GetType();
                    _logger.LogError("Unexpected message type: {MessageType}", msgType);
                    throw new ApplicationException($"Unexpected message type: { msgType }");
            }
        }

        /// <summary>
        /// Applies the given transactions internally, does not cause any direct mail. Applies them
        /// in order as received - we assume they are chronologically ordered. Result contains keys
        /// whose values are (now) equal in our DB to the received values, and the new waitingStore.
        /// </summary>
        /// <param name="waitingStore">Transactions from previous messages in same gossip exchange, which still have unmet dependencies.</param>
        private (HashSet<string>, CausalTransaction[]) ApplyTransactions(CausalTransaction[] waitingStore, params CausalTransaction[] transactions) => Shield.InTransaction(() =>
        {
            if (transactions == null || transactions.Length == 0)
                return (null, waitingStore);
            _logger.LogDebug("Applying {TransactionCount} transactions", transactions.Length);

            bool freshnessUtilized = _currTransaction.GetValueOrDefault()?.Changes != null;
            long freshnessOffset = 0;
            HashSet<string> equalKeys = null;
            List<(TransactionInfo, ChangedEventArgs)> waitingEvents = null;

            var allTransactions = waitingStore == null ? transactions : transactions.Concat(waitingStore);
            var skipTransactions = FindInapplicableTransactions(allTransactions);

            List<CausalTransaction> newWaitingStore = null;
            foreach (var trans in allTransactions)
            {
                if (skipTransactions.Contains(trans))
                {
                    if (newWaitingStore == null)
                        newWaitingStore = new();
                    newWaitingStore.Add(trans);
                }
                else
                {
                    applyTransaction(trans);
                }
            }

            void applyTransaction(CausalTransaction trans)
            {
                if (freshnessUtilized)
                {
                    freshnessUtilized = false;
                    freshnessOffset++;
                }
                var context = _currTransaction.Value = new TransactionInfo { FreshnessOffset = freshnessOffset, DisableMailFx = true };

                foreach (var item in trans.Changes)
                {
                    if (item.Data == null)
                        continue;
                    if (_local.TryGetValue(item.Key, out var curr) &&
                        item.Deleted == curr.Deleted && item.Expired == curr.Expired &&
                        Util.RoughlyEqual(item.ExpiresInMs, curr.ExpiresInMs, Configuration.ExpiryComparePrecision) &&
                        Util.IsByteEqual(curr.Data, item.Data))
                    {
                        if (equalKeys == null)
                            equalKeys = new();
                        equalKeys.Add(item.Key);
                        continue;
                    }

                    // had to specify the type here, otherwise C# weakly infers just "dynamic", which sucks.
                    (ComplexRelationship, ChangedEventArgs) itemResult = SetHelperForApply(item.Key, (dynamic)item.Value, item.Deleted, item.Expired, item.ExpiresInMs);

                    if (itemResult.Item1 == ComplexRelationship.Greater || itemResult.Item1 == ComplexRelationship.Equal ||
                        itemResult.Item1 == ComplexRelationship.EqualButGreater)
                    {
                        // if the received version >= ours - the sender already knows everything about this key.
                        if (equalKeys == null)
                            equalKeys = new();
                        equalKeys.Add(item.Key);
                    }
                    if (!freshnessUtilized && context.Changes != null)
                    {
                        freshnessUtilized = true;
                    }
                    if (itemResult.Item2 != null)
                    {
                        if (waitingEvents == null)
                            waitingEvents = new();
                        waitingEvents.Add((context, itemResult.Item2));
                    }
                }
                if (freshnessUtilized && trans.Dependencies?.Length > 0)
                    AddReceivedDependencies(context, trans.Dependencies);
            }

            if (waitingEvents != null)
            {
                var lastContext = _currTransaction.GetValueOrDefault();
                void extraTracking(string key) => equalKeys?.Remove(key);
                try
                {
                    foreach (var evPair in waitingEvents)
                    {
                        _currTransaction.Value = evPair.Item1;
                        try
                        {
                            evPair.Item1.ExtraTracking = extraTracking;
                            Changed.Raise(this, evPair.Item2);
                        }
                        finally
                        {
                            evPair.Item1.ExtraTracking = null;
                        }
                    }
                }
                finally
                {
                    _currTransaction.Value = lastContext;
                }
            }

            return (equalKeys, newWaitingStore?.ToArray());
        });

        /// <summary>
        /// From a bunch of transactions, determine which will not be applicable due to unmet dependencies.
        /// </summary>
        private HashSet<CausalTransaction> FindInapplicableTransactions(IEnumerable<CausalTransaction> allTransactions)
        {
            // algorithm outline: we will build something like a directed graph of transactions. unmet dependencies are
            // the egdes, going from the depending transaction to the transaction(s) which want(s) to change that key.
            // if any edge ends up pointing outside of the graph, i.e. none of our transactions will change that key,
            // we remove that transaction from the graph, and all others that are pointing to it, transitively.
            // what remains are the transactions with satisfied dependencies, or which only depend on each other.

            // IMPORTANT: this assumes that, for every key these transactions are changing, they will satisfy any
            // dependencies on that key. i.e. there is no newer version of that key. this is true for gossip exchanges
            // because servers always include their newest data in every next message.
            // in case multiple transactions change the same key, only one has that newest data. we will assume if any of
            // them fails (and it must be at least the newest that failed), none of the transactions that depend on that key
            // will be satisfied. but, in fact, some could be, if they depend on an older version, not on the newest.
            // checking that would require, in case a trans points to 2 or more, deserializing the values that each of those
            // wishes to write, to determine which of them it "really" depends on. that would be too much trouble.

            // all the keys we might be changing if we apply these transactions.
            var changingKeys = new HashSet<string>();
            // all the keys we unhappily depend on, with sets of transactions depending on them.
            var depMap = new Dictionary<string, HashSet<CausalTransaction>>();
            // final result - transactions that may not be applied.
            var skipTrans = new HashSet<CausalTransaction>();

            foreach (var trans in allTransactions)
            {
                foreach (var change in trans.Changes)
                    changingKeys.Add(change.Key);
                if (trans.Dependencies == null)
                    continue;
                foreach (var dep in trans.Dependencies)
                {
                    if (IsDependencyMet(dep))
                        continue;
                    if (!depMap.TryGetValue(dep.Key, out var list))
                        depMap[dep.Key] = list = new();
                    list.Add(trans);
                }
            }
            var toProcess = new Queue<string>(depMap.Keys.Where(k => !changingKeys.Contains(k)));
            while (toProcess.Count > 0)
            {
                var key = toProcess.Dequeue();
                var depTransactions = depMap[key];
                foreach (var trans in depTransactions)
                {
                    if (!skipTrans.Add(trans))
                        continue;
                    foreach (var change in trans.Changes)
                    {
                        if (!changingKeys.Remove(change.Key) || !depMap.ContainsKey(change.Key))
                            continue;
                        toProcess.Enqueue(change.Key);
                    }
                }
            }
            return skipTrans;
        }

        /// <summary>
        /// Applies the given items as part of this transaction.
        /// </summary>
        internal void ApplyItems(IEnumerable<MessageItem> items) => Shield.InTransaction(() =>
        {
            List<ChangedEventArgs> waitingEvents = null;
            foreach (var item in items)
            {
                if (item.Data == null)
                    continue;
                if (_local.TryGetValue(item.Key, out var curr) &&
                    item.Deleted == curr.Deleted && item.Expired == curr.Expired &&
                    Util.RoughlyEqual(item.ExpiresInMs, curr.ExpiresInMs, Configuration.ExpiryComparePrecision) &&
                    Util.IsByteEqual(curr.Data, item.Data))
                {
                    continue;
                }

                (ComplexRelationship, ChangedEventArgs) itemResult = SetHelperForApply(item.Key, (dynamic)item.Value, item.Deleted, item.Expired, item.ExpiresInMs);

                if (itemResult.Item2 != null)
                {
                    if (waitingEvents == null)
                        waitingEvents = new();
                    waitingEvents.Add(itemResult.Item2);
                }
            }

            if (waitingEvents != null)
            {
                foreach (var ev in waitingEvents)
                    Changed.Raise(this, ev);
            }
        });

        private (ComplexRelationship, ChangedEventArgs) SetHelperForApply<T>(string key, T val, bool deleted, bool expired, int? expiresInMs) where T : IMergeableEx<T>
        {
            return SetInternal(key, new FieldInfo<T>(val, deleted, expired, expiresInMs));
        }

        private bool IsDependencyMet(Dependency dep)
        {
            var curr = GetItem(dep.Key);
            if (curr == null)
                return false;
            return IsDependencyMetSpec((dynamic)dep.Comparable, (dynamic)curr.Value);
        }

        private bool IsDependencyMetSpec<T, TVersion>(TVersion dep, T current) where T : IMergeableEx<T> where TVersion : IVersion<T> =>
            (dep.CompareWithValue(current) | VectorRelationship.Less) == VectorRelationship.Less;

        private bool ShouldReply(GossipMessage msg, out GossipState currentState, out bool sendKill)
        {
            currentState = null;
            sendKill = false;
            var isStarter = msg is GossipStart;
            var hisReply = msg as GossipReply;
            // if our state is obsolete, we will only accept starter messages.
            if (!_gossipStates.TryGetValue(msg.From, out var state) || HasTimedOut(state))
            {
                if (isStarter)
                {
                    _logger.LogDebug("Message is a starter, and we have no current state. Will reply.");
                    return true;
                }
                else
                {
                    sendKill = hisReply != null;
                    _logger.LogDebug("Message is not a starter, and we have no current state. Will not reply. Will send kill: {SendKill}", sendKill);
                    return false;
                }
            }

            // we have an active state. handling starter messages first.
            if (isStarter)
            {
                // this means he was aware of our last message, whatever it was, and chose to send us this. OK.
                // this may happen if he sent us a GossipEnd before, but we did not receive it (yet).
                if (msg.ReplyToId == state.LastSentMsgId)
                {
                    _logger.LogDebug("An unexpected gossip start, but the ReplyToId {ReplyToId} is a match. Will reply.", msg.ReplyToId);
                    return true;
                }
                else
                {
                    // otherwise, we can only accept a starter if our msg was an end msg, or in case of simultaneous start,
                    // if the other server has higher "prio".
                    var res = state.LastSentMsgType == MessageType.End ||
                        state.LastSentMsgType == MessageType.Start &&
                            StringComparer.InvariantCultureIgnoreCase.Compare(msg.From, Transport.OwnId) < 0;
                    _logger.LogDebug("A conflicting GossipStart. Will reply: {WillReply}", res);
                    return res;
                }
            }

            // he's replying. special case: he's replying to our end message, and we already sent a GossipStart after
            // that end message. we give preference to continuing the old chain in that case.
            if (state.LastSentMsgType == MessageType.Start &&
                state.PreviousSentEndMsgId != null && state.PreviousSentEndMsgId == msg.ReplyToId)
            {
                // when replying to our end message, it must be a GossipReply and he should send us LastWindowStart == 0.
                if (hisReply == null || hisReply.LastWindowStart > 0)
                {
                    _logger.LogError("Reply chain logic failure.");
                    throw new ApplicationException("Reply chain logic failure.");
                }
                _logger.LogDebug("Other side chooses to revive previous gossip exchange. Will reply.");
                return true;
            }
            // otherwise if he's replying to something else, he must send us a correct ReplyToId
            if (state.LastSentMsgId != msg.ReplyToId)
            {
                sendKill = hisReply != null && hisReply.MessageId != state.LastReceivedMsgId;
                _logger.LogDebug("Incorrect ReplyToId {ReplyToId}. Will not reply. Will send kill: {SendKill}", msg.ReplyToId, sendKill);
                return false;
            }

            // so, he's replying. this is just a safety check, to see if the windows match. they will.
            var ourLastStart = hisReply?.LastWindowStart ?? 0;
            if (ourLastStart > 0 && ourLastStart != (state.LastWindowStart.IsDone ? 0 : state.LastWindowStart.Current.Freshness))
            {
                _logger.LogError("Reply chain logic failure.");
                throw new ApplicationException("Reply chain logic failure.");
            }

            // OK, everything checks out
            _logger.LogDebug("Gossip message OK. Will reply.");
            currentState = state;
            return true;
        }

        private object GetReply(GossipMessage replyTo, long? ignoreUpToFreshness, HashSet<string> keysToIgnore,
            CausalTransaction[] waitingTransactions) => Shield.InTransaction<object>(() =>
        {
            var server = replyTo.From;
            var hisNews = replyTo as NewGossip;
            var hisReply = replyTo as GossipReply;
            var hisEnd = replyTo as GossipEnd;

            if (!ShouldReply(replyTo, out var currentState, out var sendKill))
            {
                if (sendKill)
                {
                    _logger.LogDebug("Preparing KillGossip reply to {ServerId}/{MessageId}", replyTo.From, replyTo.MessageId);
                    return new KillGossip { From = Transport.OwnId, ReplyToId = replyTo.MessageId };
                }
                return null;
            }

            var lastWindowStart = hisReply?.LastWindowStart ?? 0;
            var lastWindowEnd = hisReply?.LastWindowEnd ?? hisEnd?.LastWindowEnd;

            var ownHash = _freshIndex.DatabaseHash;
            if (ownHash == replyTo.DatabaseHash)
            {
                if (hisEnd != null)
                {
                    _gossipStates.Remove(server);
                    Shield.SideEffect(() => _logger.LogDebug("Hashes match, gossip completed successfully."));
                    return null;
                }
                else
                    return PrepareEnd(hisNews, currentState?.LastPackageSize ?? 0, true, waitingTransactions);
            }

            var packageSize = currentState == null
                ? Configuration.AntiEntropyInitialSize
                : Math.Max(Configuration.AntiEntropyInitialSize,
                    Math.Min(Configuration.AntiEntropyItemsCutoff, currentState.LastPackageSize * 2));
            var toSend = GetPackage(packageSize,
                lastWindowStart > 0 ? currentState.LastWindowStart : default, lastWindowEnd,
                ignoreUpToFreshness, keysToIgnore, out var newStartEnumerator);

            if (toSend.Length == 0)
            {
                if (hisNews == null)
                {
                    _gossipStates.Remove(server);
                    Shield.SideEffect(() =>
                        _logger.LogWarning("Hashes do not match, but nothing left to gossip about. Completed unsuccessfully."));
                    return null;
                }
                else if (hisNews.Transactions == null || hisNews.Transactions.Length == 0)
                    return PrepareEnd(hisNews, currentState?.LastPackageSize ?? 0, false, waitingTransactions);
                else
                    return PrepareReply(server, new GossipReply
                    {
                        From = Transport.OwnId,
                        DatabaseHash = ownHash,
                        Transactions = null,
                        WindowStart = 0,
                        WindowEnd = _freshIndex.LastFreshness,
                        LastWindowStart = hisNews.WindowStart,
                        LastWindowEnd = hisNews.WindowEnd,
                        ReplyToId = replyTo.MessageId,
                    }, newStartEnumerator, currentState?.LastPackageSize ?? 0, currentState != null, waitingTransactions);
            }

            var windowStart = newStartEnumerator.IsDone ? 0 : newStartEnumerator.Current.Freshness;
            var windowEnd = _freshIndex.LastFreshness;
            return PrepareReply(server, new GossipReply
            {
                From = Transport.OwnId,
                DatabaseHash = ownHash,
                Transactions = toSend,
                WindowStart = windowStart,
                WindowEnd = windowEnd,
                LastWindowStart = hisNews?.WindowStart ?? 0,
                LastWindowEnd = (hisNews?.WindowEnd ?? hisEnd?.WindowEnd).Value,
                ReplyToId = replyTo.MessageId,
            }, newStartEnumerator, packageSize, currentState != null, waitingTransactions);
        });

        private GossipEnd PrepareEnd(NewGossip hisNews, int lastPackageSize, bool success, CausalTransaction[] waitingTransactions)
        {
            var ourHash = _freshIndex.DatabaseHash;
            var maxFresh = _freshIndex.LastFreshness;
            var endMsg = new GossipEnd
            {
                From = Transport.OwnId,
                Success = success,
                DatabaseHash = ourHash,
                WindowEnd = maxFresh,
                LastWindowEnd = hisNews.WindowEnd,
                ReplyToId = hisNews.MessageId,
            };
            // if we're sending GossipEnd, we clear this in transaction, to make sure
            // IsGossipActive is correct, and to guarantee that we actually are done.
            _gossipStates[hisNews.From] = new GossipState(hisNews.MessageId, waitingTransactions, endMsg.MessageId, default, lastPackageSize, MessageType.End);
            Shield.SideEffect(() =>
            {
                if (success)
                    _logger.LogDebug("Prepared GossipEnd reply {ServerId}/{MessageId}, successful.", endMsg.From, endMsg.MessageId);
                else
                    _logger.LogWarning("Prepared GossipEnd reply {ServerId}/{MessageId}, unsuccessful, with {WaitingTransactionCount} transactions with unmet dependencies.",
                        endMsg.From, endMsg.MessageId, waitingTransactions?.Length ?? 0);
            });
            return endMsg;
        }

        private GossipReply PrepareReply(string server, GossipReply msg, ReverseTimeIndex.Enumerator startEnumerator, int newPackageSize,
            bool hasActiveState, CausalTransaction[] waitingTransactions)
        {
            // we try to keep the reply transaction read-only. but if we do not already have an active gossip state, then
            // we must set it consistently, to conflict with any possible concurrent reply or StartGossip process.
            if (!hasActiveState)
            {
                _gossipStates[server] = new GossipState(
                    msg.ReplyToId, waitingTransactions, msg.MessageId, startEnumerator, newPackageSize, MessageType.Reply);
            }
            else
            {
                Shield.SideEffect(() => Shield.InTransaction(() =>
                {
                    _gossipStates[server] = new GossipState(
                        msg.ReplyToId, waitingTransactions, msg.MessageId, startEnumerator, newPackageSize, MessageType.Reply);
                }));
            }
            Shield.SideEffect(() =>
                _logger.LogDebug("Prepared GossipReply {ServerId}/{MessageId} with {ItemCount} items, window: {WindowStart} - {WindowEnd}",
                    msg.From, msg.MessageId, msg.Transactions?.Sum(t => t.Changes?.Length ?? 0) ?? 0, msg.WindowStart, msg.WindowEnd));
            return msg;
        }

        private CausalTransaction[] GetPackage(int packageSize, ReverseTimeIndex.Enumerator lastWindowStart, long? lastWindowEnd,
            long? ignoreUpToFreshness, HashSet<string> keysToIgnore, out ReverseTimeIndex.Enumerator newWindowStart)
        {
            if (packageSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(packageSize), "The size of an anti-entropy package must be greater than zero.");

            static CausalTransaction[] Finalize(List<CausalTransaction> result)
            {
                result.Reverse();
                return result.ToArray();
            }

            var result = new List<CausalTransaction>();
            newWindowStart = _freshIndex.GetCloneableEnumerator();
            // first, if we have a last window, fetch all the new items that happened after lastWindowEnd.
            if (lastWindowEnd != null)
            {
                if (!AddPackagePart(result, ref newWindowStart, lastWindowEnd, null, ignoreUpToFreshness, keysToIgnore))
                    return Finalize(result);

                // and then prepare to continue where we left off last time
                newWindowStart = lastWindowStart;
                if (newWindowStart.IsDone)
                    return Finalize(result);
                // last time we iterated this one, we stopped without adding the current item. let's see if that item is still up to date.
                if (newWindowStart.Current.Item != GetItem(newWindowStart.Current.Item.Key) && !newWindowStart.MoveNext())
                    return Finalize(result);
            }

            // and now, the (next) package of old items
            AddPackagePart(result, ref newWindowStart, null, packageSize, ignoreUpToFreshness, keysToIgnore);
            return Finalize(result);
        }

        // returns true if it added everything - either it reached stopAtFreshness, or it exhausted the enumerator.
        private bool AddPackagePart(List<CausalTransaction> result, ref ReverseTimeIndex.Enumerator enumerator,
            long? stopAtFreshness, int? packageSize, long? ignoreUpToFreshness, HashSet<string> keysToIgnore)
        {
            ReverseTimeIndex.Enumerator prevFreshnessStart = default;
            List<MessageItem> currentChanges = null;
            Dependency[] currentDeps = null;
            var itemsCutoff = Configuration.AntiEntropyItemsCutoff;
            var bytesCutoff = Configuration.AntiEntropyBytesCutoff;
            var bytes = result.Sum(t => t.Changes.Sum(mi => mi.Data.Length));
            var itemCount = result.Sum(t => t.Changes.Length);

            if (!enumerator.IsOpen && !enumerator.MoveNext())
                return true;
            do
            {
                var item = enumerator.Current;
                if (item.Freshness <= stopAtFreshness)
                    break;
                if (prevFreshnessStart.IsDone || prevFreshnessStart.Current.Freshness != item.Freshness)
                {
                    if (currentChanges?.Count > 0)
                    {
                        result.Add(new CausalTransaction
                        {
                            Dependencies = currentDeps,
                            Changes = currentChanges.ToArray(),
                        });
                    }
                    // bytes not checked, cause we do not know if this particular item will get added.
                    if (itemCount >= itemsCutoff || itemCount >= packageSize)
                        return false;
                    prevFreshnessStart = enumerator;
                    currentChanges = new List<MessageItem>();
                    currentDeps = null;
                }
                if (keysToIgnore == null || item.Freshness > ignoreUpToFreshness.Value || !keysToIgnore.Contains(item.Item.Key))
                {
                    bytes += item.Item.Data.Length;
                    if ((itemCount == itemsCutoff || bytes > bytesCutoff) &&
                        // we only stop adding the current transaction if we already have at least one other in result.
                        result.Count > 0)
                    {
                        enumerator = prevFreshnessStart;
                        return false;
                    }
                    itemCount++;
                    currentChanges.Add((MessageItem)item.Item);
                    if (currentChanges.Count == 1)
                    {
                        var deps = GetDependenciesForPackage(item.Freshness, item.Item.Dependencies).ToArray();
                        if (deps.Length > 0)
                            currentDeps = deps;
                    }
                }
            }
            while (enumerator.MoveNext());
            if (currentChanges?.Count > 0)
            {
                result.Add(new CausalTransaction
                {
                    Dependencies = currentDeps,
                    Changes = currentChanges.ToArray(),
                });
            }
            return true;
        }

        private IEnumerable<Dependency> GetDependenciesForPackage(long freshness, StoredDependency[] dependencies)
        {
            foreach (var dep in dependencies)
            {
                var currItem = GetItem(dep.Key);
                // dependencies on something deleted/expired could prevent the receiver from ever applying this transaction.
                // if the item is null, then obviously the dep would never be met. if the item is already Deleted/Expired,
                // it might be pretty old, not in this package, and by the time we get to preparing that package it might be
                // fully removed - thus, again it can never be met.
                // if however, the item exists now, and is not Deleted/Expired, then if it were to become Deleted/Expired
                // later, that will be a new change to it, and will cause it to be included as such in the immediately following
                // message. so, as long as linger timeout is much larger than the gossip timeout, this is safe.
                if (currItem == null || currItem.Deleted || currItem.Expired || currItem.Freshness == freshness)
                    continue;
                yield return new Dependency
                {
                    Key = dep.Key,
                    VersionData = dep.VersionData,
                };
            }
        }

        private TransactionInfo GetOrCreateTrans()
        {
            return _currTransaction.GetValueOrDefault() ?? (_currTransaction.Value = new TransactionInfo());
        }

        /// <summary>
        /// Disables the direct mail message for the current transaction. Throws out of transaction.
        /// </summary>
        public void DisableDirectMail()
        {
            GetOrCreateTrans().DisableMailFx = true;
        }

        /// <summary>
        /// Did someone call <see cref="DisableDirectMail"/> in this transaction.
        /// </summary>
        public bool IsDirectMailDisabled => GetOrCreateTrans().DisableMailFx;

        private void EnlistRead(string key, StoredItem readItem)
        {
            var trans = GetOrCreateTrans();
            if (trans.Reads.Add(key) && readItem != null && !readItem.Deleted && !readItem.Expired)
            {
                trans.Dependencies[key] = readItem.GetOwnDependency();
            }
        }

        private void EnlistWrite<T>(StoredItem oldItem, StoredItem newItem, T value, VersionHash hashEffect) where T : IMergeableEx<T>
        {
            var trans = GetOrCreateTrans();
            newItem.OpenTransaction = trans;
            if (trans.Changes == null)
            {
                trans.Changes = new Dictionary<string, StoredItem>();
                _freshIndex.RegisterTransaction(trans);
                if (!trans.DisableMailFx)
                    Shield.SideEffect(() => DoDirectMail(trans));
            }
            trans.Reads.Add(newItem.Key);
            trans.Changes[newItem.Key] = newItem;
            if (newItem.Deleted || newItem.Expired)
                trans.Dependencies.Remove(newItem.Key);
            else
                trans.Dependencies[newItem.Key] = new StoredDependency { Key = newItem.Key, Comparable = value.GetVersionOnly() };
            if (oldItem != null && oldItem.OpenTransaction != trans)
                CloseDependencies(trans, oldItem);
            trans.HashEffect ^= hashEffect;
            if (trans.ExtraTracking != null)
                trans.ExtraTracking(newItem.Key);
        }

        // only used for fields we write into, since the old version gets lost - pull its dependencies into our transaction.
        private void CloseDependencies(TransactionInfo trans, StoredItem oldItem)
        {
            var deps = (IEnumerable<StoredDependency>)oldItem.OpenTransaction?.Dependencies.Values ?? oldItem.Dependencies;
            if (deps == null)
                return;
            foreach (var dep in deps)
            {
                // the "self" dependency was handled by EnlistWrite, and all Reads by it or EnlistRead.
                if (dep.Key == oldItem.Key || trans.Reads.Contains(dep.Key))
                    continue;
                // don't create dependencies on things that no longer exist, or will not exist soon.
                if (!_local.TryGetValue(dep.Key, out var item) || item.Deleted || item.Expired)
                    continue;
                if (trans.Dependencies.TryGetValue(dep.Key, out var oldDep))
                {
                    // unpleasant, but important - if different keys are pulling in the same dependency, they can easily depend
                    // on different versions of it. we will depend on the merged version. typically, that will just be the greater one,
                    // but if they are actually conflicting versions, we depend on their merge, i.e. we'll depend on both.
                    // note how that merge could be way older than the version dep.Key currently has. this is good.
                    if (oldDep == dep)
                        continue;
                    trans.Dependencies[dep.Key] = DependencyMergeHelper(oldDep, (dynamic)oldDep.Comparable, dep, (dynamic)dep.Comparable);
                }
                else
                {
                    trans.Dependencies.Add(dep.Key, dep);
                }
            }
        }

        private StoredDependency DependencyMergeHelper<TVersion>(StoredDependency depA, TVersion a, StoredDependency depB, TVersion b)
            where TVersion : IMergeable<TVersion>
        {
            var cmp = a.VectorCompare(b);
            if ((cmp | VectorRelationship.Greater) == VectorRelationship.Greater)
                return depA;
            else if (cmp == VectorRelationship.Less)
                return depB;
            return new StoredDependency
            {
                Key = depA.Key,
                Comparable = a.MergeWith(b),
            };
        }

        private void AddReceivedDependencies(TransactionInfo trans, Dependency[] receivedDeps)
        {
            if (receivedDeps == null)
                return;
            foreach (var dep in receivedDeps)
            {
                if (trans.Reads.Contains(dep.Key))
                    continue;
                if (!_local.TryGetValue(dep.Key, out var item) || item.Deleted || item.Expired)
                    continue;
                if (trans.Dependencies.TryGetValue(dep.Key, out var oldDep))
                {
                    trans.Dependencies[dep.Key] = ReceivedDependencyMergeHelper(oldDep, (dynamic)oldDep.Comparable, dep, (dynamic)dep.Comparable);
                }
                else
                {
                    trans.Dependencies.Add(dep.Key, new StoredDependency
                    {
                        Key = dep.Key,
                        VersionData = dep.VersionData,
                    });
                }
            }
        }

        private StoredDependency ReceivedDependencyMergeHelper<TVersion>(StoredDependency depA, TVersion a, Dependency depB, TVersion b)
            where TVersion : IMergeable<TVersion>
        {
            var cmp = a.VectorCompare(b);
            if ((cmp | VectorRelationship.Greater) == VectorRelationship.Greater)
                return depA;
            else if (cmp == VectorRelationship.Less)
                return new StoredDependency
                {
                    Key = depB.Key,
                    VersionData = depB.VersionData,
                };
            return new StoredDependency
            {
                Key = depA.Key,
                Comparable = a.MergeWith(b),
            };
        }

        /// <summary>
        /// Returns true if the backend contains a (non-deleted and non-expired) value under the key.
        /// </summary>
        public bool ContainsKey(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            return GetActiveItemWEnlist(key) != null;
        }

        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        public bool TryGet<T>(string key, out T item) where T : IMergeableEx<T>
        {
            var res = TryGet(key, out var obj);
            item = res ? (T)obj : default;
            return res;
        }

        /// <summary>
        /// Try to read the value under the given key.
        /// </summary>
        public bool TryGet(string key, out object item)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            item = default;
            var mi = GetActiveItemWEnlist(key);
            if (mi == null)
                return false;
            item = mi.Value;
            return true;
        }

        /// <summary>
        /// Returns true if the backend contains a value under the key, including any expired or deleted value
        /// that still lingers.
        /// </summary>
        public bool ContainsKeyWithInfo(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            return GetItemWEnlist(key) != null;
        }

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        public FieldInfo<T> TryGetWithInfo<T>(string key) where T : IMergeableEx<T>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            var mi = GetItemWEnlist(key);
            return mi == null ? null : new FieldInfo<T>(mi);
        }

        /// <summary>
        /// Try to read the value under the given key. Will return deleted and expired values as well,
        /// in case they are still present in the storage for communicating the removal to other servers.
        /// </summary>
        public FieldInfo TryGetWithInfo(string key)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            var mi = GetItemWEnlist(key);
            return mi == null ? null : new FieldInfo(mi);
        }

        internal StoredItem GetItem(string key) => _local.TryGetValue(key, out var mi) ? mi : null;
        internal StoredItem GetItemWEnlist(string key)
        {
            var item = GetItem(key);
            if (Shield.IsInTransaction)
                EnlistRead(key, item);
            return item;
        }

        internal StoredItem GetActiveItem(string key)
        {
            var i = GetItem(key);
            return i != null && IsActive(i) ? i : null;
        }
        internal StoredItem GetActiveItemWEnlist(string key)
        {
            var i = GetItemWEnlist(key);
            return i != null && IsActive(i) ? i : null;
        }

        private static bool IsActive(StoredItem mi) => !mi.Deleted && !mi.Expired && !(mi.ExpiresInMs <= 0);

        /// <summary>
        /// Gets all (non-deleted and non-expired) keys contained in the backend. Will not register as a dependency.
        /// </summary>
        public ICollection<string> Keys => Shield.InTransaction(() => _local.Where(kvp => IsActive(kvp.Value)).Select(kvp => kvp.Key).ToList());

        /// <summary>
        /// Gets all keys contained in the backend, including deleted and expired keys that still linger. Will not register as a dependency.
        /// </summary>
        public ICollection<string> KeysWithInfo => _local.Keys;

        private VersionHash GetHash<TItem>(string key, TItem i) where TItem : IHasVersionBytes
        {
            return VersionHash.Hash(
                new[] { Encoding.UTF8.GetBytes(key) }
                .Concat(i.GetVersionBytes()));
        }

        /// <summary>
        /// A non-update, which ensures that when your local transaction is transmitted to other servers, this
        /// field will be transmitted as well, even if you did not change its value.
        /// </summary>
        /// <param name="key">The key to touch.</param>
        public void Touch(string key) => Shield.InTransaction(() =>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            var mi = GetItem(key);
            if (mi == null)
            {
                EnlistRead(key, null);
                return;
            }
            var newItem = new StoredItem
            {
                Key = key,
                Data = mi.Data,
                Deleted = mi.Deleted,
                Expired = mi.Expired,
                ExpiresInMs = mi.ExpiresInMs,
            };
            _local[key] = newItem;
            EnlistWrite(mi, newItem, (dynamic)newItem.Value, default(VersionHash));
        });

        /// <summary>
        /// Set a value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value.
        /// </summary>
        /// <param name="expireInMs">If given, the item will expire and be removed from the storage in
        /// this many milliseconds. If not null, must be > 0.</param>
        public VectorRelationship Set<T>(string key, T value, int? expireInMs = null) where T : IMergeableEx<T>
            => Shield.InTransaction(() =>
        {
            if (expireInMs <= 0)
                throw new ArgumentOutOfRangeException(nameof(expireInMs));
            var (res, ev) = SetInternal(key, new FieldInfo<T>(value, expireInMs));
            if (ev != null)
                Changed.Raise(this, ev);
            return res.GetValueRelationship();
        });

        private (ComplexRelationship, ChangedEventArgs) SetInternal<T>(string key, FieldInfo<T> value) where T : IMergeableEx<T>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            var oldItem = GetItem(key);
            if (oldItem != null)
            {
                var oldValue = new FieldInfo<T>(oldItem);
                var oldHash = oldItem.Deleted || oldItem.Expired ? default : GetHash(key, oldValue.Value);
                var (mergedValue, cmp) = value.MergeWith(oldValue, Configuration.ExpiryComparePrecision);
                if (cmp == ComplexRelationship.Less || cmp == ComplexRelationship.Equal || cmp == ComplexRelationship.EqualButLess)
                {
                    EnlistRead(key, oldItem);
                    return (cmp, null);
                }
                var newItem = new StoredItem
                {
                    Key = key,
                    Value = mergedValue.Value,
                    Deleted = mergedValue.Deleted,
                    Expired = mergedValue.Expired,
                    ExpiresInMs = mergedValue.ExpiresInMs,
                };
                _local[key] = newItem;
                var hash = oldHash ^ (mergedValue.Deleted || mergedValue.Expired ? default : GetHash(key, mergedValue.Value));
                EnlistWrite(oldItem, newItem, mergedValue.Value, hash);

                var ev = cmp.GetValueRelationship() != VectorRelationship.Equal ? new ChangedEventArgs(key, oldValue.Value, mergedValue.Value, mergedValue.Deleted) : null;
                return (cmp, ev);
            }
            else
            {
                if (value.Deleted || value.Expired)
                {
                    EnlistRead(key, null);
                    return (ComplexRelationship.Equal, null);
                }
                var newItem = new StoredItem
                {
                    Key = key,
                    Value = value.Value,
                    ExpiresInMs = value.ExpiresInMs,
                };
                _local[key] = newItem;
                var hash = GetHash(key, value.Value);
                EnlistWrite(null, newItem, value.Value, hash);

                var ev = value.ExpiresInMs == null || value.ExpiresInMs > 0 ? new ChangedEventArgs(key, null, value.Value, false) : null;
                return (ComplexRelationship.Greater, ev);
            }
        }

        /// <summary>
        /// Remove the given key from the storage.
        /// </summary>
        public bool Remove(string key) => Shield.InTransaction(() =>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));
            var oldItem = GetItem(key);
            if (oldItem == null || oldItem.Deleted)
            {
                EnlistRead(key, oldItem);
                return false;
            }

            var oldVal = (IHasVersionBytes)oldItem.Value;
            var hash = oldItem.Expired ? default : GetHash(key, oldVal);
            var newItem = new StoredItem
            {
                Key = key,
                Data = oldItem.Data,
                Deleted = true,
            };
            _local[key] = newItem;
            EnlistWrite(oldItem, newItem, (dynamic)oldVal, hash);

            if (oldItem.Expired || oldItem.ExpiresInMs <= 0)
                return false;
            OnChanged(key, oldVal, oldVal, true);
            return true;
        });

        private void OnChanged(string key, object oldVal, object newVal, bool deleted)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal, deleted);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired after any key changes. Please note that it also fires during processing of incoming gossip
        /// messages, so, unless you really need to, don't do anything slow here.
        /// </summary>
        public ShieldedEvent<ChangedEventArgs> Changed { get; } = new ShieldedEvent<ChangedEventArgs>();

        public void Dispose()
        {
            _logger.LogInformation("Disposing the backend.");
            Transport.Dispose();
            _gossipTimer.Dispose();
            _deletableTimer.Dispose();
        }

        /// <summary>
        /// An enumerable of keys only read or written into by the current transaction. Includes keys that did not have a value.
        /// </summary>
        public IEnumerable<string> Reads => GetOrCreateTrans().Reads;

        /// <summary>
        /// An enumerable of keys which are considered dependencies of this transaction - all the keys in <see cref="Reads"/>
        /// plus any transitive dependencies of those keys.
        /// </summary>
        public IEnumerable<string> Dependencies => GetOrCreateTrans().Dependencies.Keys;

        /// <summary>
        /// An enumerable of keys written into by the current transaction.
        /// </summary>
        public IEnumerable<string> Changes => GetOrCreateTrans().Changes?.Keys ?? Enumerable.Empty<string>();
    }
}
