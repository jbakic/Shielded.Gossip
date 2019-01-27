using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;

namespace Shielded.Gossip
{
    /// <summary>
    /// A backend supporting a key/value store which is distributed using a simple gossip protocol
    /// implementation. Use it in ordinary <see cref="Shield"/> transactions.
    /// Values should be CRDTs, implementing <see cref="IMergeable{T}"/>, or you can use the
    /// <see cref="Multiple{T}"/> and <see cref="Vc{T}"/> wrappers to make them a CRDT. If a type
    /// implements <see cref="IDeletable"/>, it can be deleted from the storage.
    /// </summary>
    public class GossipBackend : IGossipBackend, IDisposable
    {
        private readonly ShieldedDictNc<string, MessageItem> _local = new ShieldedDictNc<string, MessageItem>();
        private readonly ReverseTimeIndex _freshIndex;
        private readonly Shielded<VersionHash> _databaseHash = new Shielded<VersionHash>();
        private readonly ShieldedLocal<bool> _changeLock = new ShieldedLocal<bool>();
        private readonly ShieldedLocal<HashSet<string>> _keysToMail = new ShieldedLocal<HashSet<string>>();

        private readonly Timer _gossipTimer;
        private readonly Timer _deletableTimer;
        private readonly IDisposable _preCommit;

        public readonly ITransport Transport;
        public readonly GossipConfiguration Configuration;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transport">The message transport to use. The backend will dispose it when it gets disposed.</param>
        /// <param name="configuration">The configuration.</param>
        public GossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            Transport = transport;
            Configuration = configuration;
            Transport.MessageReceived += Transport_MessageReceived;

            _freshIndex = new ReverseTimeIndex(GetItem);

            _gossipTimer = new Timer(_ => SpreadRumors(), null, Configuration.GossipInterval, Configuration.GossipInterval);
            _deletableTimer = new Timer(GetDeletableTimerMethod(), null, Configuration.DeletableCleanUpInterval, Configuration.DeletableCleanUpInterval);

            _preCommit = Shield.PreCommit(() => _local.TryGetValue("any", out MessageItem _) || true, () =>
            {
                // so nobody sneaks in a PreCommit after this one, and screws up the fresh index.
                _changeLock.Value = true;
                SyncIndexes();
                if (_keysToMail.HasValue && _keysToMail.Value.Count > 0)
                {
                    var items = _keysToMail.Value
                        .Select(key => _local.TryGetValue(key, out var mi) ? mi : null)
                        .Where(mi => mi != null)
                        .ToArray();
                    Shield.SideEffect(() => DoDirectMail(items));
                }
            });
        }

        private TimerCallback GetDeletableTimerMethod()
        {
            var lastFreshness = new Shielded<long>();
            var lockObj = new object();
            return _ =>
            {
                bool lockTaken = false;
                try
                {
                    Monitor.TryEnter(lockObj, ref lockTaken);
                    if (!lockTaken)
                        return;
                    var (toRemove, freshness) = Shield.InTransaction(() =>
                        (_freshIndex.SkipWhile(i => i.Freshness > lastFreshness)
                            .Where(i => i.Item.Deletable)
                            .Select(i => i.Item)
                            .ToArray(),
                        _freshIndex.LastFreshness));
                    // minor issue - the following transaction will make all the deletable items removable
                    // from the time index, but they won't be actually removed until the next iteration.
                    Shield.InTransaction(() =>
                    {
                        foreach (var item in toRemove)
                            if (_local.TryGetValue(item.Key, out var mi) && mi == item)
                                _local.Remove(item.Key);
                        lastFreshness.Value = freshness;
                    });
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(lockObj);
                }
            };
        }

        private void SyncIndexes()
        {
            _freshIndex.Append(_local.Changes
                .Select(key => _local.TryGetValue(key, out var mi) ? mi : null)
                .Where(mi => mi != null)
                .ToArray());
        }

        private void DoDirectMail(MessageItem[] items)
        {
            if (Configuration.DirectMail == DirectMailType.Off || items.Length == 0)
                return;
            var package = new DirectMail { Items = items };
            if (Configuration.DirectMail == DirectMailType.Always)
            {
                Transport.Broadcast(package);
            }
            else
            {
                foreach (var server in Transport.Servers)
                    SendMail(server, package);
            }
        }

        private void SendMail(string server, DirectMail package)
        {
            if (Configuration.DirectMail == DirectMailType.Always ||
                Configuration.DirectMail == DirectMailType.GossipSupressed && !IsGossipActive(server))
                Transport.Send(server, package);
            else if (Configuration.DirectMail == DirectMailType.StartGossip)
                StartGossip(server);
        }

        private class GossipState
        {
            public readonly DateTimeOffset? LastReceivedTime;
            public readonly DateTimeOffset LastSentTime;
            public readonly ReverseTimeIndex.Enumerator LastWindowStart;
            public readonly int LastPackageSize;
            public readonly bool LastSentMsgWasEnd;

            public GossipState(DateTimeOffset? lastReceivedTime, DateTimeOffset lastSentTime, ReverseTimeIndex.Enumerator lastWindowStart,
                int lastPackageSize, bool lastSentMsgWasEnd = false)
            {
                LastReceivedTime = lastReceivedTime;
                LastSentTime = lastSentTime;
                LastWindowStart = lastWindowStart;
                LastPackageSize = lastPackageSize;
                LastSentMsgWasEnd = lastSentMsgWasEnd;
            }
        }

        private ShieldedDictNc<string, GossipState> _gossipStates = new ShieldedDictNc<string, GossipState>(StringComparer.InvariantCultureIgnoreCase);

        private bool HasGossipTimedOut(DateTimeOffset lastTime, DateTimeOffset? now = null) =>
            ((now ?? DateTimeOffset.UtcNow) - lastTime).TotalMilliseconds >= Configuration.AntiEntropyIdleTimeout;

        private bool IsGossipActive(string server) => Shield.InTransaction(() =>
        {
            if (!_gossipStates.TryGetValue(server, out var state) || state.LastSentMsgWasEnd)
                return false;
            if (HasGossipTimedOut(state.LastSentTime))
            {
                _gossipStates.Remove(server);
                return false;
            }
            return true;
        });

        private void SpreadRumors()
        {
            try
            {
                Shield.InTransaction(() =>
                {
                    var servers = Transport.Servers;
                    if (servers == null || !servers.Any())
                        return;
                    var limit = Configuration.AntiEntropyHuntingLimit;
                    var rand = new Random();
                    string server;
                    do
                    {
                        server = servers.Skip(rand.Next(servers.Count)).First();
                    }
                    while (!StartGossip(server) && --limit >= 0);
                });
            }
            catch { } // TODO
        }

        private bool StartGossip(string server) => Shield.InTransaction(() =>
        {
            if (IsGossipActive(server))
                return false;
            var toSend = GetPackage(Configuration.AntiEntropyInitialSize, default, null, null, null, out var newWindowStart);
            var msg = new NewGossip
            {
                From = Transport.OwnId,
                DatabaseHash = _databaseHash.Value,
                Items = toSend.Length == 0 ? null : toSend,
                WindowStart = toSend.Length == 0 || newWindowStart.IsDefault ? 0 : newWindowStart.Current.Freshness,
                WindowEnd = toSend.Length == 0 ? _freshIndex.LastFreshness : toSend[0].Freshness
            };
            _gossipStates[server] = new GossipState(null, msg.Time, newWindowStart, Configuration.AntiEntropyInitialSize);
            Shield.SideEffect(() => Transport.Send(server, msg));
            return true;
        });

        private void Transport_MessageReceived(object sender, object msg)
        {
            switch (msg)
            {
                case DirectMail trans:
                    ApplyItems(trans.Items, true);
                    break;

                case NewGossip pkg:
                    var reply = pkg as GossipReply;
                    if (!ShouldAcceptMsg(pkg.From, pkg.Time, reply?.LastWindowStart, reply?.LastTime, out var currentState1))
                        return;
                    long? ignoreUpToFreshness = null;
                    HashSet<string> keysToIgnore = null;
                    if (pkg.Items != null && pkg.DatabaseHash != _databaseHash.Value)
                    {
                        Shield.InTransaction(() =>
                        {
                            keysToIgnore = ApplyItems(pkg.Items, true);
                            if (keysToIgnore != null)
                                Shield.SyncSideEffect(() =>
                                {
                                    // this happens if we receive a msg just as someone disposed us! the pre-commit is
                                    // not subscribed any more.
                                    if (!_changeLock.HasValue)
                                    {
                                        keysToIgnore = null;
                                    }
                                    else
                                    {
                                        // we don't want to send back the same things we just received. so, ignore all keys from
                                        // the incoming msg for which the result of application was Greater, which means our
                                        // local value is now identical to the received one, unless they also appear in _keysToMail,
                                        // which means a Changed handler made further changes to them.
                                        // we need the max freshness in case these fields change after this transaction.
                                        ignoreUpToFreshness = _freshIndex.LastFreshness;
                                        if (_keysToMail.HasValue)
                                            keysToIgnore.ExceptWith(_keysToMail.Value);
                                    }
                                });
                            else
                                keysToIgnore = null;
                        });
                    }
                    SendReply(pkg, currentState1, ignoreUpToFreshness, keysToIgnore);
                    break;

                case GossipEnd end:
                    if (!ShouldAcceptMsg(end.From, end.Time, null, end.LastTime, out var currentState2))
                        return;
                    SendReply(end, currentState2);
                    break;
            }
        }

        private readonly ApplyMethods _applyMethods = new ApplyMethods(typeof(GossipBackend)
            .GetMethod("SetInternal", BindingFlags.Instance | BindingFlags.NonPublic));

        private static readonly ShieldedLocal<long> _freshnessContext = new ShieldedLocal<long>();

        /// <summary>
        /// Applies the given items internally, does not cause any direct mail. Applies them
        /// starting from the last, and if they have different Freshness values, they will be
        /// indexed in this backend with different values too. It is assumed they are sorted
        /// by descending freshness. Result contains keys whose values are (now) equal in our
        /// DB to the received values.
        /// </summary>
        internal HashSet<string> ApplyItems(MessageItem[] items, bool respectFreshness) => Shield.InTransaction(() =>
        {
            if (items == null || items.Length == 0)
                return null;
            long prevItemFreshness = items[items.Length - 1].Freshness;
            bool freshnessUtilized = false;
            HashSet<string> equalKeys = null;
            for (var i = items.Length - 1; i >= 0; i--)
            {
                var item = items[i];
                if (item.Data == null)
                    continue;
                if (_local.TryGetValue(item.Key, out var curr) && IsByteEqual(curr.Data, item.Data))
                {
                    if (equalKeys == null)
                        equalKeys = new HashSet<string>();
                    equalKeys.Add(item.Key);
                    continue;
                }
                if (respectFreshness && prevItemFreshness != item.Freshness)
                {
                    prevItemFreshness = item.Freshness;
                    if (freshnessUtilized)
                        _freshnessContext.Value = _freshnessContext.GetValueOrDefault() + 1;
                    freshnessUtilized = false;
                }
                var obj = item.Value;
                var method = _applyMethods.Get(this, obj.GetType());
                var itemResult = method(item.Key, obj);
                freshnessUtilized |= (itemResult & VectorRelationship.Greater) != 0;
                if (itemResult == VectorRelationship.Greater || itemResult == VectorRelationship.Equal)
                {
                    if (equalKeys == null)
                        equalKeys = new HashSet<string>();
                    equalKeys.Add(item.Key);
                }
            }
            return equalKeys;
        });

        private static bool IsByteEqual(byte[] one, byte[] two)
        {
            if (one == null && two == null)
                return true;
            if (one == null || two == null || one.Length != two.Length)
                return false;
            var len = one.Length;
            for (int i = 0; i < len; i++)
                if (one[i] != two[i])
                    return false;
            return true;
        }

        private bool ShouldAcceptMsg(string server, DateTimeOffset hisTime, long? ourLastStart, DateTimeOffset? ourLastTime,
            out GossipState currentState)
        {
            currentState = null;
            // first, a regular RTT timeout check
            var now = DateTimeOffset.UtcNow;
            if (ourLastTime != null && HasGossipTimedOut(ourLastTime.Value, now))
                return false;
            // then, if our state is obsolete, we will only accept starter messages.
            if (!_gossipStates.TryGetValue(server, out var state) || HasGossipTimedOut(state.LastSentTime, now))
            {
                return ourLastTime == null;
            }

            // here we have a state. we will cover all possible cases separately.
            // did we previously send him a gossip start message?
            if (state.LastReceivedTime == null)
            {
                // is he replying to it? if yes, the times will match.
                if (ourLastTime == state.LastSentTime)
                {
                    // if he's sending a GossipEnd, ourLastStart will be null. that's OK.
                    if (ourLastStart == null)
                        return true;
                    // otherwise, we expect the LastWindowStart he sent us to be correct.
                    else if (ourLastStart != (state.LastWindowStart.IsDefault ? 0 : state.LastWindowStart.Current.Freshness))
                        throw new ApplicationException("Reply chain logic failure.");
                    currentState = state;
                    return true;
                }
                else
                {
                    // if he's not replying, then we will only answer if it's his start msg. it means we're starting
                    // an exchange in parallel, and the higher prio server will win.
                    return ourLastTime == null && StringComparer.InvariantCultureIgnoreCase.Compare(server, Transport.OwnId) < 0;
                }
            }
            // so, we last replied to something.
            // if this message of his is older than or the same one as the one we already replied to, then we ignore it.
            else if (state.LastReceivedTime >= hisTime)
            {
                return false;
            }
            else
            {
                // this is a weird case - his message Time is newer than the LastReceivedTime, and yet, instead of
                // replying to our last msg to him, he's sending a new gossip start message... we may reply to this,
                // since by sending this message to us he already changed his state and will insist on us replying to this.
                if (ourLastTime == null)
                {
                    return true;
                }
                // if here, then we replied to him, and he's replying to us. we only answer to replies to our latest message.
                else if (ourLastTime != state.LastSentTime)
                {
                    return false;
                }
                else
                {
                    // if he's sending a GossipEnd, ourLastStart will be null. that's OK.
                    if (ourLastStart == null)
                        return true;
                    // otherwise, he is replying to our latest message. here, again, we expect the window starts to match.
                    else if ((ourLastStart ?? 0) != (state.LastWindowStart.IsDefault ? 0 : state.LastWindowStart.Current.Freshness))
                        throw new ApplicationException("Reply chain logic failure.");
                    currentState = state;
                    return true;
                }
            }
        }

        private void SendReply(GossipMessage replyTo, GossipState currentState,
            long? ignoreUpToFreshness = null, HashSet<string> keysToIgnore = null) => Shield.InTransaction(() =>
        {
            var server = replyTo.From;
            var hisNews = replyTo as NewGossip;
            var hisReply = replyTo as GossipReply;
            var hisEnd = replyTo as GossipEnd;

            var lastWindowStart = hisReply?.LastWindowStart;
            var lastWindowEnd = hisReply?.LastWindowEnd ?? hisEnd?.LastWindowEnd;

            var ownHash = _databaseHash.Value;
            if (ownHash == replyTo.DatabaseHash)
            {
                if (hisEnd != null)
                    _gossipStates.Remove(server);
                else
                    SendEnd(hisNews, currentState?.LastPackageSize ?? 0, true);
                return;
            }

            var packageSize = currentState == null
                ? Configuration.AntiEntropyInitialSize
                : Math.Max(Configuration.AntiEntropyInitialSize,
                    (int)Math.Min(Configuration.AntiEntropyCutoff, currentState.LastPackageSize * 2));
            var toSend = GetPackage(packageSize, currentState?.LastWindowStart ?? default, lastWindowEnd,
                ignoreUpToFreshness, keysToIgnore, out var newStartEnumerator);

            if (toSend.Length == 0)
            {
                if (hisNews == null)
                    _gossipStates.Remove(server);
                else if (hisNews.Items == null || hisNews.Items.Length == 0)
                    SendEnd(hisNews, currentState?.LastPackageSize ?? 0, false);
                else
                    SendReply(server, new GossipReply
                    {
                        From = Transport.OwnId,
                        DatabaseHash = ownHash,
                        Items = null,
                        WindowStart = 0,
                        WindowEnd = _freshIndex.LastFreshness,
                        LastWindowStart = hisNews.WindowStart,
                        LastWindowEnd = hisNews.WindowEnd,
                        LastTime = replyTo.Time,
                    }, newStartEnumerator, currentState?.LastPackageSize ?? 0);
                return;
            }

            var windowStart = newStartEnumerator.IsDefault ? 0 : newStartEnumerator.Current.Freshness;
            var windowEnd = _freshIndex.LastFreshness;
            SendReply(server, new GossipReply
            {
                From = Transport.OwnId,
                DatabaseHash = ownHash,
                Items = toSend,
                WindowStart = windowStart,
                WindowEnd = windowEnd,
                LastWindowStart = hisNews?.WindowStart ?? 0,
                LastWindowEnd = (hisNews?.WindowEnd ?? hisEnd?.WindowEnd).Value,
                LastTime = replyTo.Time,
            }, newStartEnumerator, packageSize);
        });

        private void SendEnd(NewGossip hisNews, int lastPackageSize, bool success)
        {
            // if we're sending GossipEnd, we clear this in transaction, to make sure
            // IsGossipActive is correct, and to guarantee that we actually are done.
            var ourHash = _databaseHash.Value;
            var maxFresh = _freshIndex.LastFreshness;
            var endMsg = new GossipEnd
            {
                From = Transport.OwnId,
                Success = success,
                DatabaseHash = ourHash,
                WindowEnd = maxFresh,
                LastWindowEnd = hisNews.WindowEnd,
                LastTime = hisNews.Time,
            };
            _gossipStates[hisNews.From] = new GossipState(hisNews.Time, endMsg.Time, default, lastPackageSize, true);
            Shield.SideEffect(() => Transport.Send(hisNews.From, endMsg));
        }

        private void SendReply(string server, GossipReply msg, ReverseTimeIndex.Enumerator startEnumerator, int newPackageSize)
        {
            Shield.SideEffect(() =>
            {
                // reply transactions are kept read-only since they conflict too easily,
                // and it really makes no difference, whatever we skipped now, we'll see
                // in the next reply. so we change this only in the side-effect.
                Shield.InTransaction(() => _gossipStates[server] = new GossipState(
                    msg.LastTime, msg.Time, startEnumerator, newPackageSize));
                Transport.Send(server, msg);
            });
        }

        private MessageItem[] GetPackage(int packageSize, ReverseTimeIndex.Enumerator lastWindowStart, long? lastWindowEnd,
            long? ignoreUpToFreshness, HashSet<string> keysToIgnore, out ReverseTimeIndex.Enumerator newWindowStart)
        {
            if (packageSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(packageSize), "The size of an anti-entropy package must be greater than zero.");
            int cutoff = Configuration.AntiEntropyCutoff;
            ReverseTimeIndex.Enumerator prevFreshnessStart = default;
            var result = new List<MessageItem>();
            bool interrupted = false;

            newWindowStart = _freshIndex.GetCloneableEnumerator();
            while (newWindowStart.MoveNext())
            {
                var item = newWindowStart.Current;
                if (item.Freshness <= lastWindowEnd)
                    break;
                if (prevFreshnessStart.IsDefault || prevFreshnessStart.Current.Freshness != item.Freshness)
                {
                    if (result.Count >= cutoff || (lastWindowEnd == null && result.Count >= packageSize))
                        return result.ToArray();
                    prevFreshnessStart = newWindowStart;
                }
                if (keysToIgnore == null || item.Freshness > ignoreUpToFreshness.Value || !keysToIgnore.Contains(item.Item.Key))
                {
                    if (result.Count == cutoff && !prevFreshnessStart.IsDefault)
                    {
                        newWindowStart = prevFreshnessStart;
                        var index = result.FindIndex(mi => mi.Freshness == item.Freshness);
                        result.RemoveRange(index, result.Count - index);
                        return result.ToArray();
                    }
                    result.Add(item.Item);
                }
            }
            newWindowStart.Dispose();

            newWindowStart = lastWindowStart;
            if (newWindowStart.IsDefault)
                return result.ToArray();

            // last time we iterated this one, we stopped without adding the current item. so this
            // will be a do/while. but first, let's see if that item is still up to date.
            if (newWindowStart.Current.Item != GetItem(newWindowStart.Current.Item.Key) && !newWindowStart.MoveNext())
            {
                newWindowStart.Dispose();
                newWindowStart = default;
                return result.ToArray();
            }
            do
            {
                var item = newWindowStart.Current;
                if (prevFreshnessStart.IsDefault || prevFreshnessStart.Current.Freshness != item.Freshness)
                {
                    if (result.Count >= cutoff || result.Count >= packageSize)
                    {
                        interrupted = true;
                        break;
                    }
                    prevFreshnessStart = newWindowStart;
                }
                if (result.Count == cutoff && !prevFreshnessStart.IsDefault)
                {
                    newWindowStart = prevFreshnessStart;
                    var index = result.FindIndex(mi => mi.Freshness == item.Freshness);
                    result.RemoveRange(index, result.Count - index);
                    return result.ToArray();
                }
                result.Add(item.Item);
            } while (newWindowStart.MoveNext());
            if (!interrupted)
            {
                newWindowStart.Dispose();
                newWindowStart = default;
            }
            return result.ToArray();
        }

        /// <summary>
        /// Tries to read the value under the given key. The type of the value must be a CRDT.
        /// </summary>
        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem>
        {
            item = default;
            if (!_local.TryGetValue(key, out MessageItem i))
                return false;
            item = (TItem)i.Value;
            return true;
        }

        internal MessageItem GetItem(string key)
        {
            return _local.TryGetValue(key, out MessageItem i) ? i : null;
        }

        private VersionHash GetHash<TItem>(string key, TItem i) where TItem : IHasVersionHash
        {
            return VersionHash.Hash(
                Encoding.UTF8.GetBytes(key),
                i.GetVersionHash().GetBytes());
        }

        /// <summary>
        /// A non-update, which ensures that when your local transaction is transmitted to other servers, this
        /// field will be transmitted as well, even if you did not change its value.
        /// </summary>
        /// <param name="key">The key to touch.</param>
        public void Touch(string key) => Shield.InTransaction(() =>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            if (!_local.TryGetValue(key, out var mi))
                return;
            _local[key] = new MessageItem
            {
                Key = key,
                Data = mi.Data,
                Deletable = mi.Deletable,
                FreshnessOffset = _freshnessContext.GetValueOrDefault()
            };
            var set = _keysToMail.HasValue ? _keysToMail.Value : (_keysToMail.Value = new HashSet<string>());
            set.Add(key);
        });

        /// <summary>
        /// Sets the given value under the given key, merging it with any already existing value
        /// there. Returns the relationship of the new to the old value, or
        /// <see cref="VectorRelationship.Greater"/> if there is no old value. The storage gets affected
        /// only if the result of comparison is Greater or Conflict.
        /// </summary>
        public VectorRelationship Set<TItem>(string key, TItem val) where TItem : IMergeable<TItem>
            => Shield.InTransaction(() =>
        {
            var res = SetInternal(key, val);
            if ((res & VectorRelationship.Greater) == 0)
                return res;
            var set = _keysToMail.HasValue ? _keysToMail.Value : (_keysToMail.Value = new HashSet<string>());
            set.Add(key);
            return res;
        });

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem>
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("key");
            if (val == null)
                throw new ArgumentNullException("val");
            if (_changeLock.GetValueOrDefault())
                throw new InvalidOperationException("Changes are blocked at this time.");
            if (_local.TryGetValue(key, out MessageItem oldItem))
            {
                var oldVal = (TItem)oldItem.Value;
                var cmp = val.VectorCompare(oldVal);
                if (cmp == VectorRelationship.Less || cmp == VectorRelationship.Equal)
                    return cmp;
                // we support this only for safety - a CanDelete should never accept any changes, nor switch to !CanDelete.
                var oldDeletable = oldVal is IDeletable oldDel && oldDel.CanDelete;
                var oldHash = oldDeletable ? default : GetHash(key, oldVal);
                // in case someone screws up the MergeWith impl, we call it after extracting the critical info above.
                val = oldVal.MergeWith(val);
                if (val == null)
                    throw new ApplicationException("IMergeable.MergeWith should not return null for non-null arguments.");

                var deletable = val is IDeletable del && del.CanDelete;
                _local[key] = new MessageItem
                {
                    Key = key,
                    Value = val,
                    Deletable = deletable,
                    FreshnessOffset = _freshnessContext.GetValueOrDefault(),
                };
                var hash = oldHash ^ (deletable ? default : GetHash(key, val));
                _databaseHash.Commute((ref VersionHash h) => h ^= hash);

                OnChanged(key, oldVal, val);
                return cmp;
            }
            else
            {
                if (val is IDeletable del && del.CanDelete)
                    return VectorRelationship.Equal;
                _local[key] = new MessageItem
                {
                    Key = key,
                    Value = val,
                    FreshnessOffset = _freshnessContext.GetValueOrDefault(),
                };
                var hash = GetHash(key, val);
                _databaseHash.Commute((ref VersionHash h) => h ^= hash);

                OnChanged(key, default(TItem), val);
                return VectorRelationship.Greater;
            }
        }

        private void OnChanged(string key, object oldVal, object newVal)
        {
            var ev = new ChangedEventArgs(key, oldVal, newVal);
            Changed.Raise(this, ev);
        }

        /// <summary>
        /// Fired when any key changes.
        /// </summary>
        public readonly ShieldedEvent<ChangedEventArgs> Changed = new ShieldedEvent<ChangedEventArgs>();

        public void Dispose()
        {
            Transport.Dispose();
            _gossipTimer.Dispose();
            _deletableTimer.Dispose();
            _preCommit.Dispose();
        }

        /// <summary>
        /// An enumerable of keys read or written into by the current transaction. Includes
        /// keys that did not have a value.
        /// </summary>
        public IEnumerable<string> Reads => _local.Reads;

        /// <summary>
        /// An enumerable of keys written into by the current transaction.
        /// </summary>
        public IEnumerable<string> Changes => _local.Changes;
    }
}
