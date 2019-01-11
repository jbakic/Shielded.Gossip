﻿using Shielded.Standard;
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
            public readonly bool IsStart;
            public readonly IEnumerator<ReverseTimeIndexItem> LastStart;
            public readonly DateTimeOffset LastSendTime;

            public GossipState(bool isStart, IEnumerator<ReverseTimeIndexItem> lastStart, DateTimeOffset lastSendTime)
            {
                IsStart = isStart;
                LastStart = lastStart;
                LastSendTime = lastSendTime;
            }
        }

        private ShieldedDictNc<string, GossipState> _gossipStates = new ShieldedDictNc<string, GossipState>(StringComparer.InvariantCultureIgnoreCase);

        private bool IsGossipActive(string server) =>
            _gossipStates.TryGetValue(server, out var state) &&
            (DateTimeOffset.UtcNow - state.LastSendTime).TotalMilliseconds < Configuration.AntiEntropyIdleTimeout;

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
            var toSend = GetPackage(Configuration.AntiEntropyInitialTransactions, null, null, out var newWindowStart);
            var msg = new NewGossip
            {
                From = Transport.OwnId,
                DatabaseHash = _databaseHash.Value,
                Items = toSend.Length == 0 ? null : toSend,
                WindowStart = toSend.Length == 0 ? 0 : (newWindowStart?.Current.Freshness ?? 0),
                WindowEnd = toSend.Length == 0 ? _freshIndex.LastFreshness : toSend[0].Freshness
            };
            _gossipStates[server] = new GossipState(true, newWindowStart, msg.Time);
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
                    if (pkg.DatabaseHash != _databaseHash.Value)
                        ApplyItems(pkg.Items, true);
                    SendReply(pkg);
                    break;

                case GossipEnd end:
                    SendReply(end);
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
        /// by descending freshness.
        /// </summary>
        internal void ApplyItems(MessageItem[] items, bool respectFreshness) => Shield.InTransaction(() =>
        {
            if (items == null || items.Length == 0)
                return;
            long prevItemFreshness = items[items.Length - 1].Freshness;
            bool freshnessUtilized = false;
            for (var i = items.Length - 1; i >= 0; i--)
            {
                var item = items[i];
                if (item.Data == null)
                    continue;
                if (_local.TryGetValue(item.Key, out var curr) && IsByteEqual(curr.Data, item.Data))
                    continue;
                if (respectFreshness && prevItemFreshness != item.Freshness)
                {
                    prevItemFreshness = item.Freshness;
                    if (freshnessUtilized)
                        _freshnessContext.Value = _freshnessContext.GetValueOrDefault() + 1;
                    freshnessUtilized = false;
                }
                var obj = item.Value;
                var method = _applyMethods.Get(this, obj.GetType());
                freshnessUtilized |= (method(item.Key, obj) & VectorRelationship.Greater) != 0;
            }
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

        private bool ShouldReply(string server, long? ourLastStart, DateTimeOffset? ourLastTime,
            out IEnumerator<ReverseTimeIndexItem> lastStartEnumerator)
        {
            lastStartEnumerator = null;
            // first, a regular timeout check
            if (ourLastTime != null && (DateTimeOffset.UtcNow - ourLastTime.Value).TotalMilliseconds >= Configuration.AntiEntropyIdleTimeout)
                return false;
            // then, if our state is obsolete, we will only accept starter messages, or replies to GossipEnd (they will
            // also have ourLastStart == null, which makes sense, we sent no items in a GossipEnd.
            if (!_gossipStates.TryGetValue(server, out var state) ||
                (DateTimeOffset.UtcNow - state.LastSendTime).TotalMilliseconds >= Configuration.AntiEntropyIdleTimeout)
            {
                return ourLastStart == null;
            }
            // otherwise, the ourLastStart he sent us must match our remembered last start. the only exception is if both
            // this new message and our last message are starter messages, in which case we reply if we have lower precedence.
            if (ourLastStart == null)
                return state.IsStart && StringComparer.InvariantCultureIgnoreCase.Compare(server, Transport.OwnId) < 0;
            lastStartEnumerator = state.LastStart;
            return ourLastStart.Value == (state.LastStart?.Current.Freshness ?? 0);
        }

        private void SendReply(GossipMessage replyTo) => Shield.InTransaction(() =>
        {
            var server = replyTo.From;
            var hisReply = replyTo as IGossipReply;
            var hisNews = replyTo as NewGossip;

            if (!ShouldReply(replyTo.From, hisReply?.LastWindowStart, hisReply?.LastTime, out var lastStartEnumerator))
                return;

            var ownHash = _databaseHash.Value;
            if (ownHash == replyTo.DatabaseHash)
            {
                // hisNews == null means his message was already a GossipEnd.
                if (hisNews == null)
                    _gossipStates.Remove(server);
                else
                    SendEnd(hisNews, true);
                return;
            }

            var packageSize = hisNews == null || hisNews.Items == null ? Configuration.AntiEntropyInitialTransactions :
                Math.Max(Configuration.AntiEntropyInitialTransactions, hisNews.Items.Length * 2);
            var toSend = GetPackage(packageSize,
                lastStartEnumerator, hisReply?.LastWindowEnd, out var newStartEnumerator);

            if (toSend.Length == 0)
            {
                if (hisNews == null)
                    _gossipStates.Remove(server);
                else if (hisNews.Items == null || hisNews.Items.Length == 0)
                    SendEnd(hisNews, false);
                else
                    SendReply(server, new GossipReply
                    {
                        From = Transport.OwnId,
                        DatabaseHash = ownHash,
                        Items = null,
                        WindowStart = hisReply?.LastWindowStart ?? 0,
                        WindowEnd = hisReply?.LastWindowEnd ?? _freshIndex.LastFreshness,
                        LastWindowStart = hisNews.WindowStart,
                        LastWindowEnd = hisNews.WindowEnd,
                        LastTime = replyTo.Time,
                    }, newStartEnumerator);
                return;
            }

            var windowStart = newStartEnumerator?.Current.Freshness ?? 0;
            var windowEnd = _freshIndex.LastFreshness;
            SendReply(server, new GossipReply
            {
                From = Transport.OwnId,
                DatabaseHash = ownHash,
                Items = toSend,
                WindowStart = windowStart,
                WindowEnd = windowEnd,
                LastWindowStart = hisNews?.WindowStart,
                LastWindowEnd = hisNews?.WindowEnd,
                LastTime = replyTo.Time,
            }, newStartEnumerator);
        });

        private void SendEnd(NewGossip hisNews, bool success)
        {
            // if we're sending GossipEnd, we clear this in transaction, to make sure
            // IsGossipActive is correct, and to guarantee that we actually are done.
            _gossipStates.Remove(hisNews.From);
            var ourHash = _databaseHash.Value;
            Shield.SideEffect(() => Transport.Send(hisNews.From, new GossipEnd
            {
                From = Transport.OwnId,
                Success = success,
                DatabaseHash = ourHash,
                LastWindowStart = hisNews.WindowStart,
                LastWindowEnd = hisNews.WindowEnd,
                LastTime = hisNews.Time,
            }));
        }

        private void SendReply(string server, GossipReply msg, IEnumerator<ReverseTimeIndexItem> startEnumerator)
        {
            Shield.SideEffect(() =>
            {
                // reply transactions are kept read-only since they conflict too easily,
                // and it really makes no difference, whatever we skipped now, we'll see
                // in the next reply. so we change this only in the side-effect.
                Shield.InTransaction(() => _gossipStates[server] = new GossipState(false, startEnumerator, msg.Time));
                Transport.Send(server, msg);
            });
        }

        private MessageItem[] GetPackage(int packageSize, IEnumerator<ReverseTimeIndexItem> lastWindowStart, long? lastWindowEnd,
            out IEnumerator<ReverseTimeIndexItem> newWindowStart)
        {
            if (packageSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(packageSize), "The size of an anti-entropy package must be greater than zero.");
            int cutoff = Configuration.AntiEntropyCutoff;
            long? prevFreshness = null;
            var result = new List<MessageItem>();
            bool interrupted = false;

            newWindowStart = _freshIndex.GetEnumerator();
            while (newWindowStart.MoveNext())
            {
                var item = newWindowStart.Current;
                if (item.Freshness <= lastWindowEnd)
                    break;
                if (prevFreshness == null || prevFreshness.Value != item.Freshness)
                {
                    if (result.Count >= cutoff || (lastWindowEnd == null && result.Count >= packageSize))
                        return result.ToArray();
                    prevFreshness = item.Freshness;
                }
                result.Add(item.Item);
            }
            newWindowStart.Dispose();

            newWindowStart = lastWindowStart;
            if (newWindowStart == null)
                return result.ToArray();

            // last time we iterated this one, we stopped without adding the current item. so this
            // will be a do/while. but first, let's see if that item is still up to date.
            if (newWindowStart.Current.Item != GetItem(newWindowStart.Current.Item.Key))
                newWindowStart.MoveNext();
            do
            {
                var item = newWindowStart.Current;
                if (prevFreshness == null || prevFreshness.Value != item.Freshness)
                {
                    if (result.Count >= cutoff || result.Count >= packageSize)
                    {
                        interrupted = true;
                        break;
                    }
                    prevFreshness = item.Freshness;
                }
                result.Add(item.Item);
            } while (newWindowStart.MoveNext());
            if (!interrupted)
            {
                newWindowStart.Dispose();
                newWindowStart = null;
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
