using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public class ConsistentGossipBackend : IBackend, IDisposable
    {
        private readonly GossipBackend _wrapped;

        public ITransport Transport => _wrapped.Transport;
        public GossipConfiguration Configuration => _wrapped.Configuration;

        public ConsistentGossipBackend(ITransport transport, GossipConfiguration configuration)
        {
            _wrapped = new GossipBackend(transport, configuration);
            _wrapped.Changing += _wrapped_Changing;
        }

        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            key = WrapPublicKey(key);
            return TryGetInternal(key, out item);
        }

        public bool TryGetInternal<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            if (!Distributed.IsConsistent)
                return _wrapped.TryGet(key, out item);
            item = default;
            var local = _state.GetValueOrDefault()?.Changes;
            if (local != null && local.TryGetValue(key, out TransactionItem i))
            {
                item = (TItem)Serializer.Deserialize(i.Data);
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

        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            key = WrapPublicKey(key);
            if (!Distributed.IsConsistent)
                return _wrapped.Set(key, item);
            return SetInternal(key, item);
        }

        public VectorRelationship SetVersion<TItem>(string key, TItem item) where TItem : IHasVectorClock
        {
            return Set(key, (Multiple<TItem>)item);
        }

        private VectorRelationship SetInternal<TItem>(string key, TItem val) where TItem : IMergeable<TItem, TItem>
        {
            Enlist();
            var cmp = VectorRelationship.Less;
            if (TryGetInternal(key, out TItem oldVal))
            {
                cmp = oldVal.VectorCompare(val);
                if (cmp == VectorRelationship.Greater || cmp == VectorRelationship.Equal)
                    return cmp;
                val = oldVal.MergeWith(val);
            }

            if (OnChanging(key, oldVal, val))
                return VectorRelationship.Greater;

            var local = _state.Value.Changes;
            // cmp and Expected can both be either Less or Conflict, and Less & Conflict == Less.
            var expected = local.TryGetValue(key, out var oldItem) ? oldItem.Expected & cmp : cmp;
            local[key] = new TransactionItem { Key = key, Data = Serializer.Serialize(val), Expected = expected };
            return cmp;
        }

        private class BackendState
        {
            public ConsistentGossipBackend Self;
            public string TransactionId;
            public Dictionary<string, TransactionItem> Changes;
            public ShieldedDict<string, object> Blocked = new ShieldedDict<string, object>();
            public TaskCompletionSource<bool> PrepareCompleter = new TaskCompletionSource<bool>();
            public TaskCompletionSource<object> Committer = new TaskCompletionSource<object>();

            public BackendState(string id, ConsistentGossipBackend self, IEnumerable<TransactionItem> changes = null)
            {
                Self = self;
                TransactionId = id;
                Changes = changes?.ToDictionary(ti => ti.Key) ?? new Dictionary<string, TransactionItem>();
            }

            public void Complete(bool res)
            {
                Shield.InTransaction(() => { Self._transactions.Remove(TransactionId); });
                Self.UnblockGossip(this);
                PrepareCompleter.TrySetResult(res);
                Committer.TrySetResult(null);
            }
        }

        private readonly ShieldedLocal<BackendState> _state = new ShieldedLocal<BackendState>();

        private void Enlist()
        {
            Distributed.EnlistBackend(this);
            if (!_state.HasValue)
            {
                // a hack to keep the transaction open.
                new Shielded<int>().Value = 1;
                _state.Value = new BackendState(WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString()), this);
            }
        }

        private bool OnChanging(string key, object oldVal, object newVal)
        {
            var ch = Changing;
            if (ch == null)
                return false;
            var ev = new ChangingEventArgs(key, oldVal, newVal);
            ch.Invoke(this, ev);
            return ev.Cancel;
        }

        public event EventHandler<ChangingEventArgs> Changing;

        private readonly ShieldedDictNc<string, BackendState> _transactions = new ShieldedDictNc<string, BackendState>();
        private readonly ShieldedDictNc<string, BackendState> _fieldBlockers = new ShieldedDictNc<string, BackendState>();

        async Task<bool> IBackend.Prepare(CommitContinuation cont)
        {
            BackendState ourState = null;
            cont.InContext(() =>
            {
                ourState = _state.Value;
                Shield.SideEffect(null, () => ourState.Complete(false));
            });
            if (ourState.Changes == null || !ourState.Changes.Any())
                return true;

            var transaction = new TransactionInfo
            {
                Initiator = Transport.OwnId,
                Items = ourState.Changes.Values.ToArray(),
                State = new TransactionVector(
                    new[] { new VectorItem<TransactionState>(Transport.OwnId, TransactionState.None) }
                    .Concat(Transport.Servers.Select(s => new VectorItem<TransactionState>(s, TransactionState.None)))
                    .ToArray()),
            };
            var id = ourState.TransactionId;
            Shield.InTransaction(() => _transactions.Add(id, ourState));
            if (!await PrepareInternal(id, ourState, transaction))
            {
                ourState.Complete(false);
                return false;
            }
            await Distributed.Run(() => _wrapped.Set(id, transaction));
            return await ourState.PrepareCompleter.Task;
        }

        int GetNewTimeout()
        {
            var rnd = new Random();
            return rnd.Next(Configuration.ConsistentPrepareTimeoutRange.Min, Configuration.ConsistentPrepareTimeoutRange.Max);
        }

        async Task<bool> PrepareInternal(string id, BackendState ourState, TransactionInfo newInfo = null)
        {
            var lockTask = BlockGossip(ourState);
            // "slaves" always wait for max timeout.
            var timeout = newInfo != null ? GetNewTimeout() : Configuration.ConsistentPrepareTimeoutRange.Max;
            var resTask = await Task.WhenAny(lockTask, Task.Delay(timeout));
            return resTask == lockTask && lockTask.Result &&
                Check(id, newInfo);
        }

        private async Task<bool> BlockGossip(BackendState ourState)
        {
            var prepareTask = ourState.PrepareCompleter.Task;
            foreach (var key in ourState.Changes.Keys.OrderBy(k => k, StringComparer.InvariantCulture))
            {
                Task nextWait = null;
                while ((nextWait = Shield.InTransaction(() =>
                {
                    if (_fieldBlockers.TryGetValue(key, out var someState) &&
                        // we won't wait twice, in case someone forgot to remove himself.
                        someState.Committer.Task != nextWait)
                    {
                        return someState.Committer.Task;
                    }
                    _fieldBlockers[key] = ourState;
                    return null;
                })) != null)
                {
                    try
                    {
                        if (await Task.WhenAny(nextWait, prepareTask) == prepareTask)
                            return false;
                    }
                    catch { }
                }
            }
            return true;
        }

        private readonly ApplyMethods _unblockMethods = new ApplyMethods(
            typeof(GossipBackend).GetMethod("Set", BindingFlags.Instance | BindingFlags.Public));

        private void UnblockGossip(BackendState ourState)
        {
            Distributed.RunLocal(() =>
            {
                foreach (var key in ourState.Changes.Keys)
                    if (_fieldBlockers.TryGetValue(key, out BackendState state) && state == ourState)
                        _fieldBlockers.Remove(key);
                foreach (var kvp in ourState.Blocked)
                {
                    var setter = _unblockMethods.Get(_wrapped, kvp.Value.GetType());
                    setter(kvp.Key, kvp.Value);
                }
            });
        }

        private readonly ApplyMethods _compareMethods = new ApplyMethods(
            typeof(ConsistentGossipBackend).GetMethod("CheckOne", BindingFlags.NonPublic | BindingFlags.Instance));

        private VectorRelationship CheckOne<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            if (!_wrapped.TryGet(key, out TItem current))
                return VectorRelationship.Less;
            return current.VectorCompare(item);
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
                        var obj = Serializer.Deserialize(item.Data);
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

        private void _wrapped_Changing(object sender, ChangingEventArgs e)
        {
            if (e.Key.StartsWith(TransactionPfx))
            {
                OnTransactionChanging(e.Key, (TransactionInfo)e.OldValue, (TransactionInfo)e.NewValue);
            }
            else if (e.Key.StartsWith(PublicPfx))
            {
                var merger = _blockMergers.Get(this, e.NewValue.GetType());
                if (merger(e.Key, e.NewValue) == VectorRelationship.Greater ||
                    OnChanging(e.Key.Substring(PublicPfx.Length), e.OldValue, e.NewValue))
                    e.Cancel = true;
            }
        }

        private readonly ApplyMethods _blockMergers = new ApplyMethods(
            typeof(ConsistentGossipBackend).GetMethod("MergeWithBlocked", BindingFlags.NonPublic | BindingFlags.Instance));

        private VectorRelationship MergeWithBlocked<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            if (!_fieldBlockers.TryGetValue(key, out var state))
                return VectorRelationship.Less;
            state.Blocked.TryGetValue(key, out var current);
            state.Blocked[key] = current != null ? ((TItem)current).MergeWith(item) : item;
            return VectorRelationship.Greater;
        }

        private void OnTransactionChanging(string id, TransactionInfo oldVal, TransactionInfo newVal)
        {
            if (oldVal == null)
            {
                if (newVal == null)
                    throw new ApplicationException("Both old and new transaction info are null.");
                if (_transactions.ContainsKey(id))
                    return;
                var ourState = new BackendState(id, this, newVal.Items);
                _transactions.Add(id, ourState);
                Shield.SideEffect(async () =>
                {
                    if (!await PrepareInternal(id, ourState))
                    {
                        ourState.Complete(false);
                        await SetFail(id);
                        return;
                    }
                    await SetPrepared(id);
                });
            }
            else if (newVal == null)
            {
                // TODO: currently we never remove a transaction. or anything else, for that matter.
            }
            else // both != null
            {
                Shield.SideEffect(async () =>
                {
                    await OnStateChange(id);
                });
            }
        }

        private async Task SetPrepared(string id)
        {
            await Distributed.Run(() =>
            {
                if (!_wrapped.TryGet(id, out TransactionInfo current) || current.State[Transport.OwnId] != TransactionState.None)
                    return;
                _wrapped.DirectMailRestriction.Value = current.Initiator;
                _wrapped.Set(id, current.WithState(current.State.Modify(Transport.OwnId, TransactionState.Prepared)));
            });
        }

        private async Task SetFail(string id)
        {
            await Distributed.Run(() =>
            {
                if (!_wrapped.TryGet(id, out TransactionInfo current) || current.State[Transport.OwnId] != TransactionState.None)
                    return;
                _wrapped.Set(id, current.WithState(current.State.Modify(Transport.OwnId, TransactionState.Fail)));
            });
        }

        private async Task OnStateChange(string id)
        {
            await Distributed.Run(() =>
            {
                if (!_transactions.TryGetValue(id, out var ourState))
                    return;
                if (!_wrapped.TryGet(id, out TransactionInfo current) || current.State.Items.Any(s => s.Value == TransactionState.Fail))
                {
                    _transactions.Remove(id);
                    Shield.SideEffect(() => ourState.Complete(false));
                }
                else if (current.State.Without(Transport.OwnId).All(s => s.Value == TransactionState.Prepared) &&
                    current.State[Transport.OwnId] == TransactionState.None)
                {
                    _transactions.Remove(id);
                    Shield.SideEffect(() => ourState.PrepareCompleter.TrySetResult(true));
                }
                else if (current.State.Items.Any(s => s.Value == TransactionState.Success) &&
                    current.State[Transport.OwnId] != TransactionState.Success)
                {
                    _transactions.Remove(id);
                    Shield.SideEffect(() =>
                    {
                        Shield.InTransaction(() => Apply(current));
                        ourState.Complete(true);
                    });
                }
            });
        }

        private void Apply(TransactionInfo current)
        {
            _wrapped.ApplyItems(current.Items);
        }

        async Task IBackend.Commit(CommitContinuation cont)
        {
            BackendState ourState = null;
            cont.InContext(() => ourState = _state.Value);
            await Distributed.Run(() =>
            {
                var id = ourState.TransactionId;
                if (!_wrapped.TryGet(id, out TransactionInfo current) || current.State[Transport.OwnId] != TransactionState.None)
                    throw new ApplicationException("Critical error - unexpected commit failure.");
                Apply(current);
                _wrapped.Set(id, current.WithState(current.State.Modify(Transport.OwnId, TransactionState.Success)));
                Shield.SideEffect(() => ourState.Complete(true));
            });
        }

        void IBackend.Rollback() { }

        public void Dispose()
        {
            _wrapped.Dispose();
        }
    }
}
