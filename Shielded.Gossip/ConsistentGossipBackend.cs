using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    /// <summary>
    /// A backend based on <see cref="GossipBackend"/>, extending it with support for
    /// consistent transactions. Also works in eventually consistent transactions.
    /// Values should be CRDTs, implementing <see cref="IMergeable{TIn, TOut}"/>,
    /// or should implement <see cref="IHasVectorClock"/> and then they can be wrapped in a
    /// <see cref="Multiple{T}"/> to make them a CRDT. If a type implements <see cref="IDeletable"/>,
    /// it can be deleted from the storage.
    /// </summary>
    /// <remarks>The transactions are very simple - for every Set operation you perform in your
    /// transaction lambda, we will make a transaction item only if it affects the storage.
    /// The result of such a Set operation will be Less or Conflict. We send this result too, as
    /// the expected result. If Conflict is expected, other servers will accept this Set regardless
    /// of their local result. But if Less is expected, then they will check if the result is also
    /// Less on their local DBs, and fail the transaction if not. This is all that a transaction
    /// checks!
    /// 
    /// If eventually consistent transactions are changing the same fields in parallel, then the
    /// consistent transaction, if successful, guarantees only that on every server at some point
    /// of time in their change history this transaction checked out OK. This is important to
    /// note, because eventual changes get written in immediately, maybe before some transaction has
    /// fully completed, and can overwrite her effects before they even become visible.</remarks>
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

        /// <summary>
        /// Tries to read the value under the given key. Does not involve any network communication.
        /// </summary>
        public bool TryGet<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
        {
            key = WrapPublicKey(key);
            return TryGetInternal(key, out item);
        }

        private bool TryGetInternal<TItem>(string key, out TItem item) where TItem : IMergeable<TItem, TItem>
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

        /// <summary>
        /// Sets the given value under the given key, merging it with any already existing value
        /// there. Returns the result of comparison between the old and new value, or
        /// <see cref="VectorRelationship.Less"/> if there is no old value. In a consistent
        /// transaction, only the calls which return Less or Conflict, that actually affect the
        /// storage, get transmitted to other servers. If the result was Less, we will insist
        /// that it's Less on all servers.
        /// </summary>
        public VectorRelationship Set<TItem>(string key, TItem item) where TItem : IMergeable<TItem, TItem>
        {
            key = WrapPublicKey(key);
            if (!Distributed.IsConsistent)
                return _wrapped.Set(key, item);
            return SetInternal(key, item);
        }

        /// <summary>
        /// Small helper - calls <see cref="Set{TItem}(string, TItem)"/>, but wraps the parameter in a
        /// <see cref="Multiple{T}"/> container.
        /// </summary>
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
            public string Initiator;
            public string TransactionId;
            public Dictionary<string, TransactionItem> Changes;
            public TaskCompletionSource<PrepareResult> PrepareCompleter = new TaskCompletionSource<PrepareResult>();
            public TaskCompletionSource<object> Committer = new TaskCompletionSource<object>();

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
            Distributed.EnlistBackend(this);
            if (!_state.HasValue)
            {
                // a hack to keep the transaction open.
                new Shielded<int>().Value = 1;
                _state.Value = new BackendState(WrapInternalKey(TransactionPfx, Guid.NewGuid().ToString()), Transport.OwnId, this);
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

        /// <summary>
        /// Fired during any change, allowing the change to be cancelled.
        /// </summary>
        public event EventHandler<ChangingEventArgs> Changing;

        private readonly ShieldedDictNc<string, BackendState> _transactions = new ShieldedDictNc<string, BackendState>();
        private readonly ShieldedDictNc<string, BackendState> _fieldBlockers = new ShieldedDictNc<string, BackendState>();

        async Task<PrepareResult> IBackend.Prepare(CommitContinuation cont)
        {
            BackendState ourState = null;
            cont.InContext(() =>
            {
                ourState = _state.Value;
                Shield.SideEffect(null, () => ourState.Complete(false));
            });
            if (ourState.Changes == null || !ourState.Changes.Any())
                return new PrepareResult(true);

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
            async Task<PrepareResult> PreparationProcess()
            {
                var prepare = await Task.WhenAny(PrepareInternal(id, ourState, transaction), ourState.PrepareCompleter.Task);
                if (!prepare.Result.Success)
                    return prepare.Result;
                await Distributed.Run(() =>
                {
                    if (_transactions.ContainsKey(id))
                        _wrapped.Set(id, transaction);
                });
                return await ourState.PrepareCompleter.Task;
            }
            var prepTask = PreparationProcess();
            var resTask = await Task.WhenAny(prepTask, Task.Delay(GetNewTimeout()));
            if (resTask != prepTask || !prepTask.Result.Success)
            {
                ourState.Complete(false);
                await SetFail(ourState.TransactionId);
                if (resTask != prepTask)
                    return new PrepareResult(false, null);
            }
            return prepTask.Result;
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
            Distributed.RunLocal(() =>
            {
                foreach (var key in ourState.Changes.Keys)
                    if (_fieldBlockers.TryGetValue(key, out BackendState state) && state == ourState)
                        _fieldBlockers.Remove(key);
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
                if (OnChanging(e.Key.Substring(PublicPfx.Length), e.OldValue, e.NewValue))
                    e.Cancel = true;
            }
        }

        private void OnTransactionChanging(string id, TransactionInfo oldVal, TransactionInfo newVal)
        {
            if (oldVal == null && newVal != null)
            {
                if (newVal.State.Items.Any(s => (s.Value & TransactionState.Done) != 0) ||
                    _transactions.ContainsKey(id))
                {
                    OnStateChange(id, newVal);
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
                            await SetFail(id);
                            ourState.Complete(false);
                            return;
                        }
                        if (!await SetPrepared(id))
                        {
                            ourState.Complete(false);
                            return;
                        }
                        ourState.PrepareCompleter.TrySetResult(new PrepareResult(true));
                    }
                    catch
                    {
                        await SetFail(id);
                        ourState.Complete(false);
                    }
                });
            }
            else if (newVal != null)
            {
                OnStateChange(id, newVal);
            }
        }

        private async Task<bool> SetPrepared(string id)
        {
            return await Distributed.Run(() =>
            {
                if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                    current.State[Transport.OwnId] != TransactionState.None ||
                    current.State.Items.Any(i => i.Value == TransactionState.Fail))
                    return false;
                _wrapped.DirectMailRestriction.Value = current.Initiator;
                _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Prepared));
                return true;
            });
        }

        private async Task SetFail(string id)
        {
            await Distributed.Run(() =>
            {
                if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                    (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                    return;
                _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Fail));
            });
        }

        private bool ApplyAndSetSuccess(string id)
        {
            bool res = false;
            Distributed.RunLocal(() =>
            {
                if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                    (current.State[Transport.OwnId] & TransactionState.Done) != 0)
                {
                    res = false;
                    return;
                }
                Apply(current);
                _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Success));
                res = true;
            });
            return res;
        }

        private void OnStateChange(string id, TransactionInfo current)
        {
            _transactions.TryGetValue(id, out var ourState);
            if (current.State.Items.Any(s => s.Value == TransactionState.Fail) &&
                (current.State[Transport.OwnId] & TransactionState.Done) == 0)
            {
                Shield.SideEffect(async () =>
                {
                    await SetFail(id);
                    if (ourState != null)
                        ourState.Complete(false);
                });
            }
            else if (current.State.Without(Transport.OwnId).All(s => s.Value == TransactionState.Prepared) &&
                current.State[Transport.OwnId] == TransactionState.None)
            {
                if (ourState != null)
                    Shield.SideEffect(() =>
                        ourState.PrepareCompleter.TrySetResult(new PrepareResult(true)));
            }
            else if (current.State.Items.All(s => (s.Value & TransactionState.Prepared) != 0) &&
                (current.State[Transport.OwnId] & TransactionState.Done) == 0)
            {
                Shield.SideEffect(() =>
                {
                    ApplyAndSetSuccess(id);
                    if (ourState != null)
                        ourState.Complete(true);
                });
            }
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
                if (!_wrapped.TryGet(id, out TransactionInfo current) ||
                    current.State[Transport.OwnId] != TransactionState.None)
                    throw new ApplicationException("Critical error - unexpected commit failure.");
                Apply(current);
                _wrapped.Set(id, current.WithState(Transport.OwnId, TransactionState.Success));
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
