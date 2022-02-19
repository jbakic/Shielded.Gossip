using Shielded.Gossip.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Shielded.Gossip.Backend
{
    internal struct ReverseTimeIndexItem
    {
        public readonly long Freshness;
        public readonly StoredItem Item;

        public ReverseTimeIndexItem(long freshness, StoredItem item)
        {
            Freshness = freshness;
            Item = item;
        }
    }

    internal class ReverseTimeIndex : IEnumerable<ReverseTimeIndexItem>
    {
        private class ListElement
        {
            public StoredItem Item;
            public long Freshness;
            public ListElement Previous;
        }

        /// <summary>
        /// Enumerator, implemented as a struct to allow cheap cloning.
        /// </summary>
        public struct Enumerator : IEnumerator<ReverseTimeIndexItem>
        {
            private readonly Func<string, StoredItem> _currentItemGetter;
            private ListElement _current;
            private bool _open;

            public Enumerator(ReverseTimeIndex owner)
            {
                _currentItemGetter = owner._currentItemGetter;
                _current = owner._listHead.Value;
                _open = false;
            }

            public bool IsOpen => _open;

            public bool IsDone => _current == null;

            public ReverseTimeIndexItem Current =>
                !_open ? throw new InvalidOperationException("MoveNext not called yet.") :
                _current == null ? throw new InvalidOperationException("Enumeration already completed.") :
                new ReverseTimeIndexItem(_current.Freshness, _current.Item);

            object IEnumerator.Current => ((IEnumerator<ReverseTimeIndexItem>)this).Current;

            public void Dispose() { }

            public bool MoveNext()
            {
                if (!_open)
                {
                    _open = true;
                    if (_current == null)
                        return false;
                    if (_currentItemGetter(_current.Item.Key) != _current.Item)
                        MoveToNextValid();
                    return _current != null;
                }

                if (_current == null)
                    throw new InvalidOperationException("Enumeration already completed.");
                MoveToNextValid();
                return _current != null;
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            private void MoveToNextValid()
            {
                var prev = _current.Previous;
                while (prev != null && _currentItemGetter(prev.Item.Key) != prev.Item)
                {
                    prev = prev.Previous;
                }
                // iterators help clean up the list. this is dangerous, but we never iterate in transactions
                // that also change the items, so the _currentItemGetter will only return committed values,
                // so this is safe to do. otherwise, the _currentItemGetter should use Shield.ReadOldState.
                _current = _current.Previous = prev;
            }
        }


        private readonly Shielded<ListElement> _listHead = new Shielded<ListElement>();
        private readonly Shielded<VersionHash> _databaseHash = new Shielded<VersionHash>();
        private readonly Func<string, StoredItem> _currentItemGetter;

        public ReverseTimeIndex(Func<string, StoredItem> currentItemGetter)
        {
            _currentItemGetter = currentItemGetter ?? throw new ArgumentNullException();
        }

        private readonly ShieldedLocal<List<GossipBackend.TransactionInfo>> _registeredTransactions = new();

        public void RegisterTransaction(GossipBackend.TransactionInfo trans)
        {
            var regs = _registeredTransactions.GetValueOrDefault();
            if (regs == null)
            {
                _registeredTransactions.Value = regs = new();
                _listHead.Commute((ref ListElement el) => AppendChanges(ref el, regs));
                _databaseHash.Commute((ref VersionHash h) => ApplyHashFx(ref h, regs));
            }
            regs.Add(trans);
        }

        private void AppendChanges(ref ListElement curr, List<GossipBackend.TransactionInfo> transactions)
        {
            var lastFreshness = curr?.Freshness ?? 0;
            var baseFreshness = lastFreshness + 1;
            var referenceTickCount = TransactionalTickCount.Value;
            foreach (var trans in transactions)
            {
                var deps = trans.Dependencies.Values.ToArray();
                var freshness = baseFreshness + trans.FreshnessOffset;
                if (freshness <= lastFreshness)
                    throw new InvalidOperationException("FreshnessOffset may only increase between sub-transactions.");
                lastFreshness = freshness;
                foreach (var item in trans.Changes.Values)
                {
                    item.OpenTransaction = null;
                    item.Freshness = freshness;
                    item.Dependencies = deps;
                    item.ActivateExpiry(referenceTickCount);
                    if (item.Deleted || item.Expired)
                        item.RemovableSince = referenceTickCount;
                    curr = new ListElement
                    {
                        Item = item,
                        Freshness = freshness,
                        Previous = curr,
                    };
                }
            }
        }

        private void ApplyHashFx(ref VersionHash h, List<GossipBackend.TransactionInfo> transactions)
        {
            foreach (var trans in transactions)
                h ^= trans.HashEffect;
        }

        public Enumerator GetCloneableEnumerator() => new Enumerator(this);

        public IEnumerator<ReverseTimeIndexItem> GetEnumerator() => GetCloneableEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetCloneableEnumerator();

        public long LastFreshness => _listHead.Value?.Freshness ?? 0;

        public VersionHash DatabaseHash => _databaseHash.Value;
    }
}
