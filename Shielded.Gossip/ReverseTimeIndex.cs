using Shielded.Standard;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.Gossip
{
    internal struct ReverseTimeIndexItem
    {
        public readonly long Freshness;
        public readonly MessageItem Item;

        public ReverseTimeIndexItem(long freshness, MessageItem item)
        {
            Freshness = freshness;
            Item = item;
        }
    }

    internal class ReverseTimeIndex : IEnumerable<ReverseTimeIndexItem>
    {
        private class ListElement
        {
            public MessageItem Item;
            public long Freshness;
            public ListElement Previous;
        }

        /// <summary>
        /// Enumerator, implemented as a struct to allow cheap cloning.
        /// </summary>
        public struct Enumerator : IEnumerator<ReverseTimeIndexItem>
        {
            private readonly Func<string, MessageItem> _currentItemGetter;
            private ListElement _current;
            private bool _open;

            public Enumerator(ReverseTimeIndex owner)
            {
                _currentItemGetter = owner._currentItemGetter;
                _current = owner._listHead.Value;
                _open = false;
            }

            public bool IsDefault => _currentItemGetter == null;

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
        private readonly Func<string, MessageItem> _currentItemGetter;

        public ReverseTimeIndex(Func<string, MessageItem> currentItemGetter)
        {
            _currentItemGetter = currentItemGetter ?? throw new ArgumentNullException();
        }

        private readonly ShieldedLocal<Dictionary<string, MessageItem>> _toAppend = new ShieldedLocal<Dictionary<string, MessageItem>>();

        public void Append(MessageItem item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            var toAppend = _toAppend.GetValueOrDefault();
            if (toAppend == null)
            {
                _toAppend.Value = toAppend = new Dictionary<string, MessageItem>();
                _listHead.Commute(AppendCommute);
            }
            toAppend[item.Key] = item;
        }

        private void AppendCommute(ref ListElement cell)
        {
            var newFresh = (cell?.Freshness ?? 0) + 1;
            foreach (var kvp in _toAppend.Value.OrderBy(kvp => kvp.Value.FreshnessOffset))
            {
                var item = kvp.Value;
                item.Freshness = newFresh + item.FreshnessOffset;
                cell = new ListElement
                {
                    Item = item,
                    Freshness = item.Freshness,
                    Previous = cell,
                };
            }
        }

        public Enumerator GetCloneableEnumerator() => new Enumerator(this);

        public IEnumerator<ReverseTimeIndexItem> GetEnumerator() => GetCloneableEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetCloneableEnumerator();

        public long LastFreshness => _listHead.Value?.Freshness ?? 0;
    }
}
