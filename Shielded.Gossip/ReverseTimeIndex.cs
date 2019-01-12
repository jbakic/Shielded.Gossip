using Shielded.Standard;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.Gossip
{
    //internal interface IReverseTimeIndexIterator
    //{
    //    long Freshness { get; }
    //    MessageItem Item { get; }
    //    bool Finished { get; }
    //    void MovePrevious();
    //    IReverseTimeIndexIterator Clone();
    //}

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

        //private class ListIterator : IReverseTimeIndexIterator
        //{
        //    private ListElement _current;
        //    private readonly Func<string, MessageItem> _currentItemGetter;

        //    public ListIterator(ListElement current, Func<string, MessageItem> currentItemGetter)
        //    {
        //        _current = current;
        //        _currentItemGetter = currentItemGetter;

        //        if (_current != null && _currentItemGetter(_current.Item.Key) == null)
        //            MovePrevious();
        //    }

        //    public long Freshness => _current.Freshness;
        //    public MessageItem Item => _current.Item;
        //    public bool Finished => _current == null;

        //    public void MovePrevious()
        //    {
        //        while (_current.Previous != null &&
        //            _currentItemGetter(_current.Previous.Item.Key) != _current.Previous.Item)
        //        {
        //            // iterators help clean up the list. this is dangerous, but we never iterate in transactions
        //            // that also change the items, so the _currentItemGetter will only return committed values,
        //            // so this is safe to do. otherwise, the _currentItemGetter should use Shield.ReadOldState.
        //            _current.Previous = _current.Previous.Previous;
        //        }
        //        _current = _current.Previous;
        //    }

        //    public IReverseTimeIndexIterator Clone() => new ListIterator(_current, _currentItemGetter);
        //}

        private readonly Shielded<ListElement> _listHead = new Shielded<ListElement>();
        private readonly Func<string, MessageItem> _currentItemGetter;

        public ReverseTimeIndex(Func<string, MessageItem> currentItemGetter)
        {
            _currentItemGetter = currentItemGetter;
        }

        //public IReverseTimeIndexIterator GetIterator() => new ListIterator(_listHead.Value, _currentItemGetter);

        //public IEnumerable<(long Freshness, MessageItem Item)> Enumerable
        //{
        //    get
        //    {
        //        var iter = GetIterator();
        //        while (!iter.Finished)
        //            yield return (iter.Freshness, iter.Item);
        //    }
        //}

        public void Append(MessageItem[] items)
        {
            if (!items.Any())
                return;
            Array.Sort(items, (a, b) => a.FreshnessOffset.CompareTo(b.FreshnessOffset));
            ListElement last = null, first = null;
            foreach (var item in items)
            {
                last = new ListElement
                {
                    Item = item,
                    Previous = last,
                };
                if (first == null)
                    first = last;
            }
            _listHead.Commute((ref ListElement cell) =>
            {
                // we must assign Freshness to items within this transaction, so that the correct value is
                // visible as soon as we commit. it may not be changed later, because that would be unsafe.
                // to maintain this commute, we must read the last appended Freshness from cell.
                var newFresh = (cell?.Freshness ?? 0) + 1;
                var curr = last;
                while (true)
                {
                    curr.Freshness = curr.Item.Freshness = newFresh + curr.Item.FreshnessOffset;
                    // since we change first.Previous below, we cannot just continue down Previouses until null...
                    if (curr == first)
                        break;
                    curr = curr.Previous;
                }
                first.Previous = cell;
                cell = last;
            });
        }

        public IEnumerator<ReverseTimeIndexItem> GetEnumerator()
        {
            Shield.AssertInTransaction();
            var current = _listHead.Value;
            if (current == null)
                yield break;
            if (_currentItemGetter(current.Item.Key) == current.Item)
                yield return new ReverseTimeIndexItem(current.Freshness, current.Item);

            while (true)
            {
                var prev = current.Previous;
                while (prev != null && _currentItemGetter(prev.Item.Key) != prev.Item)
                {
                    prev = prev.Previous;
                }
                // iterators help clean up the list. this is dangerous, but we never iterate in transactions
                // that also change the items, so the _currentItemGetter will only return committed values,
                // so this is safe to do. otherwise, the _currentItemGetter should use Shield.ReadOldState.
                current = current.Previous = prev;
                if (current == null)
                    yield break;
                yield return new ReverseTimeIndexItem(current.Freshness, current.Item);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<ReverseTimeIndexItem>)this).GetEnumerator();
        }

        public long LastFreshness => _listHead.Value?.Freshness ?? 0;
    }
}
