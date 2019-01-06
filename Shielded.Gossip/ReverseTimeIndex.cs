using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Shielded.Gossip
{
    internal class TimeIndexAppendItem
    {
        public string Key;
        public long? OldFreshness;
        public MessageItem NewItem;

        public TimeIndexAppendItem(string key, long? oldFreshness, MessageItem newItem)
        {
            Key = key;
            OldFreshness = oldFreshness;
            NewItem = newItem;
        }
    }

    internal class ReverseTimeIndex
    {
        private class AppendBlock
        {
            public TimeIndexAppendItem[] Items;
            public long LastFreshness;
            public AppendBlock Previous;
        }

        private readonly ShieldedTreeNc<long, string> _freshIndex = new ShieldedTreeNc<long, string>();
        private readonly Shielded<long> _lastAppliedFreshness = new Shielded<long>();
        private readonly Shielded<AppendBlock> _appendQueue = new Shielded<AppendBlock>();

        private HashSet<string> GetKeysToBeAppended()
        {
            var result = new HashSet<string>();
            var current = _appendQueue.Value;
            while (current != null)
            {
                foreach (var item in current.Items)
                    result.Add(item.Key);
                current = current.Previous;
            }
            return result;
        }

        /// <summary>
        /// This one will never include items still in the append queue, which is safe since it is
        /// only used from the deletable items timer thread, which only looks at older changes.
        /// </summary>
        public IEnumerable<KeyValuePair<long, string>> Range(long from, long to)
        {
            var skip = GetKeysToBeAppended();
            return _freshIndex.Range(from, to)
                .Where(kvp => !skip.Contains(kvp.Value));
        }

        private IEnumerable<KeyValuePair<long, string>> GetAppendQueueEffectDescending()
        {
            var alreadyCovered = new HashSet<string>();
            var current = _appendQueue.Value;
            while (current != null)
            {
                for (var i = current.Items.Length - 1; i >= 0; i--)
                {
                    var item = current.Items[i];
                    if (!alreadyCovered.Add(item.Key))
                        continue;
                    if (item.NewItem != null)
                        yield return new KeyValuePair<long, string>(item.NewItem.Freshness, item.Key);
                }
                current = current.Previous;
            }
        }

        /// <summary>
        /// This one includes the items from the queue as well, to make sure gossip replies always
        /// see everything.
        /// </summary>
        public IEnumerable<KeyValuePair<long, string>> RangeDescending(long from, long to)
        {
            var fromQueue = GetAppendQueueEffectDescending()
                .SkipWhile(kvp => kvp.Key > from)
                .TakeWhile(kvp => kvp.Key >= to);
            if (to > _lastAppliedFreshness)
                return fromQueue;

            var skip = GetKeysToBeAppended();
            var fromTree = _freshIndex.RangeDescending(from, to).Where(kvp => !skip.Contains(kvp.Value));
            if (_lastAppliedFreshness.Value >= from)
                return fromTree;
            return fromQueue.Concat(fromTree);
        }

        public void Append(TimeIndexAppendItem[] items)
        {
            Array.Sort(items, (a, b) => (a.NewItem?.FreshnessOffset ?? 0).CompareTo(b.NewItem?.FreshnessOffset ?? 0));
            var lastFreshness = _lastAppliedFreshness.Value;
            _appendQueue.Commute((ref AppendBlock cell) =>
            {
                // we must assign Freshness to items within this transaction, so that the correct value is
                // visible as soon as we commit. it may not be changed later, because that would be unsafe.
                // to maintain this commute, we must read the last appended Freshness from cell.
                lastFreshness = cell?.LastFreshness ?? lastFreshness;
                var newFresh = lastFreshness + 1;
                var maxFresh = lastFreshness;
                foreach (var item in items)
                    if (item.NewItem != null)
                    {
                        item.NewItem.Freshness = newFresh + item.NewItem.FreshnessOffset;
                        if (maxFresh < item.NewItem.Freshness)
                            maxFresh = item.NewItem.Freshness;
                    }
                cell = new AppendBlock
                {
                    Items = items,
                    LastFreshness = maxFresh,
                    Previous = cell
                };
            });
            Shield.SideEffect(ProcessQueue);
        }

        private IEnumerable<AppendBlock> GetAppendEnumerable()
        {
            var current = _appendQueue.Value;
            while (current != null)
            {
                yield return current;
                current = current.Previous;
            }
        }

        private void ProcessQueue() => Shield.InTransaction(() =>
        {
            var maxFresh = _lastAppliedFreshness.Value;
            var toAdd = new Dictionary<string, long?>();
            var toRemove = new Dictionary<string, long?>();
            foreach (var block in GetAppendEnumerable())
            {
                if (maxFresh < block.LastFreshness)
                    maxFresh = block.LastFreshness;
                foreach (var item in block.Items)
                {
                    // we will be removing a key based on its smallest OldFreshness we encounter. this assumes
                    // we are moving through the queue in descending freshness order. and although we are going
                    // in ascending order through the Items, one Items never contain the same key twice.
                    toRemove[item.Key] = item.OldFreshness;
                    // adding is the other way around - we take only the biggest, the first one we see, even if it is empty.
                    if (!toAdd.ContainsKey(item.Key))
                        toAdd[item.Key] = item.NewItem?.Freshness;
                }
            }
            foreach (var kvp in toRemove)
                if (kvp.Value != null)
                    _freshIndex.Remove(kvp.Value.Value, kvp.Key);
            foreach (var kvp in toAdd)
                if (kvp.Value != null)
                    _freshIndex.Add(kvp.Value.Value, kvp.Key);
            _appendQueue.Value = null;
            _lastAppliedFreshness.Value = maxFresh;
        });

        public long LastFreshness => _appendQueue.Value?.LastFreshness ?? _lastAppliedFreshness;
    }
}
