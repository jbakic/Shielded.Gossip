using System;

namespace Shielded.Gossip.Backend
{
    public class ChangedEventArgs : EventArgs
    {
        public readonly string Key;
        public readonly object OldValue;
        public readonly object NewValue;
        public readonly bool Deleted;

        public ChangedEventArgs(string key, object oldVal, object newVal, bool deleted)
        {
            Key = key;
            OldValue = oldVal;
            NewValue = newVal;
            Deleted = deleted;
        }
    }
}
