using System;

namespace Shielded.Gossip
{
    public class ChangedEventArgs : EventArgs
    {
        public readonly string Key;
        public readonly object OldValue;
        public readonly object NewValue;

        public ChangedEventArgs(string key, object oldVal, object newVal)
        {
            Key = key;
            OldValue = oldVal;
            NewValue = newVal;
        }
    }
}
