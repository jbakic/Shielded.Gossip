using System;

namespace Shielded.Gossip
{
    public class ChangedEventArgs : EventArgs
    {
        public readonly string Key;
        public readonly object NewValue;

        public ChangedEventArgs(string key, object newVal)
        {
            Key = key;
            NewValue = newVal;
        }
    }
}
