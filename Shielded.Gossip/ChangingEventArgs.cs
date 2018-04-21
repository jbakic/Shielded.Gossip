using System.ComponentModel;

namespace Shielded.Gossip
{
    public class ChangingEventArgs : CancelEventArgs
    {
        public readonly string Key;
        public readonly object OldValue;
        public readonly object NewValue;

        public ChangingEventArgs(string key, object oldVal, object newVal)
        {
            Key = key;
            OldValue = oldVal;
            NewValue = newVal;
        }
    }
}
