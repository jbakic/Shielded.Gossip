using System;

namespace Shielded.Gossip
{
    [Serializable]
    public class MessageItem
    {
        public string Key { get; set; }
        public string TypeName { get; set; }
        public byte[] Data { get; set; }
    }
}
