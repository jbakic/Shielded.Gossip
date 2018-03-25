using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    [Serializable]
    [KnownType(typeof(TransactionMessage))]
    public abstract class Message
    {
        public string From { get; set; }
        public string To { get; set; }
        public byte[] Data { get; set; }
    }

    [Serializable]
    public class TransactionMessage : Message
    {
        [IgnoreDataMember]
        public MessageItem[] Items
        {
            get => Serializer.Deserialize<MessageItem[]>(Data);
            set => Data = Serializer.Serialize(value);
        }
    }
}
