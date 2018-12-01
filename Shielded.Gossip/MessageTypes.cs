using System;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    [DataContract(Namespace = ""), Serializable]
    public class DirectMail
    {
        [DataMember]
        public MessageItem[] Items { get; set; }
    }

    [DataContract(Namespace = ""), Serializable]
    public abstract class GossipMessage
    {
        [DataMember]
        public string From { get; set; }
        [DataMember]
        public ulong DatabaseHash { get; set; }
        [DataMember]
        public DateTimeOffset Time { get; set; } = DateTimeOffset.UtcNow;
    }

    [DataContract(Namespace = ""), Serializable]
    public class NewGossip : GossipMessage
    {
        [DataMember]
        public MessageItem[] Items { get; set; }
        [DataMember]
        public long? WindowStart { get; set; }
        [DataMember]
        public long? WindowEnd { get; set; }
    }

    public interface IGossipReply
    {
        long? LastWindowStart { get; }
        long? LastWindowEnd { get; }
        DateTimeOffset LastTime { get; }
    }

    [DataContract(Namespace = ""), Serializable]
    public class GossipReply : NewGossip, IGossipReply
    {
        [DataMember]
        public long? LastWindowStart { get; set; }
        [DataMember]
        public long? LastWindowEnd { get; set; }
        [DataMember]
        public DateTimeOffset LastTime { get; set; }
    }

    [DataContract(Namespace = ""), Serializable]
    public class GossipEnd : GossipMessage, IGossipReply
    {
        [DataMember]
        public bool Success { get; set; }

        [DataMember]
        public long? LastWindowStart { get; set; }
        [DataMember]
        public long? LastWindowEnd { get; set; }
        [DataMember]
        public DateTimeOffset LastTime { get; set; }
    }

    [DataContract(Namespace = ""), Serializable]
    public class MessageItem
    {
        [DataMember]
        public string Key { get; set; }
        [DataMember]
        public byte[] Data { get; set; }

        [IgnoreDataMember]
        public object Value
        {
            get => Serializer.Deserialize(Data);
            set => Data = Serializer.Serialize(value);
        }

        [IgnoreDataMember]
        public long Freshness
        {
            get => _freshness;
            set => _freshness = value;
        }
        [NonSerialized]
        private long _freshness;

        [IgnoreDataMember]
        public bool Deletable
        {
            get => _deletable;
            set => _deletable = value;
        }
        [NonSerialized]
        private bool _deletable;

        public override string ToString()
        {
            return $"\"{Key}\"{(Deletable ? "*" : "")}{(Freshness != 0 ? " at " + Freshness : "")}: {Value}";
        }
    }
}
