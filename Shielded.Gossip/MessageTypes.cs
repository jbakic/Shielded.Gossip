using System;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    [DataContract(Namespace = "")]
    public class DirectMail
    {
        [DataMember]
        public MessageItem[] Items { get; set; }
    }

    [DataContract(Namespace = "")]
    public class GossipStart
    {
        [DataMember]
        public string From { get; set; }
        [DataMember]
        public ulong DatabaseHash { get; set; }
        [DataMember]
        public DateTimeOffset Time { get; set; } = DateTimeOffset.UtcNow;
    }

    [DataContract(Namespace = "")]
    public class GossipReply
    {
        [DataMember]
        public string From { get; set; }
        [DataMember]
        public ulong DatabaseHash { get; set; }
        [DataMember]
        public MessageItem[] Items { get; set; }
        [DataMember]
        public long WindowStart { get; set; }
        [DataMember]
        public long WindowEnd { get; set; }
        [DataMember]
        public DateTimeOffset Time { get; set; } = DateTimeOffset.UtcNow;

        [DataMember]
        public long? LastWindowStart { get; set; }
        [DataMember]
        public long? LastWindowEnd { get; set; }
        [DataMember]
        public DateTimeOffset LastTime { get; set; }
    }

    [DataContract(Namespace = "")]
    public class GossipEnd
    {
        [DataMember]
        public string From { get; set; }
        [DataMember]
        public bool Success { get; set; }
    }

    [DataContract(Namespace = "")]
    public class MessageItem
    {
        [DataMember]
        public string Key { get; set; }
        [DataMember]
        public byte[] Data { get; set; }

        [IgnoreDataMember]
        public long Freshness { get; set; }

        [IgnoreDataMember]
        public object Deserialized
        {
            get
            {
                return Serializer.Deserialize(Data);
            }
        }

        public override string ToString()
        {
            return string.Format("{0}{1}: {2}", Key, Freshness != 0 ? " at " + Freshness : "", Deserialized);
        }
    }
}
