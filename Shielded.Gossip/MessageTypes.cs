using System;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    [DataContract(Namespace = "")]
    public class DirectMail
    {
        [DataMember]
        public Item[] Items;
    }

    [DataContract(Namespace = "")]
    public class GossipStart
    {
        [DataMember]
        public string From;
        [DataMember]
        public ulong DatabaseHash;
        [DataMember]
        public DateTimeOffset Time = DateTimeOffset.UtcNow;
    }

    [DataContract(Namespace = "")]
    public class GossipReply
    {
        [DataMember]
        public string From;
        [DataMember]
        public ulong DatabaseHash;
        [DataMember]
        public Item[] Items;
        [DataMember]
        public long WindowStart;
        [DataMember]
        public long WindowEnd;
        [DataMember]
        public DateTimeOffset Time = DateTimeOffset.UtcNow;

        [DataMember]
        public long? LastWindowStart;
        [DataMember]
        public long? LastWindowEnd;
        [DataMember]
        public DateTimeOffset LastTime;
    }

    [DataContract(Namespace = "")]
    public class GossipEnd
    {
        [DataMember]
        public string From;
        [DataMember]
        public bool Success;
    }

    [DataContract(Namespace = "")]
    public class Item
    {
        [DataMember]
        public string Key;
        [DataMember]
        public byte[] Data;

        [IgnoreDataMember]
        public long Freshness;

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
