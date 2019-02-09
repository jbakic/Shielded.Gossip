﻿using System;
using System.Runtime.Serialization;
using System.Threading;

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
        public VersionHash DatabaseHash { get; set; }

        private static int _msgIdGenerator = new Random().Next();

        [DataMember]
        public int MessageId { get; set; } = Interlocked.Increment(ref _msgIdGenerator);

        /// <summary>
        /// Will always be set on reply and end messages.
        /// </summary>
        [DataMember]
        public int? ReplyToId { get; set; }
    }

    [DataContract(Namespace = ""), Serializable]
    public abstract class NewGossip : GossipMessage
    {
        [DataMember]
        public MessageItem[] Items { get; set; }
        [DataMember]
        public long WindowStart { get; set; }
        [DataMember]
        public long WindowEnd { get; set; }
    }

    [DataContract(Namespace = ""), Serializable]
    public class GossipStart : NewGossip { }

    [DataContract(Namespace = ""), Serializable]
    public class GossipReply : NewGossip
    {
        [DataMember]
        public long LastWindowStart { get; set; }
        [DataMember]
        public long LastWindowEnd { get; set; }
    }

    [DataContract(Namespace = ""), Serializable]
    public class GossipEnd : GossipMessage
    {
        [DataMember]
        public bool Success { get; set; }
        [DataMember]
        public long WindowEnd { get; set; }

        [DataMember]
        public long LastWindowEnd { get; set; }
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
            get => Data == null ? null : Serializer.Deserialize(Data);
            set => Data = value == null ? null : Serializer.Serialize(value);
        }

        [DataMember]
        public long Freshness { get; set; }

        [IgnoreDataMember]
        public long FreshnessOffset
        {
            get => _freshnessOffset;
            set => _freshnessOffset = value;
        }
        [NonSerialized]
        private long _freshnessOffset;

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
