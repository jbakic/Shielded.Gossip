using System;
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
    public class KillGossip
    {
        [DataMember]
        public string From { get; set; }
        [DataMember]
        public int ReplyToId { get; set; }
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

        [DataMember]
        public bool Deleted { get; set; }

        [DataMember]
        public int? ExpiresInMs
        {
            get
            {
                if (!_expiresInMs.HasValue)
                    return null;
                if (!_referenceTickCount.HasValue)
                    return _expiresInMs;
                return unchecked(_expiresInMs.Value + _referenceTickCount.Value - GetEnvironmentTickCount());
            }
            set
            {
                _referenceTickCount = null;
                _expiresInMs = value;
            }
        }
        private int? _expiresInMs;
        [NonSerialized]
        private int? _referenceTickCount;

        private static ShieldedLocal<int> _transactionTickCount = new ShieldedLocal<int>();

        /// <summary>
        /// Like Environment.TickCount, but does not change within one Shielded transaction run.
        /// </summary>
        private int GetEnvironmentTickCount()
        {
            if (!Shield.IsInTransaction)
                return Environment.TickCount;
            if (!_transactionTickCount.HasValue)
                return _transactionTickCount.Value = Environment.TickCount;
            return _transactionTickCount.Value;
        }

        public void ActivateExpiry(int referenceTickCount)
        {
            if (_expiresInMs.HasValue)
                _referenceTickCount = referenceTickCount;
        }

        public override string ToString()
        {
            return $"\"{Key}\"{(Deleted ? "*" : "")}{(Freshness != 0 ? " at " + Freshness : "")}" +
                $"{(ExpiresInMs.HasValue ? " exp " + ExpiresInMs : "")}: {Value}";
        }
    }
}
