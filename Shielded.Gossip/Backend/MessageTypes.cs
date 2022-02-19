using Shielded.Gossip.Mergeables;
using Shielded.Gossip.Serializing;
using System;
using System.Runtime.Serialization;
using System.Threading;

namespace Shielded.Gossip.Backend
{
    [DataContract(Namespace = ""), Serializable]
    public class CausalTransaction
    {
        [DataMember]
        public Dependency[] Dependencies { get; set; }

        [DataMember]
        public MessageItem[] Changes { get; set; }
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
        /// <summary>
        /// Transactions are ordered as they occurred on the sending server, chronologically.
        /// </summary>
        [DataMember]
        public CausalTransaction[] Transactions { get; set; }
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
        public bool Deleted { get; set; }

        [DataMember]
        public bool Expired { get; set; }

        [DataMember]
        public int? ExpiresInMs { get; set; }

        public override string ToString()
        {
            return $"\"{Key}\"{(Deleted ? "*" : "")}{(ExpiresInMs.HasValue ? " exp " + ExpiresInMs : "")}: {Value}";
        }
    }

    /// <summary>
    /// Contains a key and the version it had when a transaction accessed it.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class Dependency
    {
        [DataMember]
        public string Key { get; set; }
        [DataMember]
        public byte[] VersionData { get; set; }
        [DataMember]
        public bool IsInFuture { get; set; }

        /// <summary>
        /// Will be an <see cref="IMergeableEx{T}"/>, but one obtained by calling <see cref="IMergeableEx{T}.GetVersionOnly()"/>.
        /// </summary>
        [IgnoreDataMember]
        public object Comparable
        {
            get => VersionData == null ? null : Serializer.Deserialize(VersionData);
            set => VersionData = value == null ? null : Serializer.Serialize(value);
        }

        public override string ToString()
        {
            return $"\"{Key}\": {Comparable}";
        }
    }
}
