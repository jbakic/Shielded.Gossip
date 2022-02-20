using Shielded.Gossip.Mergeables;
using Shielded.Gossip.Serializing;
using Shielded.Gossip.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip.Backend
{
    /// <summary>
    /// The state of a transaction on one server.
    /// </summary>
    [Flags]
    public enum TransactionState
    {
        None = 0,
        Prepared = 1,
        Rejected = 2,
        Done = 4,

        Success = Prepared | Done,
        Fail = Rejected | Done,
    }

    /// <summary>
    /// The vector of <see cref="TransactionState"/>, a CRDT representing the state of a distributed
    /// transaction.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class TransactionVector : VectorBase<TransactionVector, TransactionState>, IVersion<TransactionInfo, TransactionVector>
    {
        public TransactionVector() : base() { }
        public TransactionVector(params VectorItem<TransactionState>[] items) : base(items) { }
        public TransactionVector(string ownServerId, TransactionState init) : base(ownServerId, init) { }

        public static implicit operator TransactionVector((string, TransactionState) t) => new TransactionVector(t.Item1, t.Item2);

        protected override VectorRelationship Compare(TransactionState left, TransactionState right) => ((int)left).VectorCompare((int)right);

        protected override TransactionState Merge(TransactionState left, TransactionState right) =>
            right > left ? right : left;

        protected override IEnumerable<byte[]> GetBytes(TransactionState val)
        {
            yield return SafeBitConverter.GetBytes((int)val);
        }

        public VectorRelationship CompareWithValue(TransactionInfo value) => VectorCompare(value?.State);
    }

    /// <summary>
    /// A CRDT containing the state and full meta-data on a distributed transaction. Used by the
    /// <see cref="ConsistentGossipBackend"/>.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class TransactionInfo : IMergeableEx<TransactionInfo>, IDeletable
    {
        /// <summary>
        /// ID of the server that started the transaction.
        /// </summary>
        [DataMember]
        public string Initiator { get; set; }
        [DataMember]
        public bool InitiatorVotes { get; set; }
        [DataMember]
        public ConsistentDependency[] Reads { get; set; }
        [DataMember]
        public MessageItem[] Changes { get; set; }
        [DataMember]
        public TransactionVector State { get; set; }

        /// <summary>
        /// An enumerable of all keys involved in the transaction, reads and writes.
        /// </summary>
        public IEnumerable<string> AllKeys =>
            Reads?.Select(r => r.Key) ?? Enumerable.Empty<string>();

        private IEnumerable<TransactionState> VotingStates =>
            (InitiatorVotes ? State : State.Without(Initiator)).Select(vi => vi.Value);

        public bool IsPrepared => VotingStates.Count(s => (s & TransactionState.Prepared) != 0) > VotingStates.Count() / 2;
        public bool IsRejected
        {
            get
            {
                var voterCount = VotingStates.Count();
                var rejectVotes = VotingStates.Count(s => (s & TransactionState.Rejected) != 0);
                // if the voterCount is even, the threshold for rejection is exactly 1/2, i.e. in case we ever have
                // equal number of Prepared and Rejected votes, we reject.
                var rejectThreshold = voterCount / 2 + voterCount % 2;
                return rejectVotes >= rejectThreshold;
            }
        }

        public bool IsDone => State.Any(i => (i.Value & TransactionState.Done) != 0);
        public bool IsSuccess => State.Any(i => i.Value == TransactionState.Success);
        public bool IsFail => State.Any(i => i.Value == TransactionState.Fail);

        /// <summary>
        /// True as soon as the transaction completes.
        /// </summary>
        public bool CanDelete => IsDone;

        public IEnumerable<byte[]> GetVersionBytes() => (State ?? new TransactionVector()).GetVersionBytes();

        public TransactionInfo MergeWith(TransactionInfo other) => WithState(other?.State);

        public TransactionInfo WithState(TransactionVector newState)
        {
            return new TransactionInfo
            {
                Initiator = Initiator,
                InitiatorVotes = InitiatorVotes,
                Reads = Reads,
                Changes = Changes,
                State = (State ?? new TransactionVector()).MergeWith(newState)
            };
        }

        public TransactionInfo WithState(string ownServerId, TransactionState newState) => WithState((ownServerId, newState));

        public VectorRelationship VectorCompare(TransactionInfo other) => (State ?? new TransactionVector()).VectorCompare(other?.State);

        public IVersion<TransactionInfo> GetVersionOnly() => State ?? new TransactionVector();
    }

    [DataContract(Namespace = ""), Serializable]
    public class ConsistentDependency
    {
        [DataMember]
        public string Key { get; set; }
        [DataMember]
        public byte[] VersionData { get; set; }

        [IgnoreDataMember]
        public object Comparable
        {
            get => VersionData == null ? null : Serializer.Deserialize(VersionData);
            set => VersionData = value == null ? null : Serializer.Serialize(value);
        }

        internal static ConsistentDependency Create(string key, StoredItem item)
        {
            return new ConsistentDependency
            {
                Key = key,
                VersionData = item?.GetOwnDependency().VersionData,
            };
        }
    }
}
