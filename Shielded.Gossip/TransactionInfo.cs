using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
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
    public class TransactionVector : VectorBase<TransactionVector, TransactionState>
    {
        public TransactionVector() : base() { }
        public TransactionVector(params VectorItem<TransactionState>[] items) : base(items) { }
        public TransactionVector(string ownServerId, TransactionState init) : base(ownServerId, init) { }

        public static implicit operator TransactionVector((string, TransactionState) t) => new TransactionVector(t.Item1, t.Item2);

        protected override TransactionState Merge(TransactionState left, TransactionState right) =>
            right > left ? right : left;
    }

    /// <summary>
    /// A CRDT containing the state and full meta-data on a distributed transaction. Used by the
    /// <see cref="ConsistentGossipBackend"/>.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class TransactionInfo : IMergeable<TransactionInfo>, IDeletable
    {
        /// <summary>
        /// ID of the server that started the transaction.
        /// </summary>
        [DataMember]
        public string Initiator { get; set; }
        [DataMember]
        public bool InitiatorVotes { get; set; }
        [DataMember]
        public MessageItem[] Reads { get; set; }
        [DataMember]
        public MessageItem[] Changes { get; set; }
        [DataMember]
        public TransactionVector State { get; set; }

        /// <summary>
        /// An enumerable of all keys involved in the transaction, reads and writes.
        /// </summary>
        public IEnumerable<string> AllKeys =>
            (Reads?.Select(r => r.Key) ?? Enumerable.Empty<string>())
            .Concat(Changes?.Select(w => w.Key) ?? Enumerable.Empty<string>());

        private IEnumerable<TransactionState> VotingStates =>
            (InitiatorVotes ? State : State.Without(Initiator)).Select(vi => vi.Value);

        public bool IsPrepared => VotingStates.Count(s => (s & TransactionState.Prepared) != 0) > (VotingStates.Count() / 2);
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

        public bool IsDone => State.Items.Any(i => (i.Value & TransactionState.Done) != 0);
        public bool IsSuccess => State.Items.Any(i => i.Value == TransactionState.Success);
        public bool IsFail => State.Items.Any(i => i.Value == TransactionState.Fail);

        /// <summary>
        /// True if a majority of servers has reached the Success or the Fail state. It is safe to
        /// delete a done transaction, cause even if later revived, it is idempotent - a re-execution
        /// will simply fail with no effects, and the transaction will become deletable again.
        /// </summary>
        public bool CanDelete => State.Items.Count(s => (s.Value & TransactionState.Done) != 0) > (State.Items.Length / 2);

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
    }
}
