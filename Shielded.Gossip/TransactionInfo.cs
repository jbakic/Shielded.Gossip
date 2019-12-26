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
        None,
        Promised,
        Accepted,
        Rejected,
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

        protected override VectorRelationship Compare(TransactionState left, TransactionState right) => ((int)left).VectorCompare((int)right);

        protected override TransactionState Merge(TransactionState left, TransactionState right) =>
            right > left ? right : left;

        protected override IEnumerable<byte[]> GetBytes(TransactionState val)
        {
            yield return SafeBitConverter.GetBytes((int)val);
        }
    }

    /// <summary>
    /// A CRDT containing the state and full meta-data on a distributed transaction. Used by the
    /// <see cref="ConsistentGossipBackend"/>.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class TransactionInfo : IMergeable<TransactionInfo>, IDeletable
    {
        [DataMember]
        public string Initiator { get; set; }
        [DataMember]
        public long BallotNumber { get; set; }
        [DataMember]
        public MessageItem[] Reads { get; set; }
        [DataMember]
        public MessageItem[] Changes { get; set; }
        [DataMember]
        public TransactionVector State { get; set; }

        /// <summary>
        /// An enumerable of all keys involved in the transaction, reads and writes.
        /// </summary>
        public IEnumerable<string> AllKeys => Reads?.Select(r => r.Key) ?? Enumerable.Empty<string>();

        public bool IsPromised => State.Count(s => s.Value == TransactionState.Promised || s.Value == TransactionState.Accepted) > (State.Count / 2);
        public bool IsRejected
        {
            get
            {
                var voterCount = State.Count;
                var rejectVotes = State.Count(s => s.Value == TransactionState.Rejected);
                // if the voterCount is even, the threshold for rejection is exactly 1/2, i.e. in case we ever have
                // equal number of Prepared and Rejected votes, we reject.
                var rejectThreshold = voterCount / 2 + voterCount % 2;
                return rejectVotes >= rejectThreshold;
            }
        }
        public bool HasAcceptedVotes => State.Any(s => s.Value == TransactionState.Accepted);
        public bool IsSuccessful => State.Count(s => s.Value == TransactionState.Accepted) > (State.Count / 2);
        public bool IsDone => IsRejected || IsSuccessful;

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
                BallotNumber = BallotNumber,
                Reads = Reads,
                Changes = Changes,
                State = (State ?? new TransactionVector()).MergeWith(newState)
            };
        }

        public TransactionInfo WithState(string ownServerId, TransactionState newState) => WithState((ownServerId, newState));

        public VectorRelationship VectorCompare(TransactionInfo other) => (State ?? new TransactionVector()).VectorCompare(other?.State);

        public int ComparePriority(TransactionInfo other)
        {
            var ballotComp = BallotComparer.Compare(BallotNumber, other.BallotNumber);
            if (ballotComp != 0)
                return ballotComp;
            return StringComparer.InvariantCultureIgnoreCase.Compare(Initiator, other.Initiator);
        }
    }

    public static class BallotComparer
    {
        public static int Compare(long a, long b)
        {
            var ballotDiff = unchecked(a - b);
            return
                // reverse order - higher numbers go first
                ballotDiff > 0 ? -1 :
                ballotDiff < 0 ? 1 :
                0;
        }
    }
}
