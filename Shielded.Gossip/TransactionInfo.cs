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
        Done = 2,

        Success = Prepared | Done,
        Fail = Done,
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
            left == TransactionState.Fail || right == TransactionState.Fail ? TransactionState.Fail : (left | right);
    }

    [DataContract(Namespace = ""), Serializable]
    public class TransactionItem : MessageItem
    {
        [DataMember]
        public VectorRelationship Expected { get; set; }
    }

    /// <summary>
    /// A CRDT containing the state and full meta-data on a distributed transaction. Used by the
    /// <see cref="ConsistentGossipBackend"/>.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public class TransactionInfo : IMergeable<TransactionInfo, TransactionInfo>, IDeletable
    {
        /// <summary>
        /// ID of the server that started the transaction.
        /// </summary>
        [DataMember]
        public string Initiator { get; set; }
        [DataMember]
        public TransactionItem[] Items { get; set; }
        [DataMember]
        public TransactionVector State { get; set; }

        /// <summary>
        /// True if all servers have reached the Success state or the Fail state. It is safe to
        /// delete a done transaction, cause even if later revived, it is idempotent - a re-execution
        /// will simply fail with no effects, and the transaction will become deletable again.
        /// </summary>
        public bool CanDelete => State.Items.All(s => (s.Value & TransactionState.Done) != 0);

        public VersionHash GetVersionHash() => (State ?? new TransactionVector()).GetVersionHash();

        public TransactionInfo MergeWith(TransactionInfo other) => WithState(other?.State);

        public TransactionInfo WithState(TransactionVector newState)
        {
            return new TransactionInfo
            {
                Initiator = Initiator,
                Items = Items,
                State = (State ?? new TransactionVector()).MergeWith(newState)
            };
        }

        public TransactionInfo WithState(string ownServerId, TransactionState newState) => WithState((ownServerId, newState));

        public VectorRelationship VectorCompare(TransactionInfo other) => (State ?? new TransactionVector()).VectorCompare(other?.State);
    }
}
