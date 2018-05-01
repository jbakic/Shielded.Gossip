using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    [Flags]
    public enum TransactionState
    {
        None = 0,
        Prepared = 1,
        Done = 2,

        Success = Prepared | Done,
        Fail = Done,
    }

    [DataContract(Namespace = "")]
    public class TransactionVector : VectorBase<TransactionVector, TransactionState>
    {
        public TransactionVector() : base() { }
        public TransactionVector(params VectorItem<TransactionState>[] items) : base(items) { }
        public TransactionVector(string ownServerId, TransactionState init) : base(ownServerId, init) { }

        public static implicit operator TransactionVector((string, TransactionState) t) => new TransactionVector(t.Item1, t.Item2);

        protected override TransactionState Merge(TransactionState left, TransactionState right) =>
            left == TransactionState.Fail || right == TransactionState.Fail ? TransactionState.Fail : (left | right);
    }

    [DataContract(Namespace = "")]
    public class TransactionItem : MessageItem
    {
        [DataMember]
        public VectorRelationship Expected { get; set; }
    }

    // this is an item that contains other items, so it's gonna be a 3-level serialization Matroshka.
    [DataContract(Namespace = "")]
    public class TransactionInfo : IMergeable<TransactionInfo, TransactionInfo>, IDeletable
    {
        [DataMember]
        public string Initiator { get; set; }
        [DataMember]
        public TransactionItem[] Items { get; set; }
        [DataMember]
        public TransactionVector State { get; set; }

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
