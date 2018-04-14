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

        protected override TransactionState Merge(TransactionState left, TransactionState right) => left | right;
    }

    [DataContract(Namespace = "")]
    public class TransactionItem : MessageItem
    {
        [DataMember]
        public VectorRelationship Expected { get; set; }
    }

    // this is an item that contains other items, so it's gonna be a 3-level serialization Matroshka.
    [DataContract(Namespace = "")]
    public class TransactionInfo : IMergeable<TransactionInfo, TransactionInfo>
    {
        [DataMember]
        public TransactionItem[] Items { get; set; }
        [DataMember]
        public TransactionVector State { get; set; }

        public VersionHash GetVersionHash() => (State ?? new TransactionVector()).GetVersionHash();

        public TransactionInfo MergeWith(TransactionInfo other)
        {
            // we assume the items are the same...
            return new TransactionInfo { Items = Items, State = (State ?? new TransactionVector()).MergeWith(other?.State) };
        }

        public VectorRelationship VectorCompare(TransactionInfo other) => (State ?? new TransactionVector()).VectorCompare(other?.State);
    }
}
