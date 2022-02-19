using Shielded.Gossip.Serializing;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Backend
{
    internal class StoredItem
    {
        public string Key;

        public byte[] Data;

        public object Value
        {
            get => Data == null ? null : Serializer.Deserialize(Data);
            set => Data = value == null ? null : Serializer.Serialize(value);
        }

        // this will only be != null while a transaction is still open. needed for ApplyTransactions,
        // later parts of a package might introduce dependencies to previous ones, and before a changed
        // item is actually committed, Dependencies == null.
        public GossipBackend.TransactionInfo OpenTransaction;

        // this will actually contain the same Key. unusual maybe, but really helpful.
        public StoredDependency[] Dependencies;

        public long Freshness;

        public bool Deleted;

        public bool Expired;

        public int? RemovableSince;

        public int? ExpiresInMs
        {
            get
            {
                if (!_expiresInMs.HasValue)
                    return null;
                if (!_referenceTickCount.HasValue)
                    return _expiresInMs;
                return unchecked(_expiresInMs.Value + _referenceTickCount.Value - TransactionalTickCount.Value);
            }
            set
            {
                _referenceTickCount = null;
                _expiresInMs = value;
            }
        }
        private int? _expiresInMs;
        private int? _referenceTickCount;

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

        public static explicit operator MessageItem(StoredItem i) => i == null ? null : new MessageItem
        {
            Key = i.Key,
            Data = i.Data,
            Deleted = i.Deleted,
            Expired = i.Expired,
            ExpiresInMs = i.ExpiresInMs,
        };
    }

    internal class StoredDependency
    {
        public string Key;

        public byte[] VersionData;

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
