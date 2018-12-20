using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Shielded.Gossip
{
    /// <summary>
    /// Type of the hash used by the <see cref="GossipBackend"/>. It contains a 32bit and a
    /// 64bit FNV1a hash, which greatly reduces the risk of a hash conflict.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct VersionHash : IEquatable<VersionHash>
    {
        [DataMember]
        public ulong LongHash { get; set; }
        [DataMember]
        public int IntHash { get; set; }

        public VersionHash(ulong l, int i)
        {
            LongHash = l;
            IntHash = i;
        }

        public static VersionHash Hash(IEnumerable<byte[]> fieldsToHash) => Hash(fieldsToHash.ToArray());
        public static VersionHash Hash(params byte[][] fieldsToHash) =>
            new VersionHash(
                FNV1a64.Hash(fieldsToHash),
                FNV1a32.Hash(fieldsToHash));

        public bool Equals(VersionHash other) => IntHash == other.IntHash && LongHash == other.LongHash;
        public override bool Equals(object obj) => obj is VersionHash vh && Equals(vh);
        public override int GetHashCode() => unchecked((int)(LongHash >> 32) ^ (int)LongHash ^ IntHash);
        public override string ToString() => $"{LongHash}, {IntHash}";

        public static bool operator ==(VersionHash left, VersionHash right) => left.Equals(right);
        public static bool operator !=(VersionHash left, VersionHash right) => !left.Equals(right);

        public static VersionHash operator ^(VersionHash left, VersionHash right) =>
            new VersionHash(left.LongHash ^ right.LongHash, left.IntHash ^ right.IntHash);

        public byte[] GetBytes() => BitConverter.GetBytes(IntHash).Concat(BitConverter.GetBytes(LongHash)).ToArray();
    }
}
