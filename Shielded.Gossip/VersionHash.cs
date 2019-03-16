using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography;

namespace Shielded.Gossip
{
    /// <summary>
    /// Type of the hash used by the <see cref="GossipBackend"/>. Uses SHA-256.
    /// </summary>
    [DataContract(Namespace = ""), Serializable]
    public struct VersionHash : IEquatable<VersionHash>
    {
        [DataMember]
        public byte[] Data { get; set; }

        public VersionHash(byte[] data)
        {
            if (data != null && data.Length != 32)
                throw new ArgumentException(nameof(data));
            Data = data;
        }

        private static readonly byte[] _delimiter = new byte[] { 0 };

        public static VersionHash Hash(IEnumerable<byte[]> fieldsToHash) => Hash(fieldsToHash.ToArray());
        public static VersionHash Hash(params byte[][] fieldsToHash)
        {
            if (fieldsToHash == null || fieldsToHash.Length == 0)
                return default;
            using (var sha = SHA256.Create())
            {
                for (var i = 0; i < fieldsToHash.Length; i++)
                {
                    var field = fieldsToHash[i];
                    sha.TransformBlock(field, 0, field.Length, field, 0);
                    if (i == fieldsToHash.Length - 1)
                        sha.TransformFinalBlock(_delimiter, 0, _delimiter.Length);
                    else
                        sha.TransformBlock(_delimiter, 0, _delimiter.Length, _delimiter, 0);
                }
                return new VersionHash(sha.Hash);
            }
        }

        public bool Equals(VersionHash other) => Util.IsByteEqual(Data, other.Data);
        public override bool Equals(object obj) => obj is VersionHash vh && Equals(vh);

        public override int GetHashCode()
        {
            if (Data == null)
                return 0;
            unchecked
            {
                int res = 0;
                for (int i = 0; i < 8; i++)
                {
                    res ^= Data[i * 4] << 24;
                    res ^= Data[i * 4 + 1] << 16;
                    res ^= Data[i * 4 + 2] << 8;
                    res ^= Data[i * 4 + 3];
                }
                return res;
            }
        }

        public override string ToString() => Data == null ? "" : Convert.ToBase64String(Data);

        public static bool operator ==(VersionHash left, VersionHash right) => left.Equals(right);
        public static bool operator !=(VersionHash left, VersionHash right) => !left.Equals(right);

        public static VersionHash operator ^(VersionHash left, VersionHash right) =>
            new VersionHash(
                left.Data == null ? right.Data :
                right.Data == null ? left.Data :
                left.Data.Zip(right.Data, (a, b) => (byte)(a ^ b)).ToArray());

        /// <summary>
        /// To be used when XORing multiple hashes, to avoid constantly allocating new byte arrays.
        /// </summary>
        public void XorWith(VersionHash other)
        {
            if (other.Data == null)
                return;
            if (Data == null)
            {
                Data = other.Data.ToArray();
                return;
            }
            for (int i = 0; i < 32; i++)
                Data[i] ^= other.Data[i];
        }
    }
}
