using System.Collections.Generic;
using System.Linq;

namespace Shielded.Gossip
{
    /// <summary>
    /// FNV1a hash implementation, 64 bits.
    /// </summary>
    public static class FNV1a64
    {
        public static ulong Hash(IEnumerable<byte[]> fields)
        {
            return Hash(fields.ToArray());
        }

        public static ulong Hash(params byte[][] fields)
        {
            unchecked
            {
                // FNV-1a implementation for 64 bits
                ulong hash = 14695981039346656037UL;
                const ulong prime = 1099511628211UL;

                for (int f = 0; f < fields.Length; f++)
                {
                    var field = fields[f];
                    for (int i = 0; i < field.Length; i++)
                        hash = (hash ^ field[i]) * prime;
                    hash *= prime;
                }
                return hash;
            }
        }
    }

    /// <summary>
    /// FNV1a hash implementation, 32 bits.
    /// </summary>
    public static class FNV1a32
    {
        public static int Hash(IEnumerable<byte[]> fields)
        {
            return Hash(fields.ToArray());
        }

        public static int Hash(params byte[][] fields)
        {
            unchecked
            {
                // FNV-1a implementation for 32 bits
                uint hash = 2166136261U;
                const uint prime = 16777619U;

                for (int f = 0; f < fields.Length; f++)
                {
                    var field = fields[f];
                    for (int i = 0; i < field.Length; i++)
                        hash = (hash ^ field[i]) * prime;
                    hash *= prime;
                }
                return (int)hash;
            }
        }
    }
}
