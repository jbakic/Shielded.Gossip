﻿using System.Collections.Generic;
using System.Linq;

namespace Shielded.Gossip
{
    public struct VersionHash
    {
        public ulong Value;

        public static implicit operator ulong(VersionHash v) => v.Value;
        public static implicit operator VersionHash(ulong v) => new VersionHash { Value = v };
    }

    public static class FNV1a64
    {
        public static VersionHash Hash(IEnumerable<byte[]> fields)
        {
            return Hash(fields.ToArray());
        }

        public static VersionHash Hash(params byte[][] fields)
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
}
