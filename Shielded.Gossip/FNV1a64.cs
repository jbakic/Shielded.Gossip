namespace Shielded.Gossip
{
    public static class FNV1a64
    {
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
}
