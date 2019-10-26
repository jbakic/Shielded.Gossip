using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Like BitConverter, but always little-endian.
    /// </summary>
    public static class SafeBitConverter
    {
        public static byte[] GetBytes(int val)
        {
            var res = BitConverter.GetBytes(val);
            if (BitConverter.IsLittleEndian)
                return res;
            Array.Reverse(res);
            return res;
        }

        public static byte[] GetBytes(long val)
        {
            var res = BitConverter.GetBytes(val);
            if (BitConverter.IsLittleEndian)
                return res;
            Array.Reverse(res);
            return res;
        }

        public static int ToInt32(byte[] bytes, int startIndex)
        {
            if (BitConverter.IsLittleEndian)
                return BitConverter.ToInt32(bytes, startIndex);
            var rev = new byte[4];
            Array.Copy(bytes, startIndex, rev, 0, 4);
            Array.Reverse(rev);
            return BitConverter.ToInt32(rev, 0);
        }
    }
}
