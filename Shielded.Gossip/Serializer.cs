using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace Shielded.Gossip
{
    public static class Serializer
    {
        public static byte[] Serialize(object obj)
        {
            var ser = new BinaryFormatter();
            using (var ms = new MemoryStream())
            {
                ser.Serialize(ms, obj);
                return ms.ToArray();
            }
        }

        public static T Deserialize<T>(byte[] bytes)
        {
            var ser = new BinaryFormatter();
            using (var ms = new MemoryStream(bytes))
                return (T)ser.Deserialize(ms);
        }
    }
}
