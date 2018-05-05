using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// A serializer that just (de)serializes the bloody object, with no need to explicitly
    /// specify types. Currently hard-coded to use <see cref="DataContractSerializer"/>.
    /// </summary>
    public static class Serializer
    {
        public static byte[] Serialize(object msg)
        {
            var type = msg.GetType();
            var ser = new DataContractSerializer(type);
            var typeName = TypeId.Get(type);
            var nameBytes = Encoding.UTF8.GetBytes(typeName);
            var lengthBytes = BitConverter.GetBytes(nameBytes.Length);
            using (var ms = new MemoryStream())
            {
                ms.Write(lengthBytes, 0, lengthBytes.Length);
                ms.Write(nameBytes, 0, nameBytes.Length);
                ser.WriteObject(ms, msg);
                return ms.ToArray();
            }
        }

        public static object Deserialize(byte[] bytes)
        {
            var nameLength = BitConverter.ToInt32(bytes, 0);
            var name = Encoding.UTF8.GetString(bytes, 4, nameLength);
            var type = Type.GetType(name);
            if (type == null)
                throw new ApplicationException("Unable to read message type: " + name);
            var spent = 4 + nameLength;
            var ser = new DataContractSerializer(type);
            using (var ms = new MemoryStream(bytes, spent, bytes.Length - spent))
                return ser.ReadObject(ms);
        }
    }
}
