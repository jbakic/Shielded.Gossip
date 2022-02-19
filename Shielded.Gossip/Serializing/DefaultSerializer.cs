using Shielded.Gossip.Utils;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip.Serializing
{
    /// <summary>
    /// The default serializer used by Shielded.Gossip. Uses the <see cref="DataContractSerializer"/>.
    /// </summary>
    public class DefaultSerializer : ISerializer
    {
        public byte[] Serialize(object msg)
        {
            var type = msg.GetType();
            var ser = new DataContractSerializer(type);
            var typeName = TypeId.GetId(type);
            var nameBytes = Encoding.UTF8.GetBytes(typeName);
            var lengthBytes = SafeBitConverter.GetBytes(nameBytes.Length);
            using (var ms = new MemoryStream())
            {
                ms.Write(lengthBytes, 0, lengthBytes.Length);
                ms.Write(nameBytes, 0, nameBytes.Length);
                ser.WriteObject(ms, msg);
                return ms.ToArray();
            }
        }

        public object Deserialize(byte[] bytes)
        {
            var nameLength = SafeBitConverter.ToInt32(bytes, 0);
            var name = Encoding.UTF8.GetString(bytes, 4, nameLength);
            var type = TypeId.GetType(name);
            if (type == null)
                throw new ApplicationException("Unable to read message type: " + name);
            var spent = 4 + nameLength;
            var ser = new DataContractSerializer(type);
            using (var ms = new MemoryStream(bytes, spent, bytes.Length - spent))
                return ser.ReadObject(ms);
        }
    }
}
