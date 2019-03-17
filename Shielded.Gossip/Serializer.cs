using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// The basic serializer interface that the library requires. It must support
    /// serializing and deserializing without the caller specifying the type, and it
    /// should ideally handle byte[] fields well, since this library uses byte
    /// arrays internally. Must be thread-safe.
    /// </summary>
    public interface ISerializer
    {
        /// <summary>
        /// Serialize the object, including any needed information on its type.
        /// </summary>
        byte[] Serialize(object obj);

        /// <summary>
        /// Deserialize the object serialized in the given bytes. The serializer must
        /// read the correct type of the object from the given bytes.
        /// </summary>
        object Deserialize(byte[] bytes);
    }

    /// <summary>
    /// A serializer that just (de)serializes the bloody object, with no need to explicitly
    /// specify types. By default uses the <see cref="DataContractSerializer"/>, but you
    /// can replace it with your own using <see cref="Use(ISerializer)"/>.
    /// </summary>
    public static class Serializer
    {
        private static ISerializer _serializer = new DefaultSerializer();

        /// <summary>
        /// Replace the serializer used by this library.
        /// </summary>
        public static void Use(ISerializer serializer)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public static byte[] Serialize(object msg)
        {
            return _serializer.Serialize(msg);
        }

        public static object Deserialize(byte[] bytes)
        {
            return _serializer.Deserialize(bytes);
        }

        private class DefaultSerializer : ISerializer
        {
            public byte[] Serialize(object msg)
            {
                var type = msg.GetType();
                var ser = new DataContractSerializer(type);
                var typeName = TypeId.GetId(type);
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

            public object Deserialize(byte[] bytes)
            {
                var nameLength = BitConverter.ToInt32(bytes, 0);
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
}
