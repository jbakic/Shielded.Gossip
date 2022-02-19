using System;
using System.Runtime.Serialization;

namespace Shielded.Gossip.Serializing
{
    /// <summary>
    /// A serializer that just (de)serializes the bloody object, with no need to explicitly
    /// specify types. By default uses the <see cref="DefaultSerializer"/>, which uses the
    /// <see cref="DataContractSerializer"/>, but you can replace it with your own using
    /// <see cref="Use(ISerializer)"/>.
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
    }
}
