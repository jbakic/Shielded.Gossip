using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Serializing
{
    /// <summary>
    /// The basic serializer interface that the library requires. It must support
    /// serializing and deserializing without the caller specifying the type, and it
    /// should ideally handle byte[] member fields well, since this library uses byte
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
}
