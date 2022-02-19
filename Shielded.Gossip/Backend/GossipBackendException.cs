using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;

namespace Shielded.Gossip.Backend
{
    /// <summary>
    /// Base class for exceptions which occur on background tasks of the gossip backends.
    /// </summary>
    [Serializable]
    public class GossipBackendException : Exception
    {
        public GossipBackendException() { }

        public GossipBackendException(string message) : base(message) { }

        public GossipBackendException(string message, Exception innerException)
            : base(message, innerException)
        { }

        protected GossipBackendException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }
    }
}
