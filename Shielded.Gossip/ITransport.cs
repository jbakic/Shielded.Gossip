using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    /// <summary>
    /// Delegate for handling messages. Result will be a reply message, which may be null.
    /// Doing it this way helps reuse the same connection for replying, for transports
    /// which have a connection concept.
    /// </summary>
    public delegate object MessageHandler(object message);

    /// <summary>
    /// The interface to implement if you wish to use a custom means of communication
    /// between the gossip nodes. This library includes the <see cref="TcpTransport"/>,
    /// but it should be easy to use anything you want.
    /// </summary>
    public interface ITransport : IDisposable
    {
        /// <summary>
        /// Gets the ID of this server.
        /// </summary>
        string OwnId { get; }

        /// <summary>
        /// Gets a collection of all known servers participating in the cluster.
        /// </summary>
        ICollection<string> Servers { get; }

        /// <summary>
        /// Sends a message to all servers. Fire and forget, should not throw.
        /// </summary>
        void Broadcast(object msg);

        /// <summary>
        /// Sends a message to a specific server. Fire and forget, should not throw.
        /// The replyExpected argument can be used to keep a connection open, if the
        /// transport has such a concept, which may be more efficient.
        /// </summary>
        void Send(string server, object msg, bool replyExpected);

        /// <summary>
        /// Handler for incoming messages. Backends will set this. The result of calling
        /// it will be the reply message, or null if no reply is needed.
        /// </summary>
        MessageHandler MessageHandler { get; set; }
    }
}
