using System;
using System.Collections.Generic;

namespace Shielded.Gossip
{
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
        /// </summary>
        void Send(string server, object msg);

        /// <summary>
        /// Event raised when a new message is received. The gossip backends will
        /// subscribe to this.
        /// </summary>
        event EventHandler<object> MessageReceived;
    }
}
