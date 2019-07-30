using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> using a very simple TCP-based protocol.
    /// </summary>
    public class TcpTransport : ITransport
    {
        public TcpTransport(string ownId, IPEndPoint localEndpoint, IDictionary<string, IPEndPoint> serverIPs)
        {
            OwnId = ownId;
            LocalEndpoint = localEndpoint;
            ServerIPs = serverIPs;
            // TODO: this does not react to changes in ServerIPs!
            _clientConnections = Shield.InTransaction(() => serverIPs.ToDictionary(kvp => kvp.Key, kvp => new TcpClientConnection(this, kvp.Value)));
        }

        public string OwnId { get; private set; }
        public readonly IPEndPoint LocalEndpoint;
        public readonly IDictionary<string, IPEndPoint> ServerIPs;
        public ICollection<string> Servers => ServerIPs.Keys;

        /// <summary>
        /// Timeout for detecting a half-open connection. If we are expecting (more) data,
        /// and receive nothing for this long, we terminate the connection.
        /// </summary>
        public int ReceiveTimeout { get; set; } = 5000;

        public MessageHandler MessageHandler { get; set; }

        private TcpListener _listener;
        private readonly object _listenerLock = new object();
        private readonly Dictionary<string, TcpClientConnection> _clientConnections;
        private readonly ConcurrentDictionary<TcpClient, object> _serverConnections = new ConcurrentDictionary<TcpClient, object>();

        /// <summary>
        /// Stop the server. Safe to call if already stopped.
        /// </summary>
        public void StopListening()
        {
            lock (_listenerLock)
            {
                var listener = _listener;
                if (listener != null)
                {
                    try
                    {
                        listener.Stop();
                    }
                    catch { }
                    _listener = null;
                }

                foreach (var serverConn in _serverConnections.Keys)
                    serverConn.Dispose();
                _serverConnections.Clear();
            }
        }

        /// <summary>
        /// Just calls <see cref="StopListening"/>, the object may still be used.
        /// </summary>
        public void Dispose()
        {
            StopListening();
            foreach (var clConn in _clientConnections.Values)
                clConn.Dispose();
        }

        /// <summary>
        /// Start the server. Safe to call if already running, does nothing then.
        /// </summary>
        public async void StartListening()
        {
            TcpListener listener;
            lock (_listenerLock)
            {
                if (_listener != null)
                    return;
                listener = _listener = new TcpListener(LocalEndpoint);
            }
            try
            {
                listener.Start();
                while (true)
                {
                    var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _serverConnections.TryAdd(client, null);
                    var stream = client.GetStream();
                    MessageLoop(client, msg => SendFramed(stream, msg),
                        (c, ex) =>
                        {
                            if (_serverConnections.TryRemove(c, out var _) && ex != null)
                                RaiseError(ex);
                        });
                }
            }
            catch (ObjectDisposedException) { }
            catch (Exception ex)
            {
                StopListening();
                RaiseError(ex);
            }
        }

        internal async void MessageLoop(TcpClient client, Func<byte[], Task> sender, Action<TcpClient, Exception> onCloseOrError)
        {
            async Task<bool> ReceiveBuffer(NetworkStream ns, byte[] buff)
            {
                int done = 0;
                while (done < buff.Length)
                {
                    var read = await ns.ReadAsync(buff, done, buff.Length - done).ConfigureAwait(false);
                    if (read == 0)
                        return false;
                    done += read;
                }
                return true;
            }

            try
            {
                client.ReceiveTimeout = ReceiveTimeout;
                var stream = client.GetStream();
                while (client.Connected)
                {
                    byte[] buffer = null;
                    var lengthBytes = new byte[4];
                    if (!await ReceiveBuffer(stream, lengthBytes).ConfigureAwait(false))
                    {
                        onCloseOrError(client, null);
                        return;
                    }
                    var length = BitConverter.ToInt32(lengthBytes, 0);

                    buffer = new byte[length];
                    if (!await ReceiveBuffer(stream, buffer).ConfigureAwait(false))
                    {
                        onCloseOrError(client, null);
                        return;
                    }

                    var reply = Receive(buffer);
                    if (reply != null)
                        await sender(reply).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                try { client.Close(); } catch { }
                onCloseOrError(client, ex);
            }
        }

        private byte[] Receive(byte[] msg)
        {
            if (MessageHandler == null)
                return null;
            var response = MessageHandler(Serializer.Deserialize(msg));
            return response == null ? null : Serializer.Serialize(response);
        }

        /// <summary>
        /// Event raised when any error occurs. May be a listener or a sender error.
        /// </summary>
        public event EventHandler<Exception> Error;

        public void RaiseError(Exception ex)
        {
            Error?.Invoke(this, ex);
        }

        public void Broadcast(object msg)
        {
            var bytes = Serializer.Serialize(msg);
            foreach (var conn in _clientConnections.Values)
                conn.Send(bytes);
        }

        public void Send(string server, object msg, bool replyExpected)
        {
            if (_clientConnections.TryGetValue(server, out var conn))
                conn.Send(Serializer.Serialize(msg));
        }

        internal static async Task SendFramed(NetworkStream stream, byte[] bytes)
        {
            var lengthBytes = BitConverter.GetBytes(bytes.Length);
            await stream.WriteAsync(lengthBytes, 0, 4).ConfigureAwait(false);
            await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        }
    }
}
