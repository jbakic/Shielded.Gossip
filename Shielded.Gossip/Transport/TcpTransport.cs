using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shielded.Gossip.Serializing;
using Shielded.Gossip.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Transport
{
    /// <summary>
    /// Implementation of <see cref="ITransport"/> using a very simple TCP-based protocol.
    /// </summary>
    public class TcpTransport : ITransport
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ownId">The ID of this server.</param>
        /// <param name="serverIPs">The dictionary with server IDs and IP endpoints, including this server. Do not make later
        /// changes to any of the IPEndPoint objects!</param>
        /// <param name="logger">The logger to use.</param>
        public TcpTransport(string ownId, IDictionary<string, IPEndPoint> serverIPs, ILogger logger = null)
        {
            OwnId = ownId ?? throw new ArgumentNullException(nameof(ownId));
            _logger = logger ?? NullLogger.Instance;
            if (serverIPs == null)
                throw new ArgumentNullException(nameof(serverIPs));
            LocalEndpoint = serverIPs[ownId];
            (ServerIPs, _clientConnections) = Shield.InTransaction(() =>
            {
                var ips = new ShieldedDict<string, IPEndPoint>(serverIPs.Where(kvp => !StringComparer.InvariantCultureIgnoreCase.Equals(kvp.Key, ownId)));
                var clients = new ConcurrentDictionary<string, TcpClientConnection>(
                    ips.Select(kvp => new KeyValuePair<string, TcpClientConnection>(kvp.Key, new TcpClientConnection(this, kvp.Value, _logger))));
                Shield.PreCommit(() => ips.TryGetValue("any", out var _) || true,
                    () => Shield.SyncSideEffect(UpdateClientConnections));
                return (ips, clients);
            });
        }

        public string OwnId { get; private set; }
        public readonly IPEndPoint LocalEndpoint;
        /// <summary>
        /// Other servers known to this one. You may make changes to the dictionary, but please treat all IPEndPoint objects
        /// as immutable! Create a new IPEndPoint if you wish to change the address of a server.
        /// </summary>
        public readonly ShieldedDict<string, IPEndPoint> ServerIPs;
        public ICollection<string> Servers => ServerIPs.Keys;

        /// <summary>
        /// Timeout in milliseconds for detecting a half-open connection. Default is 30 seconds.
        /// </summary>
        public int ReceiveTimeout { get; set; } = 30000;

        /// <summary>
        /// Every this many milliseconds we transmit a keep-alive message over our active persistent connections,
        /// if nothing else gets sent. Default is 15 seconds. Should be smaller than the <see cref="ReceiveTimeout"/>, of course.
        /// </summary>
        public int KeepAliveInterval { get; set; } = 15000;

        public MessageHandler MessageHandler { get; set; }

        private TcpListener _listener;
        private readonly object _listenerLock = new object();
        private readonly ConcurrentDictionary<string, TcpClientConnection> _clientConnections;
        private readonly ConcurrentDictionary<TcpClient, object> _serverConnections = new ConcurrentDictionary<TcpClient, object>();
        private readonly ILogger _logger;

        private void UpdateClientConnections()
        {
            using (_logger.BeginScope("Updating client connections"))
            {
                foreach (var key in ServerIPs.Changes)
                {
                    if (!ServerIPs.TryGetValue(key, out var endpoint))
                    {
                        if (_clientConnections.TryRemove(key, out var conn))
                        {
                            conn.Dispose();
                        }
                    }
                    else if (!_clientConnections.TryGetValue(key, out var conn))
                    {
                        _clientConnections[key] = new TcpClientConnection(this, endpoint, _logger);
                    }
                    else if (conn.TargetEndPoint != endpoint)
                    {
                        conn.Dispose();
                        _clientConnections[key] = new TcpClientConnection(this, endpoint, _logger);
                    }
                }
            }
        }

        /// <summary>
        /// Stop the server. Safe to call if already stopped.
        /// </summary>
        public void StopListening()
        {
            _logger.LogInformation("StopListening called.");
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
            }
        }

        /// <summary>
        /// Calls <see cref="StopListening"/>, and also disposes all outgoing persistent connections.
        /// </summary>
        public void Dispose()
        {
            using (_logger.BeginScope("Transport disposal"))
            {
                StopListening();
                foreach (var clConn in _clientConnections.Values)
                    clConn.Dispose();
                foreach (var serverConn in _serverConnections.Keys)
                {
                    _logger.LogInformation("Disposing received connection from {RemoteEndPoint}", serverConn.Client.RemoteEndPoint);
                    serverConn.Dispose();
                }
                _serverConnections.Clear();
            }
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
            _logger.LogInformation("Starting to listen for incoming connections");
            try
            {
                listener.Start();
                while (true)
                {
                    var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _logger.LogInformation("Incoming connection from {RemoteEndPoint}", client.Client.RemoteEndPoint);
                    client.ReceiveTimeout = ReceiveTimeout;
                    _serverConnections.TryAdd(client, null);
                    var stream = client.GetStream();
                    MessageLoop(client, msg => SendFramed(stream, msg),
                        (c, ex) =>
                        {
                            if (_serverConnections.TryRemove(c, out var _) && ex != null)
                            {
                                _logger.LogWarning(ex, "Lost incoming connection from {RemoteEndPoint}", c.Client?.RemoteEndPoint);
                                RaiseError(ex);
                            }
                        });
                }
            }
            catch (ObjectDisposedException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while listening for incoming connections.");
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
                    var length = SafeBitConverter.ToInt32(lengthBytes, 0);
                    if (length == 0)
                        continue;

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
            var lengthBytes = SafeBitConverter.GetBytes(bytes.Length);
            await stream.WriteAsync(lengthBytes, 0, 4).ConfigureAwait(false);
            if (bytes.Length > 0)
                await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        }
    }
}
