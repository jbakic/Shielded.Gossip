using System;
using System.Collections.Generic;
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

        /// <summary>
        /// Stop the server. Safe to call if already stopped.
        /// </summary>
        public void StopListening()
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

        /// <summary>
        /// Just calls <see cref="StopListening"/>, the object may still be used.
        /// </summary>
        public void Dispose()
        {
            StopListening();
        }

        /// <summary>
        /// Start the server. Safe to call if already running, does nothing then.
        /// </summary>
        public async void StartListening()
        {
            if (_listener != null)
                return;
            var listener = new TcpListener(LocalEndpoint);
            if (Interlocked.CompareExchange(ref _listener, listener, null) != null)
                return;
            try
            {
                listener.Start();
                while (true)
                {
                    var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    MessageLoop(client);
                }
            }
            catch (ObjectDisposedException) { }
            catch (Exception ex)
            {
                StopListening();
                Error?.Invoke(this, ex);
            }
        }

        private async void MessageLoop(TcpClient client)
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
                        return;
                    var length = BitConverter.ToInt32(lengthBytes, 0);

                    buffer = new byte[length];
                    if (!await ReceiveBuffer(stream, buffer).ConfigureAwait(false))
                        return;

                    var reply = await Receive(buffer).ConfigureAwait(false);
                    if (reply == null)
                        break;
                    await SendFramed(stream, reply).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, ex);
            }
            finally
            {
                client.Close();
            }
        }

        private async Task<byte[]> Receive(byte[] msg)
        {
            if (MessageHandler == null)
                return null;
            var response = await MessageHandler(Serializer.Deserialize(msg)).ConfigureAwait(false);
            return response == null ? null : Serializer.Serialize(response);
        }

        /// <summary>
        /// Event raised when any error occurs. May be a listener or a sender error.
        /// </summary>
        public event EventHandler<Exception> Error;

        public void Broadcast(object msg)
        {
            var bytes = Serializer.Serialize(msg);
            foreach (var ip in ServerIPs.Values)
                Send(ip, bytes, false);
        }

        public void Send(string server, object msg, bool replyExpected)
        {
            if (ServerIPs.TryGetValue(server, out var ip))
                Send(ip, Serializer.Serialize(msg), replyExpected);
        }

        /// <summary>
        /// Sends a message to the given endpoint, minimally framed - we send the length, 32 bits, and
        /// then the bytes.
        /// </summary>
        private async void Send(IPEndPoint ip, byte[] bytes, bool replyExpected)
        {
            var client = new TcpClient();
            try
            {
                await client.ConnectAsync(ip.Address, ip.Port).ConfigureAwait(false);
                var stream = client.GetStream();
                await SendFramed(stream, bytes).ConfigureAwait(false);

                if (replyExpected)
                    MessageLoop(client);
                else
                    client.Close();
            }
            catch (Exception ex)
            {
                client.Close();
                Error?.Invoke(this, ex);
            }
        }

        private static async Task SendFramed(NetworkStream stream, byte[] bytes)
        {
            var lengthBytes = BitConverter.GetBytes(bytes.Length);
            await stream.WriteAsync(lengthBytes, 0, 4).ConfigureAwait(false);
            await stream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        }
    }
}
