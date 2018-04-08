using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
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

        public int ReceiveTimeout { get; set; } = 5000;

        private TcpListener _listener;

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

        public void Dispose()
        {
            StopListening();
        }

        public async void StartListening()
        {
            if (_listener != null)
                throw new InvalidOperationException("Already listening.");
            var listener = new TcpListener(LocalEndpoint);
            if (Interlocked.CompareExchange(ref _listener, listener, null) != null)
                return;
            try
            {
                listener.Start();
                while (true)
                {
                    var client = await listener.AcceptTcpClientAsync();
                    ProcessIncoming(client);
                }
            }
            catch (ObjectDisposedException) { }
            catch (Exception ex)
            {
                StopListening();
                Error?.Invoke(this, ex);
            }
        }

        private async void ProcessIncoming(TcpClient client)
        {
            async Task<int> ReceiveBuffer(NetworkStream stream, byte[] buff)
            {
                int done = 0;
                while (done < buff.Length)
                    done += await stream.ReadAsync(buff, done, buff.Length - done);
                return done;
            }

            byte[] buffer = null;
            try
            {
                client.ReceiveTimeout = ReceiveTimeout;
                var stream = client.GetStream();

                var lengthBytes = new byte[4];
                await ReceiveBuffer(stream, lengthBytes);
                var length = BitConverter.ToInt32(lengthBytes, 0);

                buffer = new byte[length];
                await ReceiveBuffer(stream, buffer);
                client.Close();
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, ex);
                return;
            }
            finally
            {
                try
                {
                    if (client.Connected)
                        client.Close();
                }
                catch { }
            }

            try
            {
                MessageReceived?.Invoke(this, Serializer.Deserialize(buffer));
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, ex);
            }
        }

        public event EventHandler<object> MessageReceived;
        public event EventHandler<Exception> Error;

        public void Broadcast(object msg)
        {
            var bytes = Serializer.Serialize(msg);
            foreach (var ip in ServerIPs.Values)
                Send(ip, bytes);
        }

        public void Send(string server, object msg)
        {
            var ip = ServerIPs[server];
            Send(ip, Serializer.Serialize(msg));
        }

        private async void Send(IPEndPoint ip, byte[] bytes)
        {
            try
            {
                using (var client = new TcpClient())
                {
                    await client.ConnectAsync(ip.Address, ip.Port);
                    var stream = client.GetStream();

                    var lengthBytes = BitConverter.GetBytes(bytes.Length);
                    await stream.WriteAsync(lengthBytes, 0, 4);
                    await stream.WriteAsync(bytes, 0, bytes.Length);

                    client.Close();
                }
            }
            catch (Exception ex)
            {
                Error?.Invoke(this, ex);
            }
        }
    }
}
