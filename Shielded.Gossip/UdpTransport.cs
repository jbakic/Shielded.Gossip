using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public class UdpTransport : ITransport
    {
        public UdpTransport(string ownId, IPEndPoint localEndpoint, IDictionary<string, IPEndPoint> serverIPs)
        {
            OwnId = ownId;
            LocalEndpoint = localEndpoint;
            ServerIPs = serverIPs;

            _listener = new UdpClient(LocalEndpoint);
            StartListening();
        }

        public string OwnId { get; private set; }
        public readonly IPEndPoint LocalEndpoint;
        public readonly IDictionary<string, IPEndPoint> ServerIPs;
        public ICollection<string> Servers => ServerIPs.Keys;

        private readonly UdpClient _listener;

        public void Dispose()
        {
            _listener.Dispose();
        }

        private async void StartListening()
        {
            while (true)
            {
                try
                {
                    var res = await _listener.ReceiveAsync();
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    Task.Run(() => MessageReceived?.Invoke(this, Serializer.Deserialize(res.Buffer)))
                        .ContinueWith(t =>
                        {
                            if (t.Exception != null)
                                ListenerError?.Invoke(this, t.Exception);
                        });
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    ListenerError?.Invoke(this, ex);
                }
            }
        }

        public event EventHandler<object> MessageReceived;
        public event EventHandler<Exception> ListenerError;

        public Task Broadcast(object msg)
        {
            var bytes = Serializer.Serialize(msg);
            return Task.WhenAll(ServerIPs.Select(s => Send(s.Value, bytes)).ToArray());
        }

        public Task Send(string server, object msg)
        {
            var ip = ServerIPs[server];
            return Send(ip, Serializer.Serialize(msg));
        }

        private async Task Send(IPEndPoint ip, byte[] bytes)
        {
            using (var client = new UdpClient())
            {
                await client.SendAsync(bytes, bytes.Length, ip);
            }
        }
    }
}
