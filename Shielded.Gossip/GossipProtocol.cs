using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public class GossipProtocol : IDisposable
    {
        private readonly UdpClient _listener;

        public GossipProtocol(string ownId, IDictionary<string, IPEndPoint> servers, IPEndPoint localEndpoint)
        {
            OwnId = ownId;
            Servers = servers;
            _listener = new UdpClient(localEndpoint);
            StartListening();
        }

        public readonly string OwnId;
        public readonly IDictionary<string, IPEndPoint> Servers;

        private volatile bool _disposed;

        public void Dispose()
        {
            if (!_disposed)
            {
                _listener.Dispose();
                _disposed = true;
            }
        }

        private async void StartListening()
        {
            try
            {
                while (!_disposed)
                {
                    var res = await _listener.ReceiveAsync();
                    MessageReceived?.Invoke(this, Serializer.Deserialize<Message>(res.Buffer));
                }
            }
            catch (Exception ex)
            {
                ListenerException = ex;
            }
        }

        public Exception ListenerException { get; private set; }

        public event EventHandler<Message> MessageReceived;

        public Task Broadcast(Message msg)
        {
            msg.From = OwnId;
            msg.To = null;
            return Task.WhenAll(Servers.Select(s => Send(s.Key, msg)).ToArray());
        }

        public Task Send(string server, Message msg)
        {
            var ip = Servers[server];
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
