using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    class TcpClientConnection : IDisposable
    {
        public const int MaxQueueLength = 100;

        private enum State
        {
            Disconnected,
            Connecting,
            Connected,
            Sending
        }

        private readonly IPEndPoint _targetEndPoint;

        private readonly object _lock = new object();
        private State _state = State.Disconnected;
        private TcpClient _client;
        private readonly Queue<byte[]> _messageQueue = new Queue<byte[]>();

        public readonly TcpTransport Transport;

        public TcpClientConnection(TcpTransport transport, IPEndPoint targetEndPoint)
        {
            Transport = transport;
            _targetEndPoint = targetEndPoint;
        }

        public void Send(byte[] message)
        {
            lock (_lock)
            {
                if (_messageQueue.Count >= MaxQueueLength)
                    _messageQueue.Dequeue();
                _messageQueue.Enqueue(message);

                if (_state == State.Disconnected)
                {
                    _state = State.Connecting;
                    Task.Run(Connect);
                }
                else if (_state == State.Connected)
                {
                    _state = State.Sending;
                    var client = _client;
                    Task.Run(() => WriterLoop(client));
                }
            }
        }

        private async void Connect()
        {
            var client = _client = new TcpClient();
            try
            {
                await client.ConnectAsync(_targetEndPoint.Address, _targetEndPoint.Port).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Transport.RaiseError(ex);
                lock (_lock)
                {
                    _messageQueue.Clear();
                    _client = null;
                    _state = State.Disconnected;
                }
                return;
            }
            lock (_lock)
            {
                _state = State.Sending;
            }
            WriterLoop(client);
            Transport.MessageLoop(client, async msg => Send(msg));
        }

        private async void WriterLoop(TcpClient client)
        {
            while (client.Connected)
            {
                byte[] msg;
                lock (_lock)
                {
                    msg = _messageQueue.Peek();
                }
                try
                {
                    await TcpTransport.SendFramed(client.GetStream(), msg);
                    lock (_lock)
                    {
                        if (_messageQueue.Peek() == msg)
                            _messageQueue.Dequeue();
                        if (_messageQueue.Count == 0)
                        {
                            _state = State.Connected;
                            return;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Transport.RaiseError(ex);
                    try { client.Close(); } catch { }
                    lock (_lock)
                    {
                        _state = State.Connecting;
                        Task.Run(Connect);
                    }
                    return;
                }
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                var client = _client;
                if (client == null)
                    return;
                try { client.Close(); } catch { }
                _client = null;
                _state = State.Disconnected;
            }
        }
    }
}
