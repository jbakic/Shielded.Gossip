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
                while (_messageQueue.Count >= MaxQueueLength)
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
            TcpClient client = new TcpClient();
            lock (_lock)
            {
                _client = client;
            }

            try
            {
                await client.ConnectAsync(_targetEndPoint.Address, _targetEndPoint.Port).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                lock (_lock)
                {
                    _messageQueue.Clear();
                    _client = null;
                    _state = State.Disconnected;
                }
                Transport.RaiseError(ex);
                return;
            }

            lock (_lock)
            {
                _state = State.Sending;
            }
            WriterLoop(client);
            Transport.MessageLoop(client, async msg => Send(msg), OnError);
        }

        private void OnError(TcpClient client, Exception ex)
        {
            lock (_lock)
            {
                if (_client != client)
                    return; // skipping the RaiseError call below!
                _client = null;
                if (_state == State.Sending)
                {
                    _state = State.Connecting;
                    Task.Run(Connect);
                }
                else if (_state == State.Connected)
                {
                    _state = State.Disconnected;
                }
            }
            Transport.RaiseError(ex);
        }

        private async void WriterLoop(TcpClient client)
        {
            try
            {
                while (true)
                {
                    byte[] msg;
                    lock (_lock)
                    {
                        if (_client != client)
                            return;
                        msg = _messageQueue.Peek();
                    }
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
            }
            catch (Exception ex)
            {
                try { client.Close(); } catch { }
                OnError(client, ex);
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
