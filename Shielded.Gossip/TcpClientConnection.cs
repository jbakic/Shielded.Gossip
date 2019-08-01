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

        private readonly object _lock = new object();
        private State _state = State.Disconnected;
        private TcpClient _client;
        private readonly Queue<byte[]> _messageQueue = new Queue<byte[]>();
        private Timer _keepAliveTimer;

        public readonly TcpTransport Transport;
        public readonly IPEndPoint TargetEndPoint;

        public TcpClientConnection(TcpTransport transport, IPEndPoint targetEndPoint)
        {
            Transport = transport;
            TargetEndPoint = targetEndPoint;
        }

        public void Send(byte[] message)
        {
            lock (_lock)
            {
                while (_messageQueue.Count >= MaxQueueLength)
                    _messageQueue.Dequeue();
                _messageQueue.Enqueue(message);

                if (_state == State.Disconnected)
                    StartConnecting();
                else if (_state == State.Connected)
                    StartSending();
            }
        }

        private void StartConnecting()
        {
            _state = State.Connecting;
            var client = _client = new TcpClient() { ReceiveTimeout = Transport.ReceiveTimeout };
            Task.Run(() => Connect(client));
        }

        private void StartSending()
        {
            _state = State.Sending;
            if (_keepAliveTimer != null)
            {
                _keepAliveTimer.Dispose();
                _keepAliveTimer = null;
            }
            var client = _client;
            Task.Run(() => WriterLoop(client));
        }

        private async void Connect(TcpClient client)
        {
            try
            {
                await client.ConnectAsync(TargetEndPoint.Address, TargetEndPoint.Port).ConfigureAwait(false);
                lock (_lock)
                {
                    if (_client != client)
                    {
                        try { client.Close(); } catch { }
                        return;
                    }
                    StartSending();
                }
                Transport.MessageLoop(client, async msg => Send(msg), OnCloseOrError);
            }
            catch (Exception ex)
            {
                OnCloseOrError(client, ex);
            }
        }

        private void OnCloseOrError(TcpClient client, Exception ex)
        {
            try { client.Close(); } catch { }
            lock (_lock)
            {
                if (_client != client)
                    return; // skipping the RaiseError call below!
                _client = null;

                if (_state == State.Sending && _messageQueue.Count > 0)
                {
                    StartConnecting();
                }
                else
                {
                    _state = State.Disconnected;
                    _messageQueue.Clear();
                }

                if (_keepAliveTimer != null)
                {
                    _keepAliveTimer.Dispose();
                    _keepAliveTimer = null;
                }
            }
            if (ex != null)
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
                        if (_client != client)
                            return;
                        if (_messageQueue.Count == 0)
                        {
                            _state = State.Connected;
                            // a bit of paranoia:
                            if (_keepAliveTimer != null)
                                _keepAliveTimer.Dispose();
                            _keepAliveTimer = new Timer(_ => SendKeepAlive(client), null, Transport.KeepAliveInterval, Transport.KeepAliveInterval);
                            return;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                OnCloseOrError(client, ex);
            }
        }

        private async void SendKeepAlive(TcpClient client)
        {
            try
            {
                lock (_lock)
                {
                    if (_client != client || _state != State.Connected)
                        return;
                    // to block any concurrent attempts to send, since only one thread may send over a TcpClient at one time.
                    _state = State.Sending;
                }
                await TcpTransport.SendFramed(client.GetStream(), new byte[0]);
                lock (_lock)
                {
                    if (_client != client || _state != State.Sending)
                        return;
                    if (_messageQueue.Count > 0)
                        StartSending();
                    else
                        _state = State.Connected;
                }
            }
            catch (Exception ex)
            {
                OnCloseOrError(client, ex);
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                _state = State.Disconnected;
                _messageQueue.Clear();
                if (_client != null)
                {
                    try { _client.Close(); } catch { }
                    _client = null;
                }
                if (_keepAliveTimer != null)
                {
                    _keepAliveTimer.Dispose();
                    _keepAliveTimer = null;
                }
            }
        }
    }
}
