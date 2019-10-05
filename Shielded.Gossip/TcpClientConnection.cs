using Microsoft.Extensions.Logging;
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

        public enum State
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

        private readonly ILogger _logger;

        public TcpClientConnection(TcpTransport transport, IPEndPoint targetEndPoint, ILogger logger)
        {
            Transport = transport;
            TargetEndPoint = targetEndPoint;
            _logger = logger;
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
            _logger.LogInformation("Opening connection to {TargetEndPoint}", TargetEndPoint);
            _state = State.Connecting;
            var client = _client = new TcpClient() { ReceiveTimeout = Transport.ReceiveTimeout };
            using (ExecutionContext.SuppressFlow())
                // this is mostly just needed for ASP.NET WebForms which actively prohibits any awaits on its threads.
                Task.Run(() => Connect(client));
        }

        private void StartSending()
        {
            _logger.LogDebug("Starting to send messages to {TargetEndPoint}. {MessageQueueLength} messages in queue.",
                TargetEndPoint, _messageQueue.Count);
            _state = State.Sending;
            if (_keepAliveTimer != null)
            {
                _keepAliveTimer.Dispose();
                _keepAliveTimer = null;
            }
            var client = _client;
            using (ExecutionContext.SuppressFlow())
                Task.Run(() => WriterLoop(client));
        }

        private async void Connect(TcpClient client)
        {
            try
            {
                await client.ConnectAsync(TargetEndPoint.Address, TargetEndPoint.Port).ConfigureAwait(false);
                _logger.LogInformation("Connected to {TargetEndPoint}", TargetEndPoint);
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
                _logger.LogWarning(ex, "Outgoing connection to {TargetEndPoint} lost while in state {ClientState} with {MessageQueueLength} messages in queue.",
                    TargetEndPoint, _state, _messageQueue.Count);

                if (_state == State.Sending && _messageQueue.Count > 0)
                {
                    StartConnecting();
                }
                else
                {
                    _logger.LogInformation("Dropping messages from queue and remaining disconnected.");
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
                    await TcpTransport.SendFramed(client.GetStream(), msg).ConfigureAwait(false);
                    lock (_lock)
                    {
                        if (_messageQueue.Peek() == msg)
                            _messageQueue.Dequeue();
                        if (_client != client)
                            return;
                        if (_messageQueue.Count == 0)
                        {
                            _logger.LogDebug("Message queue for {TargetEndPoint} empty, switching to state {ClientState}.", TargetEndPoint, State.Connected);
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
                _logger.LogDebug("Sending keep-alive to {TargetEndPoint}", TargetEndPoint);
                await TcpTransport.SendFramed(client.GetStream(), new byte[0]).ConfigureAwait(false);
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
            _logger.LogInformation("Disposing client connection to {TargetEndPoint}", TargetEndPoint);
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
