using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    public class DodgyTransport : ITransport
    {
        private readonly TcpTransport _wrapped;
        private readonly double _lossRisk;
        private readonly int _repeatLimit;
        private readonly double _repeatRisk;
        private readonly int _repeatDelayMaxMs;

        public int CountLosses, CountRepeats;

        public DodgyTransport(TcpTransport wrapped, double lossRisk = 0.1, int repeatLimit = 5, double repeatRisk = 0.25, int repeatDelayMaxMs = 3000)
        {
            _wrapped = wrapped;
            _lossRisk = lossRisk;
            _repeatLimit = repeatLimit;
            _repeatRisk = repeatRisk;
            _repeatDelayMaxMs = repeatDelayMaxMs;
            _wrapped.MessageReceived += _wrapped_MessageReceived;
            _wrapped.Error += _wrapped_Error;
        }

        private bool ShouldLoseMsg()
        {
            var rnd = new Random();
            return rnd.NextDouble() < _lossRisk;
        }

        private int? GetMsgDelay()
        {
            var rnd = new Random();
            if (rnd.NextDouble() >= _repeatRisk)
                return null;
            return rnd.Next(_repeatDelayMaxMs);
        }

        private async void _wrapped_MessageReceived(object sender, object msg)
        {
            int count = _repeatLimit;
            while (count --> 0)
            {
                if (!ShouldLoseMsg())
                    MessageReceived?.Invoke(this, msg);
                else
                    Interlocked.Increment(ref CountLosses);
                var delay = GetMsgDelay();
                if (delay == null)
                    return;
                Interlocked.Increment(ref CountRepeats);
                await Task.Delay(delay.Value);
            }
        }

        private void _wrapped_Error(object sender, Exception e)
        {
            Error?.Invoke(this, e);
        }

        public string OwnId => _wrapped.OwnId;
        public ICollection<string> Servers => _wrapped.Servers;
        public IDictionary<string, IPEndPoint> ServerIPs => _wrapped.ServerIPs;

        public event EventHandler<object> MessageReceived;
        public event EventHandler<Exception> Error;

        public void Broadcast(object msg)
        {
            _wrapped.Broadcast(msg);
        }

        public void Dispose()
        {
            _wrapped.Dispose();
        }

        public void Send(string server, object msg)
        {
            _wrapped.Send(server, msg);
        }
    }
}
