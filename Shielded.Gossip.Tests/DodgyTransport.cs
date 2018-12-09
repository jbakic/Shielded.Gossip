using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    public class DodgyTransport : ITransport
    {
        private readonly TcpTransport _wrapped;
        private readonly double _lossRisk;
        private readonly double _delayRisk;
        private readonly int _delayMaxMs;

        public DodgyTransport(TcpTransport wrapped, double lossRisk = 0.1, double delayRisk = 0.1, int delayMaxMs = 5000)
        {
            _wrapped = wrapped;
            _lossRisk = lossRisk;
            _delayRisk = delayRisk;
            _delayMaxMs = delayMaxMs;
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
            if (rnd.NextDouble() >= _delayRisk)
                return null;
            return rnd.Next(_delayMaxMs);
        }

        private async void _wrapped_MessageReceived(object sender, object msg)
        {
            if (ShouldLoseMsg())
                return;
            var delay = GetMsgDelay();
            if (delay != null)
                await Task.Delay(delay.Value);
            MessageReceived?.Invoke(this, msg);
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
