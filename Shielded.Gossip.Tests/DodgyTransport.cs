using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Shielded.Gossip.Tests
{
    public class DodgyTransport : ITransport
    {
        private readonly TcpTransport _wrapped;
        private readonly double _risk;

        public DodgyTransport(TcpTransport wrapped, double risk = 0.1)
        {
            _wrapped = wrapped;
            _risk = risk;
            _wrapped.MessageReceived += _wrapped_MessageReceived;
            _wrapped.Error += _wrapped_Error;
        }

        private bool ThrowDice()
        {
            var rnd = new Random();
            return rnd.NextDouble() < _risk;
        }

        private void _wrapped_MessageReceived(object sender, object msg)
        {
            if (ThrowDice())
                return;
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
