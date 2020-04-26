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
        private readonly int _receiveDelayMaxMs;
        private readonly double _lossRisk;
        private readonly int _repeatLimit;
        private readonly double _repeatRisk;
        private readonly int _repeatDelayMaxMs;

        public int CountLosses, CountRepeats;

        public bool CutOff { get; set; }

        public DodgyTransport(TcpTransport wrapped, int receiveDelayMaxMs = 300, double lossRisk = 0.1, int repeatLimit = 5, double repeatRisk = 0.25, int repeatDelayMaxMs = 300)
        {
            _wrapped = wrapped;
            _receiveDelayMaxMs = receiveDelayMaxMs;
            _lossRisk = lossRisk;
            _repeatLimit = repeatLimit;
            _repeatRisk = repeatRisk;
            _repeatDelayMaxMs = repeatDelayMaxMs;
            _wrapped.MessageHandler += _wrapped_MessageHandler;
            _wrapped.Error += _wrapped_Error;
        }

        private int _randomSeed = Environment.TickCount;

        private int GetReceiveDelay()
        {
            var rnd = new Random(Interlocked.Increment(ref _randomSeed));
            return rnd.Next(_repeatDelayMaxMs);
        }

        private bool ShouldLoseMsg()
        {
            var rnd = new Random(Interlocked.Increment(ref _randomSeed));
            return rnd.NextDouble() < _lossRisk;
        }

        private int? GetRepeatDelay()
        {
            var rnd = new Random(Interlocked.Increment(ref _randomSeed));
            if (rnd.NextDouble() >= _repeatRisk)
                return null;
            return rnd.Next(_repeatDelayMaxMs);
        }

        private object _wrapped_MessageHandler(object msg)
        {
            if (CutOff)
                return null;
            var from = (msg as GossipMessage)?.From;
            Task.Run(async () =>
            {
                int receiveDelay = GetReceiveDelay();
                await Task.Delay(receiveDelay);
                int count = _repeatLimit;
                while (count-- > 0)
                {
                    if (!ShouldLoseMsg())
                    {
                        var reply = MessageHandler?.Invoke(msg);
                        if (reply != null && from != null)
                            _wrapped.Send(from, reply, false);
                    }
                    else
                        Interlocked.Increment(ref CountLosses);
                    var delay = GetRepeatDelay();
                    if (delay == null)
                        return;
                    Interlocked.Increment(ref CountRepeats);
                    await Task.Delay(delay.Value);
                }
            });
            return null;
        }

        private void _wrapped_Error(object sender, Exception e)
        {
            Error?.Invoke(this, e);
        }

        public string OwnId => _wrapped.OwnId;
        public ICollection<string> Servers => _wrapped.Servers;
        public ShieldedDict<string, IPEndPoint> ServerIPs => _wrapped.ServerIPs;

        public MessageHandler MessageHandler { get; set; }

        public event EventHandler<Exception> Error;

        public void Broadcast(object msg)
        {
            if (CutOff)
                return;
            _wrapped.Broadcast(msg);
        }

        public void Dispose()
        {
            _wrapped.Dispose();
        }

        public void Send(string server, object msg, bool replyExpected)
        {
            if (CutOff)
                return;
            _wrapped.Send(server, msg, false);
        }
    }
}
