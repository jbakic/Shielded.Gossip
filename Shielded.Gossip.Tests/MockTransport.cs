using System;
using System.Collections.Generic;
using System.Linq;

namespace Shielded.Gossip.Tests
{
    public class MockTransport : ITransport
    {
        public MockTransport(string ownId, ICollection<string> servers)
        {
            OwnId = ownId;
            Servers = servers;
        }

        public string OwnId { get; }
        public ICollection<string> Servers { get; }

        public MessageHandler MessageHandler { get; set; }

        public void Receive(object msg)
        {
            var from = (msg as GossipMessage)?.From;
            var reply = MessageHandler(msg);
            if (from != null && reply != null)
                Send(from, reply, false);
        }

        public object ReceiveAndGetReply(object msg)
        {
            var prevCount = _sentMessages.Count;
            Receive(msg);
            if (_sentMessages.Count == prevCount)
                return null;
            if (_sentMessages.Count > prevCount + 1)
                throw new ApplicationException("More than one reply generated.");
            return LastSentMessage.Msg;
        }

        private object _lock = new object();
        private List<(string To, object Msg)> _sentMessages = new List<(string, object)>();

        public List<(string To, object Msg)> SentMessages
        {
            get
            {
                lock (_lock)
                    return _sentMessages.ToList();
            }
        }

        public (string To, object Msg) LastSentMessage
        {
            get
            {
                lock (_lock)
                    return _sentMessages.Count == 0
                        ? throw new InvalidOperationException("No messages sent.")
                        : _sentMessages[_sentMessages.Count - 1];
            }
        }

        public void Broadcast(object msg)
        {
            lock (_lock)
                _sentMessages.AddRange(Servers.Select(s => (s, msg)));
        }

        public void Dispose() { }

        public void Send(string server, object msg, bool replyExpected)
        {
            lock (_lock)
                _sentMessages.Add((server, msg));
        }
    }
}
