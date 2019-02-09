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
            var from = (msg as GossipMessage).From;
            var reply = MessageHandler(msg).Result;
            if (from != null && reply != null)
                Send(from, reply, false);
        }

        public object ReceiveAndGetReply(object msg)
        {
            var prevCount = SentMessages.Count;
            Receive(msg);
            if (SentMessages.Count == prevCount)
                return null;
            if (SentMessages.Count > prevCount + 1)
                throw new ApplicationException("More than one reply generated.");
            return LastSentMessage.Msg;
        }

        public List<(string To, object Msg)> SentMessages = new List<(string, object)>();

        public (string To, object Msg) LastSentMessage => SentMessages.Count == 0
            ? throw new InvalidOperationException("No messages sent.")
            : SentMessages[SentMessages.Count - 1];

        public void Broadcast(object msg)
        {
            SentMessages.AddRange(Servers.Select(s => (s, msg)));
        }

        public void Dispose() { }

        public void Send(string server, object msg, bool replyExpected)
        {
            SentMessages.Add((server, msg));
        }
    }
}
