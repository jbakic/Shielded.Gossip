using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class GossipMessagingTests
    {
        private const string A = "A";
        private const string B = "B";

        [TestMethod]
        public void GossipMessaging_StartGossip()
        {
            var transport = new MockTransport(A, new List<string> { B });
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = 100,
            }))
            {
                Thread.Sleep(150);

                var (to, msg) = transport.LastSentMessage;
                Assert.AreEqual(B, to);
                Assert.IsTrue(msg is NewGossip);
            }
        }

        [TestMethod]
        public void GossipMessaging_ReceiveStartGossipAndReplyEnd()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
            }))
            {
                backendA.SetVc("key", (25.5m).Clock(A));
                var (to, msg) = transportA.LastSentMessage;
                Assert.AreEqual(B, to);
                var starter = msg as NewGossip;
                Assert.IsNotNull(starter);

                transportB.Receive(msg);
                var (replyTo, replyMsg) = transportB.LastSentMessage;
                Assert.AreEqual(A, replyTo);
                var endMsg = replyMsg as GossipEnd;
                Assert.IsNotNull(endMsg);
                Assert.IsTrue(endMsg.Success);
                Assert.AreEqual(starter.DatabaseHash, endMsg.DatabaseHash);
            }
        }

        [TestMethod]
        public void GossipMessaging_GossipPackageStartAndGrowth()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 17,
                AntiEntropyCutoff = 59,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 17,
                AntiEntropyCutoff = 59,
            }))
            {
                for (int i = 0; i < 100; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendA.SetVc($"key-{i:00}-{j:00}", true.Clock(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.Set("trigger", "bla".Lww());

                var msgA1 = transportA.LastSentMessage.Msg as NewGossip;
                Assert.IsNotNull(msgA1);
                // we breach the initial size of 17 while going through the third transaction, and since initial size
                // is not strict, he takes the third transaction too, resulting in 21 items (trigger + 2x10...)
                Assert.AreEqual(21, msgA1.Items.Length);
                for (int c = 0; c < 20; c++)
                    Assert.IsTrue(msgA1.Items[c].Freshness >= msgA1.Items[c + 1].Freshness);
                Assert.AreEqual("trigger", msgA1.Items[0].Key);
                Assert.IsTrue(
                    msgA1.Items.Skip(1)
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(98, 2).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));

                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB1);
                Assert.AreEqual(0, msgB1.Items?.Length ?? 0);

                transportA.Receive(msgB1);
                var msgA2 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA2);
                // in the second round, the target size is 2*17=34, so he takes 40.
                Assert.AreEqual(40, msgA2.Items.Length);
                for (int c = 0; c < 39; c++)
                    Assert.IsTrue(msgA2.Items[c].Freshness >= msgA2.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA2.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(94, 4).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));

                transportB.Receive(msgA2);
                var msgB2 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB2);
                Assert.AreEqual(0, msgB2.Items?.Length ?? 0);

                transportA.Receive(msgB2);
                var msgA3 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA3);
                // in the third round, the target size is 2*34=68, which exceeds cut-off of 59. cut-off is strict, so
                // the message will contain only 50 items - 5 whole transactions.
                Assert.AreEqual(50, msgA3.Items.Length);
                for (int c = 0; c < 49; c++)
                    Assert.IsTrue(msgA3.Items[c].Freshness >= msgA3.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA3.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(89, 5).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
            }
        }
    }
}
