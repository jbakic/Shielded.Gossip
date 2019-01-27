﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
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
                Assert.IsTrue(msg is GossipStart);
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
                var starter = msg as GossipStart;
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

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
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
                // basic check for the window fields.
                Assert.AreEqual(msgA1.Items[19].Freshness - 1, msgA1.WindowStart);
                Assert.AreEqual(msgA1.Items[0].Freshness, msgA1.WindowEnd);

                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB1);
                // this confirms that the B server recognizes there's no need to send the same items back to A
                Assert.AreEqual(0, msgB1.Items?.Length ?? 0);
                // B does specify a window as if he did send them! also, by checking that WindowEnd is 3, we confirm
                // that B correctly recognized 3 packages in the incoming message.
                Assert.AreEqual(3, msgB1.WindowEnd);
                Assert.AreEqual(0, msgB1.WindowStart);

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
                // the window grows. end is equal since there were no new changes in between replies.
                Assert.IsTrue(msgA2.WindowStart < msgA1.WindowStart && msgA2.WindowEnd == msgA1.WindowEnd);

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
                Assert.IsTrue(msgA3.WindowStart < msgA2.WindowStart && msgA3.WindowEnd == msgA2.WindowEnd);

                transportB.Receive(msgA3);
                var msgB3 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB3);
                Assert.AreEqual(0, msgB3.Items?.Length ?? 0);

                transportA.Receive(msgB3);
                var msgA4 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA4);
                // since we hit cut-off, he will again just send 50 items. we want to make sure that he did not
                // skip the transaction he stopped at last time.
                Assert.AreEqual(50, msgA4.Items.Length);
                for (int c = 0; c < 49; c++)
                    Assert.IsTrue(msgA4.Items[c].Freshness >= msgA4.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA4.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(84, 5).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                Assert.IsTrue(msgA4.WindowStart < msgA3.WindowStart && msgA4.WindowEnd == msgA3.WindowEnd);
            }
        }

        [TestMethod]
        public void GossipMessaging_NewChangesBetweenReplies()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 7,
                AntiEntropyCutoff = 49,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 7,
                AntiEntropyCutoff = 49,
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

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // initial size is 7, he takes the trigger and one full transaction of 10 items.
                Assert.AreEqual(11, msgA1.Items.Length);
                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB1);
                Assert.AreEqual(0, msgB1.Items?.Length ?? 0);

                // perform new changes before A receives the reply from B. we'll reuse the lower keys.
                for (int i = 0; i < 1; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = $"key-{i:00}-{j:00}";
                            backendA.SetVc(key, backendA.TryGetClocked<bool>(key).Single().NextVersion(A));
                        }
                    });
                }

                transportA.Receive(msgB1);
                var msgA2 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA2);
                // in the second round, the target size is 2*7=14. he'll take 20 - 10 new items, and 10 older ones.
                Assert.AreEqual(20, msgA2.Items.Length);
                for (int c = 0; c < 19; c++)
                    Assert.IsTrue(msgA2.Items[c].Freshness >= msgA2.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA2.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(0, 10).Select(j => $"key-98-{j:00}")
                        .Concat(Enumerable.Range(0, 10).Select(j => $"key-00-{j:00}"))));
                Assert.AreEqual((A, 2), ((Multiple<Vc<bool>>)msgA2.Items[0].Value).Single().Clock);
                // the window grows. this time, the end expands as well, due to new changes.
                Assert.IsTrue(msgA2.WindowStart < msgA1.WindowStart && msgA2.WindowEnd > msgA1.WindowEnd);

                transportB.Receive(msgA2);
                var msgB2 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB2);
                Assert.AreEqual(0, msgB2.Items?.Length ?? 0);

                // this set of new changes will be larger than the next package size (28), but will be transmitted anyway.
                for (int i = 0; i < 4; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = $"key-{i:00}-{j:00}";
                            backendA.SetVc(key, backendA.TryGetClocked<bool>(key).Single().NextVersion(A));
                        }
                    });
                }

                transportA.Receive(msgB2);
                var msgA3 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA3);
                // package size is 28, which would mean 30 items, but we get 40 - all the new changes between replies.
                Assert.AreEqual(40, msgA3.Items.Length);
                for (int c = 0; c < 39; c++)
                    Assert.IsTrue(msgA3.Items[c].Freshness >= msgA3.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA3.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(0, 4).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                // here, the start did not move, the message contained only the new changes
                Assert.IsTrue(msgA3.WindowStart == msgA2.WindowStart && msgA3.WindowEnd > msgA2.WindowEnd);

                transportB.Receive(msgA3);
                var msgB3 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB3);
                Assert.AreEqual(0, msgB3.Items?.Length ?? 0);

                // we again make new changes, but now so many that their number exceeds the cut-off. the gossip
                // window will "slip" due to this. package size plays no role here.
                for (int i = 0; i < 6; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = $"key-{i:00}-{j:00}";
                            backendA.SetVc(key, backendA.TryGetClocked<bool>(key).Single().NextVersion(A));
                        }
                    });
                }

                transportA.Receive(msgB3);
                var msgA4 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA4);
                Assert.AreEqual(40, msgA4.Items.Length);
                for (int c = 0; c < 39; c++)
                    Assert.IsTrue(msgA4.Items[c].Freshness >= msgA4.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA4.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        // only the most recent changes that managed to fit in before hitting the cut-off...
                        Enumerable.Range(2, 4).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                // the window slipped!
                Assert.IsTrue(msgA4.WindowStart > msgA3.WindowEnd);

                transportB.Receive(msgA4);
                var msgB4 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB4);
                Assert.AreEqual(0, msgB4.Items?.Length ?? 0);

                // now, without any new changes, let's confirm it just continues. since the window slipped,
                // it will now resend the items it sent once before already.
                transportA.Receive(msgB4);
                var msgA5 = transportA.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgA5);
                // cut-off limits us again, but we have 41 now, because the "trigger" item is here again.
                Assert.AreEqual(41, msgA5.Items.Length);
                for (int c = 0; c < 40; c++)
                    Assert.IsTrue(msgA5.Items[c].Freshness >= msgA5.Items[c + 1].Freshness);
                Assert.IsTrue(
                    msgA5.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    .SequenceEqual(
                        Enumerable.Range(98, 2).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))
                        .Concat(new[] { "trigger" })
                        .Concat(Enumerable.Range(0, 2).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}")))));
                // the window now grows again, as usual.
                Assert.IsTrue(msgA5.WindowStart < msgA4.WindowStart && msgA5.WindowEnd == msgA4.WindowEnd);
            }
        }

        [TestMethod]
        public void GossipMessaging_NeedlessReplyItemsCheck()
        {
            // when constructing a reply, a backend tries to not send back the same items it just received,
            // even though to it these items were news. but if the items did have changes on that server,
            // then he must send them. this test checks the two ways a change could have happened - an
            // independent transaction on B, or a Changed handler which additionally changes a key
            // while we're processing the incoming message.

            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 21,
                AntiEntropyCutoff = 49,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 21,
                AntiEntropyCutoff = 49,
            }))
            {
                for (int i = 0; i < 2; i++)
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

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // the full package
                Assert.AreEqual(21, msgA1.Items.Length);

                // before B receives the message, we change some keys on it
                backendB.SetVc("key-01-05", false.Clock(B));
                // this one we make equal to the value on A. this means B does have a change on it, but
                // the value is identical to the one received, and he will not transmit it in the reply.
                backendB.SetVc("key-01-06", true.Clock(A));
                // this one we have a higher version. it would maybe make more sense if it was coming from
                // some third server, but this is just a unit test, it's fine.
                backendB.SetVc("key-01-07", false.Clock(A, 2));
                // and we create a subscription that will change stuff when the A message comes in
                Shield.InTransaction(() =>
                    backendB.Changed.Subscribe((sender, changed) =>
                    {
                        // the value clock check means we do this only once, otherwise, we'll get a stack overflow.
                        if (changed.Key == "key-00-06" && ((Multiple<Vc<bool>>)changed.NewValue).MergedClock[B] == 0)
                        {
                            // we'll change it and one unrelated key, just to be more evil.
                            backendB.SetVc("key-00-06", ((Multiple<Vc<bool>>)changed.NewValue).Single().NextVersion(B));
                            backendB.SetVc("key-00-04", backendB.TryGetClocked<bool>("key-00-04").SingleOrDefault().NextVersion(B));
                        }
                    }));

                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg as GossipReply;
                Assert.IsNotNull(msgB1);
                Assert.AreEqual(4, msgB1.Items?.Length ?? 0);
                Assert.IsTrue(
                    msgB1.Items
                    .OrderBy(i => i.Freshness).ThenBy(i => i.Key)
                    .Select(i => i.Key)
                    // key-01-07 comes first - the edit on B happened before the incoming msg, and the msg did not
                    // cause any change on it, so its freshness is less than the others.
                    .SequenceEqual(new[] { "key-01-07", "key-00-04", "key-00-06", "key-01-05" }));
                Assert.IsTrue(
                    msgB1.Items
                    .Select(i => (i.Key, ((Multiple<Vc<bool>>)i.Value).MergedClock))
                    .All(kc =>
                         kc.Key != "key-01-07" && kc.MergedClock == ((VectorClock)(A, 1) | (B, 1)) ||
                         kc.Key == "key-01-07" && kc.MergedClock == (A, 2)));
            }
        }

        [TestMethod]
        public void GossipMessaging_SimultaneousGossipStart()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string> { A });
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
            }))
            {
                backendA.SetVc("trigger", true.Clock(A));
                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);

                backendB.SetVc("trigger", true.Clock(B));
                var msgB1 = transportB.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgB1);

                transportA.Receive(msgB1);
                var msgA2 = transportA.LastSentMessage.Msg;
                transportB.Receive(msgA1);
                var msgB2 = transportB.LastSentMessage.Msg;
                // only B will answer, because he comes later in the alphabet. since that's fully arbitrary, we
                // only check that just one of them answered, does not matter which one.
                Assert.IsTrue(
                    msgA2 == msgA1 && msgB2 != msgB1 && msgB2 is GossipReply ||
                    msgA2 != msgA1 && msgB2 == msgB1 && msgA2 is GossipReply);

                // if we resend the start messages again, neither should react anymore!
                transportA.Receive(msgB1);
                var msgA3 = transportA.LastSentMessage.Msg;
                transportB.Receive(msgA1);
                var msgB3 = transportB.LastSentMessage.Msg;
                Assert.IsTrue(msgA3 == msgA2 && msgB3 == msgB2);
            }
        }

        [TestMethod]
        public void GossipMessaging_UnexpectedMessage()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 7,
                AntiEntropyCutoff = 49,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 7,
                AntiEntropyCutoff = 49,
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

                // let's get them warmed up
                var msgA1 = transportA.LastSentMessage.Msg;
                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg;
                transportA.Receive(msgB1);
                var msgA2 = transportA.LastSentMessage.Msg;
                Assert.AreNotEqual(msgA1, msgA2);
                transportB.Receive(msgA2);
                var msgB2 = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB1, msgB2);

                // now let's see if B will correctly ignore already processed messages
                transportB.Receive(msgA2);
                Assert.AreEqual(msgB2, transportB.LastSentMessage.Msg);
                transportB.Receive(msgA1);
                Assert.AreEqual(msgB2, transportB.LastSentMessage.Msg);

                // the old messages should not have screwed up his state - he should reply to the correct message
                transportA.Receive(msgB2);
                var msgA3 = transportA.LastSentMessage.Msg;
                Assert.AreNotEqual(msgA2, msgA3);
                transportB.Receive(msgA3);
                var msgB3 = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB2, msgB3);
            }
        }

        [TestMethod]
        public void GossipMessaging_ReplyToEarlyEndConflict()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string> { A });
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
            }))
            {
                backendA.SetVc("trigger", true.Clock(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg;
                Assert.IsInstanceOfType(msgB1, typeof(GossipEnd));

                // both will now get new changes.
                backendA.SetVc("additionalA", true.Clock(A));
                // no new message, since A is in gossip with B already.
                Assert.AreEqual(msgA1, transportA.LastSentMessage.Msg);

                backendB.SetVc("additionalB", true.Clock(B));
                // yes new message, because B sent a GossipEnd and he thinks the gossip is done.
                var msgBX = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB1, msgBX);
                Assert.IsInstanceOfType(msgBX, typeof(GossipStart));

                // A will accept his message, because B included in his GossipStart the same LastTime he sent
                // in his GossipEnd. this is meant for this case exactly. A can then recognize that he did
                // not (yet) receive the GossipEnd message, and will just accept the new chain.
                transportA.Receive(msgBX);
                var msgAX = transportA.LastSentMessage.Msg;
                Assert.AreNotEqual(msgA1, msgAX);
                Assert.IsInstanceOfType(msgAX, typeof(GossipReply));

                // the delayed end msg will now be rejected by A.
                transportA.Receive(msgB1);
                Assert.AreEqual(msgAX, transportA.LastSentMessage.Msg);

                // from this point on, all is well - they both accept the new chain only.
                transportB.Receive(msgAX);
                var msgBX2 = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgBX, msgBX2);
                Assert.IsInstanceOfType(msgBX2, typeof(GossipEnd));

                transportA.Receive(msgBX2);
                Assert.AreEqual(msgAX, transportA.LastSentMessage.Msg);
            }
        }

        [TestMethod]
        public void GossipMessaging_ReplyToLateEndConflict()
        {
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string> { A });
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                // temporarily...
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 10,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 10,
            }))
            {
                // just 2, enough to cause a minimal reply chain and then an end msg.
                for (int i = 0; i < 2; i++)
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
                backendA.SetVc("trigger", true.Clock(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                transportB.Receive(msgA1);
                var msgB1 = transportB.LastSentMessage.Msg;
                Assert.IsInstanceOfType(msgB1, typeof(GossipReply));

                transportA.Receive(msgB1);
                var msgA2 = transportA.LastSentMessage.Msg;
                Assert.AreNotEqual(msgA1, msgA2);
                Assert.IsInstanceOfType(msgA2, typeof(GossipReply));
                transportB.Receive(msgA2);
                var msgB2 = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB1, msgB2);
                Assert.IsInstanceOfType(msgB2, typeof(GossipEnd));

                // so, they are in sync, but before A receives the end msg, both will get new changes.
                backendA.SetVc("additionalA", true.Clock(A));
                // no new message, since A is in gossip with B already.
                Assert.AreEqual(msgA2, transportA.LastSentMessage.Msg);

                backendB.SetVc("additionalB", true.Clock(B));
                // yes new message, because B sent a GossipEnd and he thinks the gossip is done.
                var msgBX = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB2, msgBX);
                Assert.IsInstanceOfType(msgBX, typeof(GossipStart));

                // behavior here the same as above, A can recognize that he missed a GossipEnd, and
                // behaves as if he had received it. in a way, a GossipStart that has LastTime doubles as
                // a GossipEnd message too.
                transportA.Receive(msgBX);
                var msgAX = transportA.LastSentMessage.Msg;
                Assert.AreNotEqual(msgA2, msgAX);
                Assert.IsInstanceOfType(msgAX, typeof(GossipReply));

                // the delayed end msg will now be rejected by A.
                transportA.Receive(msgB2);
                Assert.AreEqual(msgAX, transportA.LastSentMessage.Msg);

                // from this point on, all is well - they both accept the new chain only.
                transportB.Receive(msgAX);
                var msgBX2 = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgBX, msgBX2);
                Assert.IsInstanceOfType(msgBX2, typeof(GossipEnd));

                transportA.Receive(msgBX2);
                Assert.AreEqual(msgAX, transportA.LastSentMessage.Msg);
            }
        }
    }
}
