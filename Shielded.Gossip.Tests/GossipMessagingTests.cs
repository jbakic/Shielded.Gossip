using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Gossip.Backend;
using Shielded.Gossip.Mergeables;
using System;
using System.Collections.Generic;
using System.Linq;
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
                backendA.SetHasVec("key", (25.5m).Version(A));
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
                AntiEntropyItemsCutoff = 59,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 17,
                AntiEntropyItemsCutoff = 59,
            }))
            {
                for (int i = 0; i < 100; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendA.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.Set("trigger", "bla".Lww());

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // we breach the initial size of 17 while going through the third transaction, and since initial size
                // is not strict, he takes the third transaction too, resulting in 21 items (trigger + 2x10...)
                Assert.IsTrue(msgA1.Transactions.Select(t => t.Changes.Length).SequenceEqual(new [] { 10, 10, 1 }));
                Assert.AreEqual("trigger", msgA1.Transactions[2].Changes[0].Key);
                Assert.IsTrue(
                    msgA1.Transactions.Take(2).SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        Enumerable.Range(98, 2).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                // basic check for the window fields. we know no other transactions were running, so we can demand exactly 3.
                Assert.AreEqual(3, msgA1.WindowEnd - msgA1.WindowStart);

                var msgB1 = transportB.ReceiveAndGetReply(msgA1) as GossipReply;
                Assert.IsNotNull(msgB1);
                // this confirms that the B server recognizes there's no need to send the same items back to A
                Assert.AreEqual(0, msgB1.Transactions?.Length ?? 0);
                // B does specify a window as if he did send them! also, by checking that WindowEnd is 3, we confirm
                // that B correctly recognized 3 packages in the incoming message.
                Assert.AreEqual(3, msgB1.WindowEnd);
                Assert.AreEqual(0, msgB1.WindowStart);

                var msgA2 = transportA.ReceiveAndGetReply(msgB1) as GossipReply;
                Assert.IsNotNull(msgA2);
                // in the second round, the target size is 2*17=34, so he takes 4*10
                Assert.IsTrue(msgA2.Transactions.Select(t => t.Changes.Length).SequenceEqual(Enumerable.Repeat(10, 4)));
                Assert.IsTrue(
                    msgA2.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        Enumerable.Range(94, 4).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                // the window grows. end is equal since there were no new changes in between replies.
                Assert.IsTrue(msgA2.WindowStart < msgA1.WindowStart && msgA2.WindowEnd == msgA1.WindowEnd);

                var msgB2 = transportB.ReceiveAndGetReply(msgA2) as GossipReply;
                Assert.IsNotNull(msgB2);
                Assert.AreEqual(0, msgB2.Transactions?.Length ?? 0);

                var msgA3 = transportA.ReceiveAndGetReply(msgB2) as GossipReply;
                Assert.IsNotNull(msgA3);
                // in the third round, the target size is 2*34=68, which exceeds cut-off of 59. cut-off is strict, so
                // the message will contain only 50 items - 5 whole transactions.
                Assert.IsTrue(msgA3.Transactions.Select(t => t.Changes.Length).SequenceEqual(Enumerable.Repeat(10, 5)));
                Assert.IsTrue(
                    msgA3.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        Enumerable.Range(89, 5).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                Assert.IsTrue(msgA3.WindowStart < msgA2.WindowStart && msgA3.WindowEnd == msgA2.WindowEnd);

                var msgB3 = transportB.ReceiveAndGetReply(msgA3) as GossipReply;
                Assert.IsNotNull(msgB3);
                Assert.AreEqual(0, msgB3.Transactions?.Length ?? 0);

                var msgA4 = transportA.ReceiveAndGetReply(msgB3) as GossipReply;
                Assert.IsNotNull(msgA4);
                // since we hit cut-off, he will again just send 50 items. we want to make sure that he did not
                // skip the transaction he stopped at last time.
                Assert.IsTrue(msgA4.Transactions.Select(t => t.Changes.Length).SequenceEqual(Enumerable.Repeat(10, 5)));
                Assert.IsTrue(
                    msgA4.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        Enumerable.Range(84, 5).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                Assert.IsTrue(msgA4.WindowStart < msgA3.WindowStart && msgA4.WindowEnd == msgA3.WindowEnd);
            }
        }

        [TestMethod]
        public void GossipMessaging_BytesCutoff()
        {
            var someBytes = Enumerable.Repeat((byte)101, 10000).ToArray();
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyBytesCutoff = 30000,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
            }))
            {
                backendA.SetHasVec("key1", someBytes.Version(A, 1));
                backendA.SetHasVec("key2", someBytes.Version(A, 1));
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.SetHasVec("key3", someBytes.Version(A, 1));

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // 2 of the keys, the third one cannot fit in the bytes cut-off.
                Assert.AreEqual(2, msgA1.Transactions.Length);
            }
        }

        [TestMethod]
        public void GossipMessaging_BytesCutoffEmptyMsg()
        {
            var someBytes = Enumerable.Repeat((byte)101, 10000).ToArray();
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string>());
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyBytesCutoff = 1000,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
            }))
            {
                backendA.SetHasVec("key1", someBytes.Version(A, 1));
                backendA.SetHasVec("key2", someBytes.Version(A, 1));
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.SetHasVec("key3", someBytes.Version(A, 1));

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // one key will pass, since we must send something...
                Assert.AreEqual(1, msgA1.Transactions.Length);
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
                AntiEntropyItemsCutoff = 49,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 7,
                AntiEntropyItemsCutoff = 49,
            }))
            {
                for (int i = 0; i < 100; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendA.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.Set("trigger", "bla".Lww());

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // initial size is 7, he takes the trigger and one full transaction of 10 items.
                Assert.IsTrue(msgA1.Transactions.Select(t => t.Changes.Length).SequenceEqual(new [] { 10, 1 }));
                var msgB1 = transportB.ReceiveAndGetReply(msgA1) as GossipReply;
                Assert.IsNotNull(msgB1);
                Assert.AreEqual(0, msgB1.Transactions?.Length ?? 0);

                // perform new changes before A receives the reply from B. we'll reuse the lower keys.
                for (int i = 0; i < 1; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = $"key-{i:00}-{j:00}";
                            backendA.SetHasVec(key, backendA.TryGetVecVersioned<bool>(key).Single().NextVersion(A));
                        }
                    });
                }

                var msgA2 = transportA.ReceiveAndGetReply(msgB1) as GossipReply;
                Assert.IsNotNull(msgA2);
                // in the second round, the target size is 2*7=14. he'll take 20 - 10 new items, and 10 older ones.
                Assert.IsTrue(msgA2.Transactions.Select(t => t.Changes.Length).SequenceEqual(Enumerable.Repeat(10, 2)));
                Assert.IsTrue(
                    msgA2.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        Enumerable.Range(0, 10).Select(j => $"key-98-{j:00}")
                        .Concat(Enumerable.Range(0, 10).Select(j => $"key-00-{j:00}"))));
                Assert.AreEqual((A, 2), ((Multiple<VecVersioned<bool>>)msgA2.Transactions[1].Changes[0].Value).Single().Version);
                // the window grows. this time, the end expands as well, due to new changes.
                Assert.IsTrue(msgA2.WindowStart < msgA1.WindowStart && msgA2.WindowEnd > msgA1.WindowEnd);

                var msgB2 = transportB.ReceiveAndGetReply(msgA2) as GossipReply;
                Assert.IsNotNull(msgB2);
                Assert.AreEqual(0, msgB2.Transactions?.Length ?? 0);

                // this set of new changes will be larger than the next package size (28), but will be transmitted anyway.
                for (int i = 0; i < 4; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = $"key-{i:00}-{j:00}";
                            backendA.SetHasVec(key, backendA.TryGetVecVersioned<bool>(key).Single().NextVersion(A));
                        }
                    });
                }

                var msgA3 = transportA.ReceiveAndGetReply(msgB2) as GossipReply;
                Assert.IsNotNull(msgA3);
                // package size is 28, which would mean 30 items, but we get 40 - all the new changes between replies.
                Assert.IsTrue(msgA3.Transactions.Select(t => t.Changes.Length).SequenceEqual(Enumerable.Repeat(10, 4)));
                Assert.IsTrue(
                    msgA3.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        Enumerable.Range(0, 4).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                // here, the start did not move, the message contained only the new changes
                Assert.IsTrue(msgA3.WindowStart == msgA2.WindowStart && msgA3.WindowEnd > msgA2.WindowEnd);

                var msgB3 = transportB.ReceiveAndGetReply(msgA3) as GossipReply;
                Assert.IsNotNull(msgB3);
                Assert.AreEqual(0, msgB3.Transactions?.Length ?? 0);

                // we again make new changes, but now so many that their number exceeds the cut-off. the gossip
                // window will "slip" due to this. package size plays no role here.
                for (int i = 0; i < 6; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            var key = $"key-{i:00}-{j:00}";
                            backendA.SetHasVec(key, backendA.TryGetVecVersioned<bool>(key).Single().NextVersion(A));
                        }
                    });
                }

                var msgA4 = transportA.ReceiveAndGetReply(msgB3) as GossipReply;
                Assert.IsNotNull(msgA4);
                Assert.IsTrue(msgA4.Transactions.Select(t => t.Changes.Length).SequenceEqual(Enumerable.Repeat(10, 4)));
                Assert.IsTrue(
                    msgA4.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    .SequenceEqual(
                        // only the most recent changes that managed to fit in before hitting the cut-off...
                        Enumerable.Range(2, 4).SelectMany(i =>
                            Enumerable.Range(0, 10).Select(j =>
                                $"key-{i:00}-{j:00}"))));
                // the window slipped!
                Assert.IsTrue(msgA4.WindowStart > msgA3.WindowEnd);

                var msgB4 = transportB.ReceiveAndGetReply(msgA4) as GossipReply;
                Assert.IsNotNull(msgB4);
                Assert.AreEqual(0, msgB4.Transactions?.Length ?? 0);

                // now, without any new changes, let's confirm it just continues. since the window slipped,
                // it will now resend the items it sent once before already.
                var msgA5 = transportA.ReceiveAndGetReply(msgB4) as GossipReply;
                Assert.IsNotNull(msgA5);
                // cut-off limits us again, but we have 41 now, because the "trigger" item is here again.
                Assert.IsTrue(msgA5.Transactions.Select(t => t.Changes.Length).SequenceEqual(new [] { 10, 10, 1, 10, 10 }));
                Assert.IsTrue(
                    msgA5.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
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
                AntiEntropyItemsCutoff = 49,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 21,
                AntiEntropyItemsCutoff = 49,
            }))
            {
                for (int i = 0; i < 2; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendA.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.Set("trigger", "bla".Lww());

                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                // the full package
                Assert.IsTrue(msgA1.Transactions.Select(t => t.Changes.Length).SequenceEqual(new [] { 10, 10, 1 }));

                // before B receives the message, we change some keys on it
                backendB.SetHasVec("key-01-05", false.Version(B));
                // this one we make equal to the value on A. this means B does have a change on it, but
                // the value is identical to the one received, and he will not transmit it in the reply.
                backendB.SetHasVec("key-01-06", true.Version(A));
                // this one we have a higher version. it would maybe make more sense if it was coming from
                // some third server, but this is just a unit test, it's fine.
                backendB.SetHasVec("key-01-07", false.Version(A, 2));
                // and we create a subscription that will change stuff when the A message comes in
                Shield.InTransaction(() =>
                    backendB.Changed.Subscribe((sender, changed) =>
                    {
                        // the value Version check means we do this only once, otherwise, we'll get a stack overflow.
                        if (changed.Key == "key-00-06" && ((Multiple<VecVersioned<bool>>)changed.NewValue).MergedClock[B] == 0)
                        {
                            // we'll change it and one unrelated key, just to be more evil.
                            backendB.SetHasVec("key-00-06", ((Multiple<VecVersioned<bool>>)changed.NewValue).Single().NextVersion(B));
                            backendB.SetHasVec("key-00-04", backendB.TryGetVecVersioned<bool>("key-00-04").SingleOrDefault().NextVersion(B));
                        }
                    }));

                var msgB1 = transportB.ReceiveAndGetReply(msgA1) as GossipReply;
                Assert.IsNotNull(msgB1);
                // why 1,2,1? see below, the assertion checking the key order, should be clear then.
                Assert.IsTrue(msgB1.Transactions.Select(t => t.Changes.Length).SequenceEqual(new [] { 1, 2, 1 }));
                Assert.IsTrue(
                    msgB1.Transactions.SelectMany(t => t.Changes.Select(mi => mi.Key).OrderBy(x => x))
                    // key-01-07 comes first - the edit on B happened before the incoming msg, and the msg did not
                    // cause any change on it, so its freshness is less than the others.
                    .SequenceEqual(new[] { "key-01-07", "key-00-04", "key-00-06", "key-01-05" }));
                Assert.IsTrue(
                    msgB1.Transactions.SelectMany(t => t.Changes.Select(mi => (mi.Key, ((Multiple<VecVersioned<bool>>)mi.Value).MergedClock)))
                    .All(kc =>
                         kc.Key != "key-01-07" && kc.MergedClock == ((VersionVector)(A, 1) | (B, 1)) ||
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
                backendA.SetHasVec("trigger", true.Version(A));
                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);

                backendB.SetHasVec("trigger", true.Version(B));
                var msgB1 = transportB.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgB1);

                var msgA2 = transportA.ReceiveAndGetReply(msgB1);
                var msgB2 = transportB.ReceiveAndGetReply(msgA1);
                // only B will answer, because he comes later in the alphabet. since that's fully arbitrary, we
                // only check that just one of them answered, does not matter which one.
                Assert.IsTrue(
                    msgA2 == null && msgB2 is GossipReply ||
                    msgB2 == null && msgA2 is GossipReply);

                // if we resend the start messages again, neither should react anymore!
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB1));
                Assert.IsNull(transportB.ReceiveAndGetReply(msgA1));
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
                AntiEntropyItemsCutoff = 49,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyInitialSize = 7,
                AntiEntropyItemsCutoff = 49,
            }))
            {
                for (int i = 0; i < 100; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendA.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.Set("trigger", "bla".Lww());

                // let's get them warmed up
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                var msgA2 = transportA.ReceiveAndGetReply(msgB1);
                Assert.IsNotNull(msgA2);
                var msgB2 = transportB.ReceiveAndGetReply(msgA2);
                Assert.IsNotNull(msgB2);

                // now let's see if B will correctly ignore already processed messages
                var killB1 = transportB.ReceiveAndGetReply(msgA1);
                // kills are sent back only for GossipReply messages, and msgA1 was a starter
                Assert.IsNull(killB1);
                var killB2 = transportB.ReceiveAndGetReply(msgA2);
                // this one does not cause KillGossip, because msgA2 is the last message from A that we replied to, and if
                // that KillGossip would reach A before our reply did, it would kill a perfectly good chain.
                Assert.IsNull(killB2);

                // and now, we should be able to continue despite the above disturbances
                var msgA3 = transportA.ReceiveAndGetReply(msgB2);
                Assert.IsNotNull(msgA3);
                var msgB3 = transportB.ReceiveAndGetReply(msgA3);
                Assert.IsNotNull(msgB3);
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
                backendA.SetHasVec("trigger", true.Version(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgB1, typeof(GossipEnd));

                // both will now get new changes.
                backendA.SetHasVec("additionalA", true.Version(A));
                // no new message, since A is in gossip with B already.
                Assert.AreEqual(msgA1, transportA.LastSentMessage.Msg);

                backendB.SetHasVec("additionalB", true.Version(B));
                // yes new message, because B sent a GossipEnd and he thinks the gossip is done.
                var msgBX = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB1, msgBX);
                Assert.IsInstanceOfType(msgBX, typeof(GossipStart));

                // A will accept his message, because B included in his GossipStart the ReplyToId he sent
                // in his GossipEnd. this is meant for this case exactly. A can then recognize that he did
                // not (yet) receive the GossipEnd message, and will just accept the new chain.
                var msgAX = transportA.ReceiveAndGetReply(msgBX);
                Assert.IsInstanceOfType(msgAX, typeof(GossipReply));

                // the delayed end msg will now be rejected by A, and end msgs cause no kill reply.
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB1));

                // from this point on, all is well - they both accept the new chain only.
                var msgBX2 = transportB.ReceiveAndGetReply(msgAX);
                Assert.IsInstanceOfType(msgBX2, typeof(GossipEnd));

                Assert.IsNull(transportA.ReceiveAndGetReply(msgBX2));
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
                            backendA.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.SetHasVec("trigger", true.Version(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgB1, typeof(GossipReply));

                var msgA2 = transportA.ReceiveAndGetReply(msgB1);
                Assert.IsInstanceOfType(msgA2, typeof(GossipReply));
                var msgB2 = transportB.ReceiveAndGetReply(msgA2);
                Assert.IsInstanceOfType(msgB2, typeof(GossipEnd));

                // so, they are in sync, but before A receives the end msg, both will get new changes.
                backendA.SetHasVec("additionalA", true.Version(A));
                // no new message, since A is in gossip with B already.
                Assert.AreEqual(msgA2, transportA.LastSentMessage.Msg);

                backendB.SetHasVec("additionalB", true.Version(B));
                // yes new message, because B sent a GossipEnd and he thinks the gossip is done.
                var msgBX = transportB.LastSentMessage.Msg;
                Assert.AreNotEqual(msgB2, msgBX);
                Assert.IsInstanceOfType(msgBX, typeof(GossipStart));

                // behavior here the same as above, A can recognize that he missed a GossipEnd, and
                // behaves as if he had received it. in a way, a GossipStart that has ReplyToId doubles as
                // a GossipEnd message too.
                var msgAX = transportA.ReceiveAndGetReply(msgBX);
                Assert.IsInstanceOfType(msgAX, typeof(GossipReply));

                // the delayed end msg will now be rejected by A, and end msgs cause no kill reply.
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB2));

                // from this point on, all is well - they both accept the new chain only.
                var msgBX2 = transportB.ReceiveAndGetReply(msgAX);
                Assert.IsInstanceOfType(msgBX2, typeof(GossipEnd));

                Assert.IsNull(transportA.ReceiveAndGetReply(msgBX2));
            }
        }

        [TestMethod]
        public void GossipMessaging_StartAfterEnd()
        {
            // the ShouldAcceptMsg became so strict at one point, that it did not accept any
            // GossipStart message coming after we sent a GossipEnd unless that new starter
            // contained the correct ReplyToId (it was LastTime then). this is of course not
            // necessary, we can always accept a starter if our last msg was an End...
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
                backendA.SetHasVec("trigger", true.Version(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgB1, typeof(GossipEnd));
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB1));

                // we now make another change on A, so that it sends a GossipStart to B. that GossipStart will
                // have ReplyToId == null, because A accepted the GossipEnd from B and cleared his state. B should
                // accept that new gossip.
                backendA.SetHasVec("trigger", false.Version(A, 2));
                var msgA2 = transportA.LastSentMessage.Msg;
                var msgB2 = transportB.ReceiveAndGetReply(msgA2);
                Assert.IsInstanceOfType(msgB2, typeof(GossipEnd));
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB2));
            }
        }

        [TestMethod]
        public void GossipMessaging_ObsoleteStartMsg()
        {
            // this is the main reason for KillGossip - if a server receives an obsolete start
            // message, it replies and enters gossip state, and then refuses to answer a GossipStart
            // that it should answer to. so by replying KillGossip to "unwanted" messages, they
            // help each other shake off the bad state.
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string> { A });
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.StartGossip,
                GossipInterval = Timeout.Infinite,
                AntiEntropyIdleTimeout = 200,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
                AntiEntropyIdleTimeout = 200,
            }))
            {
                backendA.SetHasVec("trigger", true.Version(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgB1, typeof(GossipEnd));
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB1));

                // unexpected GossipEnd messages should not cause kill replies
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB1));

                // this is an already seen GossipStart, and it has the same hash we currently have, so it actually just
                // causes a GossipEnd to be replied. this is harmless.
                var msgBX1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgBX1, typeof(GossipEnd));
                // again, no kill reply to a GossipEnd, pls.
                Assert.IsNull(transportA.ReceiveAndGetReply(msgBX1));

                // now we will change something on B, so that his hash will not be equal to the one in the delayed GossipStart.
                // he should reply, but for A that reply is unexpected.
                backendB.SetHasVec("changed", true.Version(B));
                var msgBX2 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgBX2, typeof(GossipReply));
                var msgAX2 = transportA.ReceiveAndGetReply(msgBX2);
                Assert.IsInstanceOfType(msgAX2, typeof(KillGossip));
                Assert.IsNull(transportB.ReceiveAndGetReply(msgAX2));

                // to confirm that the kill worked, let's run a small exchange
                backendA.SetHasVec("another", true.Version(A));
                var msgA3 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA3);
                // B replies GossipEnd, because A already knew about his changes - even though A previously
                // replied with KillGossip, it first applied the items from that message.
                var msgB3 = transportB.ReceiveAndGetReply(msgA3) as GossipEnd;
                Assert.IsNotNull(msgB3);
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB3));

                // and now another variant - a proper start message, but delayed too much.
                backendA.SetHasVec("again", true.Version(A));
                backendB.SetHasVec("againB", true.Version(B));
                var msgA5 = transportA.LastSentMessage.Msg as GossipStart;
                // let it time out
                Thread.Sleep(250);
                var msgB5 = transportB.ReceiveAndGetReply(msgA5) as GossipReply;
                Assert.IsNotNull(msgB5);
                var msgA6 = transportA.ReceiveAndGetReply(msgB5) as KillGossip;
                Assert.IsNotNull(msgA6);
                Assert.IsNull(transportB.ReceiveAndGetReply(msgA6));

                // and again, to confirm that the kill worked:
                backendA.SetHasVec("yet again", true.Version(A));
                var msgA7 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA7);
                var msgB7 = transportB.ReceiveAndGetReply(msgA7) as GossipEnd;
                Assert.IsNotNull(msgB7);
            }
        }

        [TestMethod]
        public void GossipMessaging_SimultaneousStartAfterEnd()
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
                backendA.SetHasVec("trigger", true.Version(A));
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgB1, typeof(GossipEnd));
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB1));

                // we now make another change on both, so they initiate simultaneous gossips
                backendA.SetHasVec("trigger", false.Version(A, 2));
                var msgA2 = transportA.LastSentMessage.Msg;
                backendB.SetHasVec("trigger", false.Version((VersionVector)(A, 1) | (B, 1)));
                var msgB2 = transportB.LastSentMessage.Msg;

                var msgA3 = transportA.ReceiveAndGetReply(msgB2);
                var msgB3 = transportB.ReceiveAndGetReply(msgA2);
                Assert.IsTrue(
                    msgA3 == null && msgB3 is GossipReply ||
                    msgB3 == null && msgA3 is GossipReply);

                if (msgA3 != null)
                    Assert.IsInstanceOfType(transportB.ReceiveAndGetReply(msgA3), typeof(GossipEnd));
                else
                    Assert.IsInstanceOfType(transportA.ReceiveAndGetReply(msgB3), typeof(GossipEnd));
            }
        }

        [TestMethod]
        public void GossipMessaging_SimultaneousStartAndReplyToEnd()
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
                backendA.SetHasVec("trigger", true.Version(A));
                backendB.SetHasVec("trigger", false.Version(B));
                var msgA1 = transportA.LastSentMessage.Msg;
                var msgB1 = transportB.ReceiveAndGetReply(msgA1);
                Assert.IsInstanceOfType(msgB1, typeof(GossipReply));
                var msgA2 = transportA.ReceiveAndGetReply(msgB1);
                Assert.IsInstanceOfType(msgA2, typeof(GossipEnd));

                // before B receives the End, both get new changes.
                backendA.SetHasVec("trigger", false.Version((VersionVector)(A, 2) | (B, 1)));
                var msgA3 = transportA.LastSentMessage.Msg;
                // A launches a new gossip, because he thinks the old one is done
                Assert.IsInstanceOfType(msgA3, typeof(GossipStart));
                backendB.SetHasVec("trigger", true.Version((VersionVector)(A, 1) | (B, 2)));
                // B does nothing, because he thinks they're still gossiping
                Assert.AreEqual(msgB1, transportB.LastSentMessage.Msg);

                // B will reply to the delayed end message
                var msgB2 = transportB.ReceiveAndGetReply(msgA2);
                Assert.IsInstanceOfType(msgB2, typeof(GossipReply));
                // and it will ignore the new GossipStart!
                Assert.IsNull(transportB.ReceiveAndGetReply(msgA3));

                // A will recognize the reply to the old End message, and both will correctly continue the old chain.
                var msgA4 = transportA.ReceiveAndGetReply(msgB2);
                Assert.IsInstanceOfType(msgA4, typeof(GossipReply));
                var msgB3 = transportB.ReceiveAndGetReply(msgA4);
                Assert.IsInstanceOfType(msgB3, typeof(GossipEnd));
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB3));
            }
        }

        [TestMethod]
        public void GossipMessaging_NeedlessItemsCheckWhenNoChangesHappened()
        {
            // the check for needless reply items involves accessing the LastFreshness in a SyncSideEffect.
            // if that field had not changed, this will cause an exception - accessing a new field is not
            // allowed in SyncSideEffect lambdas.
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string> { A });
            using (var backendA = new GossipBackend(transportA, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
            }))
            using (var backendB = new GossipBackend(transportB, new GossipConfiguration
            {
                DirectMail = DirectMailType.Off,
                GossipInterval = Timeout.Infinite,
            }))
            {
                // we'll set them up so that the B backend has the values that will come in the first message from A,
                // but their hashes are still different. B will then apply the message, but the message will cause
                // no changes.
                for (int i = 0; i < 2; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendA.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendA.Configuration.DirectMail = DirectMailType.StartGossip;
                backendA.SetHasVec("trigger", true.Version(A));

                for (int i = 1; i < 2; i++)
                {
                    Shield.InTransaction(() =>
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            backendB.SetHasVec($"key-{i:00}-{j:00}", true.Version(A));
                        }
                    });
                }
                backendB.SetHasVec("trigger", true.Version(A));

                var msgA1 = transportA.LastSentMessage.Msg;
                Assert.IsInstanceOfType(msgA1, typeof(GossipStart));

                var msgB1 = transportB.ReceiveAndGetReply(msgA1) as GossipReply;
                Assert.IsNotNull(msgB1);
                Assert.AreEqual(0, msgB1.Transactions?.Length ?? 0);

                var msgA2 = transportA.ReceiveAndGetReply(msgB1) as GossipReply;
                Assert.IsNotNull(msgA2);
                var msgB2 = transportB.ReceiveAndGetReply(msgA2) as GossipEnd;
                Assert.IsNotNull(msgB2);
                Assert.IsNull(transportA.ReceiveAndGetReply(msgB2));
            }
        }

        [TestMethod]
        public void GossipMessaging_NastyPreCommit()
        {
            // originally, the GossipBackend used a PreCommit to sync the reverse time index
            // with the main dictionary. the PreCommits are, however, problematic, because they
            // do not trigger each other - if the main transaction changes nothing, but another
            // PreCommit does make a change, the syncing of indexes would not be done, and no
            // direct mail would be activated. new version avoids using PreCommits.
            var transportA = new MockTransport(A, new List<string> { B });
            var transportB = new MockTransport(B, new List<string> { A });
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
                var x = new Shielded<int>();
                using (Shield.PreCommit(() => { int a = x; return true; }, () =>
                {
                    backendA.SetHasVec("trigger", true.Version(A));
                }))
                {
                    Shield.InTransaction(() => x.Value = 1);
                }
                var msgA1 = transportA.LastSentMessage.Msg as GossipStart;
                Assert.IsNotNull(msgA1);
                Assert.AreEqual(1, msgA1.Transactions.Length);
                Assert.AreEqual(1, msgA1.Transactions[0].Changes.Length);
                Assert.AreEqual("trigger", msgA1.Transactions[0].Changes[0].Key);
            }
        }
    }
}
