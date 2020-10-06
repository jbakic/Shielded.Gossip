using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ConsistentMessagingTests
    {
        const string A = "A";
        const string B = "B";
        const string C = "C";

        [TestMethod]
        public void ConsistentMessaging_PromisedVsPromised()
        {
            var transportA = new MockTransport(A, new List<string> { B, C });
            var transportB = new MockTransport(B, new List<string> { A, C });
            var transportC = new MockTransport(C, new List<string> { A, B });

            var transportDict = new Dictionary<string, MockTransport> { { A, transportA }, { B, transportB }, { C, transportC } };

            using var backendA = new ConsistentGossipBackend(transportA, new GossipConfiguration { GossipInterval = Timeout.Infinite });
            using var backendB = new ConsistentGossipBackend(transportB, new GossipConfiguration { GossipInterval = Timeout.Infinite });
            using var backendC = new ConsistentGossipBackend(transportC, new GossipConfiguration { GossipInterval = Timeout.Infinite });

            // A starts transactionA, goes to Promised on it, sends message to both
            var taskA = backendA.RunConsistent(() => backendA.Set("key1", 1.Version(1)));
            Assert.AreEqual(2, transportA.SentMessages.Count);
            var idA = ((DirectMail)transportA.LastSentMessage.Msg).Items[0].Key;

            // B does the same.
            var taskB = backendB.RunConsistent(() => backendB.Set("key1", 1.Version(1)));
            Assert.AreEqual(2, transportB.SentMessages.Count);
            var idB = ((DirectMail)transportB.LastSentMessage.Msg).Items[0].Key;

            // we will now just pump messages between A and B for a couple of rounds. they are both promised on their own transactions.
            // the higher prio transaction should be committed, and the lower prio one rejected.
            foreach (var _ in Enumerable.Repeat(0, 10))
            {
                foreach (var msg in transportA.SentMessages.Where(m => m.To == B))
                    transportB.Receive(msg.Msg);
                foreach (var msg in transportB.SentMessages.Where(m => m.To == A))
                    transportA.Receive(msg.Msg);
                // since they do some things with a delay, in async side-effects.
                Thread.Sleep(50);
            }

            var transAFinal = GetLatest(idA, transportDict.Values);
            var transBFinal = GetLatest(idB, transportDict.Values);
            Assert.IsTrue(transAFinal.IsDone);
            Assert.IsTrue(transBFinal.IsDone);
            Assert.IsTrue(transAFinal.IsSuccessful ^ transBFinal.IsSuccessful);

            // both tasks get completed, because during the message pumping they actually resolved the retry of the failed transaction.
            // i'm not really interested in the Tasks here, but does not hurt to check them too.
            Assert.IsTrue(taskA.IsCompleted);
            Assert.AreEqual(ConsistentOutcome.Success, taskA.Result.Outcome);
            Assert.IsTrue(taskB.IsCompleted);
            Assert.AreEqual(ConsistentOutcome.Success, taskB.Result.Outcome);
        }

        [TestMethod]
        public void ConsistentMessaging_PromisedVsAccepted()
        {
            var transportA = new MockTransport(A, new List<string> { B, C });
            var transportB = new MockTransport(B, new List<string> { A, C });
            var transportC = new MockTransport(C, new List<string> { A, B });

            var transportDict = new Dictionary<string, MockTransport> { { A, transportA }, { B, transportB }, { C, transportC } };

            using var backendA = new ConsistentGossipBackend(transportA, new GossipConfiguration { GossipInterval = Timeout.Infinite });
            using var backendB = new ConsistentGossipBackend(transportB, new GossipConfiguration { GossipInterval = Timeout.Infinite });
            using var backendC = new ConsistentGossipBackend(transportC, new GossipConfiguration { GossipInterval = Timeout.Infinite });

            // A starts transactionA, goes to Promised on it, sends message to both
            var taskA = backendA.RunConsistent(() => backendA.Set("key1", 1.Version(1)));
            Assert.AreEqual(2, transportA.SentMessages.Count);
            var idA = ((DirectMail)transportA.LastSentMessage.Msg).Items[0].Key;

            // B does the same. we run both first, since i don't know which one will have higer prio until their IDs have been decided. a bit annoying.
            var taskB = backendB.RunConsistent(() => backendB.Set("key1", 1.Version(1)));
            Assert.AreEqual(2, transportB.SentMessages.Count);
            var idB = ((DirectMail)transportB.LastSentMessage.Msg).Items[0].Key;

            var higherPrioServer = idA.CompareTo(idB) < 0 ? A : B;
            var lowerPrioServer = higherPrioServer == A ? B : A;

            // C receives the lower prio transaction, and votes Accepted on it
            var lowToC1 = transportDict[lowerPrioServer].SentMessages.Last(m => m.To == C);
            transportC.Receive(lowToC1.Msg);
            Assert.AreEqual(TransactionState.Accepted, ((TransactionInfo)((DirectMail)transportC.LastSentMessage.Msg).Items[0].Value).State[C]);

            // C now receives the higher prio transaction, but will not do anything to it. no direct mail will be created.
            var msgCountBefore = transportC.SentMessages.Count;
            var highToC1 = transportDict[higherPrioServer].SentMessages.Last(m => m.To == C);
            transportC.Receive(highToC1.Msg);
            var msgCountAfter = transportC.SentMessages.Count;
            Assert.AreEqual(msgCountBefore, msgCountAfter);

            // we will now just pump messages between high-prio and C for a couple of rounds, and confirm that the two transactions remain unresolved.
            // this simulates a partition where the lower prio initiator has disconnected. C has accepted that transaction and cannot move,
            // and the higher prio initiator is Promised on its own higher prio transaction, so it cannot move on the lower prio one either.
            foreach (var _ in Enumerable.Repeat(0, 10))
            {
                foreach (var msg in transportC.SentMessages.Where(m => m.To == higherPrioServer))
                    transportDict[higherPrioServer].Receive(msg.Msg);
                foreach (var msg in transportDict[higherPrioServer].SentMessages.Where(m => m.To == C))
                    transportC.Receive(msg.Msg);
                // since they do some things with a delay, in async side-effects.
                Thread.Sleep(50);
            }

            // check that the transactions have not been resolved. if this issue gets fixed, this test will fail.
            var transAFinal = GetLatest(idA, transportDict.Values);
            var transBFinal = GetLatest(idB, transportDict.Values);
            Assert.IsFalse(transAFinal.IsDone);
            Assert.IsFalse(transBFinal.IsDone);
            Assert.IsFalse(taskA.IsCompleted);
            Assert.IsFalse(taskB.IsCompleted);
        }

        private TransactionInfo GetLatest(string id, IEnumerable<MockTransport> transports)
        {
            return transports
                .Select(t =>
                    t.SentMessages.Select(m => m.Msg).OfType<DirectMail>().SelectMany(dm => dm.Items).LastOrDefault(i => i.Key == id))
                .Where(i => i != null)
                .Select(i => (TransactionInfo)i.Value)
                .Aggregate((ti1, ti2) => ti1.MergeWith(ti2));
        }

        [TestMethod]
        public void ConsistentMessaging_AcceptedVsAccepted()
        {
            var transportA = new MockTransport(A, new List<string> { B, C });
            var transportB = new MockTransport(B, new List<string> { A, C });
            var transportC = new MockTransport(C, new List<string> { A, B });

            var transportDict = new Dictionary<string, MockTransport> { { A, transportA }, { B, transportB }, { C, transportC } };

            using var backendA = new ConsistentGossipBackend(transportA, new GossipConfiguration { GossipInterval = Timeout.Infinite });
            using var backendB = new ConsistentGossipBackend(transportB, new GossipConfiguration { GossipInterval = Timeout.Infinite });
            using var backendC = new ConsistentGossipBackend(transportC, new GossipConfiguration { GossipInterval = Timeout.Infinite });

            // A starts transactionA, goes to Promised on it, sends message to both
            var taskA = backendA.RunConsistent(() => backendA.Set("key1", 1.Version(1)));
            Assert.AreEqual(2, transportA.SentMessages.Count);
            var idA = ((DirectMail)transportA.LastSentMessage.Msg).Items[0].Key;

            // B does the same.
            var taskB = backendB.RunConsistent(() => backendB.Set("key1", 1.Version(1)));
            Assert.AreEqual(2, transportB.SentMessages.Count);
            var idB = ((DirectMail)transportB.LastSentMessage.Msg).Items[0].Key;

            var higherPrioServer = idA.CompareTo(idB) < 0 ? A : B;
            var lowerPrioServer = higherPrioServer == A ? B : A;

            // C receives the lower prio transaction, and votes Accepted on it
            var lowToC1 = transportDict[lowerPrioServer].SentMessages.Last(m => m.To == C);
            transportC.Receive(lowToC1.Msg);
            Assert.AreEqual(TransactionState.Accepted, ((TransactionInfo)((DirectMail)transportC.LastSentMessage.Msg).Items[0].Value).State[C]);

            // the lower prio server receives the higher prio transaction, and votes Accepted on it, since it is higher prio and he does not know
            // about C's vote for his transaction...
            var msgCountBefore = transportDict[lowerPrioServer].SentMessages.Count;
            var highToLow1 = transportDict[higherPrioServer].SentMessages.Last(m => m.To == lowerPrioServer);
            transportDict[lowerPrioServer].Receive(highToLow1.Msg);
            var msgCountAfter = transportDict[lowerPrioServer].SentMessages.Count;
            Assert.IsTrue(msgCountAfter > msgCountBefore);
            Assert.AreEqual(TransactionState.Accepted, ((TransactionInfo)((DirectMail)transportDict[lowerPrioServer].LastSentMessage.Msg).Items[0].Value).State[lowerPrioServer]);

            // so far no transaction is resolved, we've done the bare minimum of communicating. now we pump messages between the two servers with
            // Accepted votes - the lower prio server, who is now Accepted on the high prio transaction, and the C server who is Accepted on the
            // low prio transaction.
            foreach (var _ in Enumerable.Repeat(0, 10))
            {
                foreach (var msg in transportC.SentMessages.Where(m => m.To == lowerPrioServer))
                    transportDict[lowerPrioServer].Receive(msg.Msg);
                foreach (var msg in transportDict[lowerPrioServer].SentMessages.Where(m => m.To == C))
                    transportC.Receive(msg.Msg);
                // since they do some things with a delay, in async side-effects.
                Thread.Sleep(50);
            }

            // the high-prio transaction should have been committed by C and the lower prio server. the low prio one remains open on C.
            // he cannot safely change that Accepted vote to Rejected because he does not know if maybe that transaction actually
            // committed already, but he was just not informed about it yet. from his perspective it's possible that the higher prio
            // transaction is actually a logical successor of the lower prio one (had this case in some race tests).
            var transAFinal = GetLatest(idA, transportDict.Values);
            var transBFinal = GetLatest(idB, transportDict.Values);
            if (higherPrioServer == A)
            {
                Assert.IsTrue(transAFinal.IsSuccessful);
                Assert.AreEqual((TransactionVector)(B, TransactionState.Rejected) | (C, TransactionState.Accepted), transBFinal.State);
            }
            else
            {
                Assert.IsTrue(transBFinal.IsSuccessful);
                Assert.AreEqual((TransactionVector)(A, TransactionState.Rejected) | (C, TransactionState.Accepted), transAFinal.State);
            }
        }

        [TestMethod]
        public void ConsistentMessaging_RevivingAcceptedTrans()
        {
            // this test will commit a transaction between 2 servers while its initiator is disconnected. we will then give them enough
            // time to clean up all traces of the transaction from their memory. once the initiator reconnects, this transaction will
            // get rejected, but it's important that he does not retry in this case!

            var transportA = new MockTransport(A, new List<string> { B, C });
            var transportB = new MockTransport(B, new List<string> { A, C });
            var transportC = new MockTransport(C, new List<string> { A, B });

            var transportDict = new Dictionary<string, MockTransport> { { A, transportA }, { B, transportB }, { C, transportC } };

            var config = new GossipConfiguration
            {
                GossipInterval = Timeout.Infinite,
                CleanUpInterval = 100,
                RemovableItemLingerMs = 200,
            };

            using var backendA = new ConsistentGossipBackend(transportA, config);
            using var backendB = new ConsistentGossipBackend(transportB, config);
            using var backendC = new ConsistentGossipBackend(transportC, config);

            // A starts transactionA, goes to Promised on it, sends message to both. this transaction is written
            // so that if retried after it has committed already, it does not notice and produces a duplicate effect.
            var taskA = backendA.RunConsistent(() =>
            {
                var curr = backendA.TryGetIntVersioned<int>("key1");
                backendA.Set("key1", curr.NextVersion(curr.Value + 1));
            });
            Assert.AreEqual(2, transportA.SentMessages.Count);
            var idA = ((DirectMail)transportA.LastSentMessage.Msg).Items[0].Key;

            // C receives the transaction, and votes Accepted on it
            var aToC1 = transportDict[A].SentMessages.Last(m => m.To == C);
            transportC.Receive(aToC1.Msg);
            Assert.AreEqual(TransactionState.Accepted, ((TransactionInfo)((DirectMail)transportC.LastSentMessage.Msg).Items[0].Value).State[C]);

            // we now pump messages between B and C to have them complete this transaction. since the transactions is quick to expire, this
            // loop is more careful and sends only new stuff. without this, they actually revive the transaction all by themselves.
            var processedCountC = 0;
            var processedCountB = 0;
            foreach (var _ in Enumerable.Repeat(0, 10))
            {
                foreach (var msg in transportC.SentMessages.Skip(processedCountC).Where(m => m.To == B))
                    transportDict[B].Receive(msg.Msg);
                processedCountC = transportC.SentMessages.Count;
                foreach (var msg in transportDict[B].SentMessages.Skip(processedCountB).Where(m => m.To == C))
                    transportC.Receive(msg.Msg);
                processedCountB = transportB.SentMessages.Count;
                // since they do some things with a delay, in async side-effects.
                Thread.Sleep(50);
            }

            // due to the Thread.Sleep, at least 500 ms have passed. they only require one round to commit, so at least 400 ms
            // after committing. with the config above, that's enough time to clean the transaction from their store. there is no
            // API to confirm this right now, it's not possible to query transactions.

            // we still have the message from A to B, so we use it to revive the transaction. B promptly rejects the transaction.
            var aToB1 = transportDict[A].SentMessages.Last(m => m.To == B);
            transportB.Receive(aToB1.Msg);
            // NB that B also sends the reason for the rejection, so we must pick the right item from the message a bit more carefully:
            Assert.AreEqual(TransactionState.Rejected, ((DirectMail)transportB.LastSentMessage.Msg).Items.Select(i => i.Value).OfType<TransactionInfo>().FirstOrDefault().State[B]);

            // we pass B's latest message to C, who will now also reject this transaction.
            var bToC1 = transportDict[B].SentMessages.Last(m => m.To == C);
            transportC.Receive(bToC1.Msg);
            Assert.AreEqual(TransactionState.Rejected, ((DirectMail)transportC.LastSentMessage.Msg).Items.Select(i => i.Value).OfType<TransactionInfo>().FirstOrDefault().State[C]);

            // the transaction is now fully rejected. we send word of this to A. it may not retry!
            var prevAMsgCount = transportA.SentMessages.Count;
            var cToA1 = transportDict[C].SentMessages.Last(m => m.To == A);
            transportA.Receive(cToA1.Msg);
            // retry gets started async, so give it time
            Thread.Sleep(100);
            var currAMsgCount = transportA.SentMessages.Count;

            Assert.AreEqual(prevAMsgCount, currAMsgCount);
        }
    }
}
