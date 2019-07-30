using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ConsistentWithoutGossipTests : GossipBackendThreeNodeTestsBase<ConsistentGossipBackend, TcpTransport>
    {
        protected override ConsistentGossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            configuration.GossipInterval = Timeout.Infinite;
            return new ConsistentGossipBackend(transport, configuration);
        }

        protected override TcpTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers)
        {
            var transport = new TcpTransport(ownId, servers);
            transport.Error += OnListenerError;
            transport.StartListening();
            return transport;
        }

        [TestMethod]
        public void ConsistentWithoutGossip_BasicCheck()
        {
            const int transactions = 50;
            const int fieldCount = 20;

            // test non-consistent transactions to confirm the consistent backend correctly works with the wrapped backend.
            ParallelEnumerable.Range(1, transactions).ForAll(i =>
                Shield.InTransaction(() =>
                {
                    var backend = _backends.Values.Skip(i % 3).First();
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }));

            Thread.Sleep(500);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Shield.InTransaction(() =>
                Enumerable.Range(0, fieldCount).Sum(i => _backends[B].TryGet("key" + i, out CountVector v) ? v.Value : 0));

            Assert.AreEqual(transactions, read);
        }

        [TestMethod]
        public void ConsistentWithoutGossip_RejectionOverruled()
        {
            // we'll set a version on one server, but only locally. he will reject the transaction, but will
            // be in a minority, and the transaction will go through.
            _backends[C].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[C].SetHasVec("key", "rejected".Version(C));
            });
            _backends[C].Configuration.DirectMail = DirectMailType.GossipSupressed;

            Assert.IsTrue(_backends[A].RunConsistent(() => { _backends[A].SetHasVec("key", "accepted".Version(A)); }, 1).Result);

            CheckProtocols();

            // the field will now have two versions on all servers, due to the C server transmitting his version
            // as part of rejecting the transaction.
            var readA = _backends[A].RunConsistent(() => _backends[A].TryGetVecVersioned<string>("key"))
                .Result.Value;

            Assert.AreEqual((VersionVector)(A, 1) | (C, 1), readA.MergedClock);

            var readB = _backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<string>("key"))
                .Result.Value;

            Assert.AreEqual((VersionVector)(A, 1) | (C, 1), readB.MergedClock);

            var readC = _backends[C].RunConsistent(() => _backends[C].TryGetVecVersioned<string>("key"))
                .Result.Value;

            Assert.AreEqual((VersionVector)(A, 1) | (C, 1), readC.MergedClock);
        }

        [TestMethod]
        public void ConsistentWithoutGossip_RejectionByMajority()
        {
            // we'll set a version on two servers, but only locally. the A server will try to run the
            // transaction and B and C will reject it.
            _backends[B].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[B].SetHasVec("key", "rejected".Version(B));
            });
            _backends[B].Configuration.DirectMail = DirectMailType.GossipSupressed;
            _backends[C].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[C].SetHasVec("key", "rejected".Version(B));
            });
            _backends[C].Configuration.DirectMail = DirectMailType.GossipSupressed;

            // we can make only one attempt, because the B/C version will be sent to us as part of their rejection of
            // the transaction. after that, this would succeed, but with SetHasVec result == Conflict.
            Assert.IsFalse(_backends[A].RunConsistent(() => { _backends[A].SetHasVec("key", "accepted".Version(A)); }, 1).Result);

            CheckProtocols();

            // the A server should now see the other version.
            var read = _backends[A].RunConsistent(() => _backends[A].TryGetVecVersioned<string>("key"))
                .Result.Value.Single();

            Assert.AreEqual("rejected", read.Value);
            Assert.AreEqual((B, 1), read.Version);
        }

        [TestMethod]
        public void ConsistentWithoutGossip_WriteTwoVersions()
        {
            // we'll set a version on two servers, but only locally. the A server will try to run a
            // transaction and B and C should reject it. however, A will write two versions of the data,
            // which will be only IntVersioned, so it might seem as if A's write should win! This was
            // an issue due to not remembering what state a field had at transaction start, but rather
            // just sending the written value.
            _backends[B].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[B].Set("key", "rejected".Version(1));
            });
            _backends[B].Configuration.DirectMail = DirectMailType.GossipSupressed;
            _backends[C].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[C].Set("key", "rejected".Version(1));
            });
            _backends[C].Configuration.DirectMail = DirectMailType.GossipSupressed;

            Assert.IsFalse(_backends[A].RunConsistent(() =>
            {
                _backends[A].Set("key", "first".Version(1));
                Assert.AreEqual(VectorRelationship.Greater, _backends[A].Set("key", "second".Version(2)));
            }, 1).Result);

            CheckProtocols();

            // the A server should now see the other version.
            var read = _backends[A].RunConsistent(() => _backends[A].TryGetIntVersioned<string>("key"))
                .Result.Value;

            Assert.AreEqual("rejected", read.Value);
            Assert.AreEqual(1, read.Version);
        }

        [TestMethod]
        public void ConsistentWithoutGossip_TouchInconsistent()
        {
            // we will test if the Touch method will transmit the value known only to C to other servers.
            _backends[C].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[C].SetHasVec("key", "rejected".Version(C));
            });
            _backends[C].Configuration.DirectMail = DirectMailType.GossipSupressed;

            Shield.InTransaction(() => _backends[C].Touch("key"));

            Thread.Sleep(100);
            CheckProtocols();

            var read = _backends[A].TryGetVecVersioned<string>("key");

            Assert.AreEqual("rejected", read.Single().Value);
        }

        [TestMethod]
        public void ConsistentWithoutGossip_TouchConsistent()
        {
            // in a consistent transaction, touch is just like a read, and should be transmitted with the transaction.
            _backends[C].Configuration.DirectMail = DirectMailType.Off;
            Shield.InTransaction(() =>
            {
                _backends[C].SetHasVec("key", "rejected".Version(C));
            });
            _backends[C].Configuration.DirectMail = DirectMailType.GossipSupressed;

            Assert.IsTrue(_backends[C].RunConsistent(() => _backends[C].Touch("key")).Result);

            CheckProtocols();

            var (success, read) = _backends[A].RunConsistent(() => _backends[A].TryGetVecVersioned<string>("key")).Result;

            Assert.IsTrue(success);
            Assert.AreEqual("rejected", read.Single().Value);
        }
    }
}
