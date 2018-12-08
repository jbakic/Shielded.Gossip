using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

        protected override TcpTransport CreateTransport(string ownId, IPEndPoint localEndpoint,
            IEnumerable<KeyValuePair<string, IPEndPoint>> servers)
        {
            var transport = new TcpTransport(ownId, localEndpoint,
                new ShieldedDict<string, IPEndPoint>(servers, null, StringComparer.InvariantCultureIgnoreCase));
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
            Shield.InTransaction(() =>
            {
                _backends[C].DirectMailRestriction.Value = null;
                _backends[C].SetVc("key", "rejected".Clock(C));
            });

            Assert.IsTrue(_backends[A].RunConsistent(() => { _backends[A].SetVc("key", "accepted".Clock(A)); }).Result);

            Thread.Sleep(100);
            CheckProtocols();

            var read = _backends[B].RunConsistent(() => _backends[B].TryGetClocked<string>("key"))
                .Result.Value.Single();

            Assert.AreEqual("accepted", read.Value);
            Assert.AreEqual((A, 1), read.Clock);

            // on the C server the field will have two versions! seems like he's stubborn, but
            // it makes sense. the only way he could have accepted that "rejected" version is
            // if someone wrote it in non-consistently. and consistent transactions never block
            // non-consistent ones.
            var readCMulti = _backends[C].RunConsistent(() => _backends[C].TryGetClocked<string>("key"))
                .Result.Value;

            Assert.AreEqual((VectorClock)(A, 1) | (C, 1), readCMulti.MergedClock);
        }

        [TestMethod]
        public void ConsistentWithoutGossip_RejectionByMajority()
        {
            // we'll set a version on two servers, but only locally. the A server will try to run the
            // transaction and B and C will reject it.
            Shield.InTransaction(() =>
            {
                _backends[B].DirectMailRestriction.Value = null;
                _backends[B].SetVc("key", "rejected".Clock(B));
            });
            Shield.InTransaction(() =>
            {
                _backends[C].DirectMailRestriction.Value = null;
                _backends[C].SetVc("key", "rejected".Clock(B));
            });

            Assert.IsFalse(_backends[A].RunConsistent(() => { _backends[A].SetVc("key", "accepted".Clock(A)); }).Result);

            Thread.Sleep(100);
            CheckProtocols();

            // NB we still cannot read the value on A, because gossip is disabled.
            var read = _backends[B].RunConsistent(() => _backends[B].TryGetClocked<string>("key"))
                .Result.Value.Single();

            Assert.AreEqual("rejected", read.Value);
            Assert.AreEqual((B, 1), read.Clock);
        }
    }
}
