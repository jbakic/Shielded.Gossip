using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class DodgyBackendTests : GossipBackendThreeNodeTestsBase<GossipBackend, DodgyTransport>
    {
        protected override GossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new GossipBackend(transport, configuration);
        }

        protected override DodgyTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers)
        {
            var transport = new TcpTransport(ownId, servers);
            transport.Error += OnListenerError;
            transport.StartListening();
            return new DodgyTransport(transport);
        }

        [TestMethod]
        public void DodgyBackend_RaceAsymmetric()
        {
            const int transactions = 1000;
            const int fieldCount = 50;

            Shield.InTransaction(() =>
            {
                ((DodgyTransport)_backends[A].Transport).ServerIPs.Remove(C);
                ((DodgyTransport)_backends[C].Transport).ServerIPs.Remove(A);
            });
            foreach (var back in _backends.Values)
                back.Configuration.DirectMail = DirectMailType.StartGossip;

            ParallelEnumerable.Range(1, transactions).ForAll(i =>
                Shield.InTransaction(() =>
                {
                    // run all updates on A, to cause asymmetry in the amount of data they have to gossip about.
                    var backend = _backends[A];
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }));

            Thread.Sleep(5000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Shield.InTransaction(() =>
                Enumerable.Range(0, fieldCount).Sum(i => _backends[C].TryGet("key" + i, out CountVector v) ? v.Value : 0));

            if (read < transactions)
                Assert.Inconclusive($"Servers did not achieve sync within given time. Expected {transactions}, read {read}");
            else if (read > transactions)
                Assert.Fail();
        }
    }
}
