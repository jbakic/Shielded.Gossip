using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
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
    public class DodgyBackendTests : GossipBackendThreeNodeTestsBase<GossipBackend, DodgyTransport>
    {
        protected override GossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new GossipBackend(transport, configuration);
        }

        protected override DodgyTransport CreateTransport(string ownId, IPEndPoint localEndpoint,
            IEnumerable<KeyValuePair<string, IPEndPoint>> servers)
        {
            var transport = new TcpTransport(ownId, localEndpoint,
                new ShieldedDict<string, IPEndPoint>(servers, null, StringComparer.InvariantCultureIgnoreCase));
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
                back.Configuration.DirectMail = false;

            Task.WaitAll(Enumerable.Range(1, transactions).Select(i =>
                Task.Run(() => Distributed.Run(() =>
                {
                    // run all updates on A, to cause asymmetry in the amount of data they have to gossip about.
                    var backend = _backends[A];
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }).Wait())).ToArray());

            Thread.Sleep(5000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Distributed.Run(() =>
                    Enumerable.Range(0, fieldCount).Sum(i => _backends[C].TryGet("key" + i, out CountVector v) ? v.Value : 0))
                .Result;

            Assert.AreEqual(transactions, read);
        }
    }
}
