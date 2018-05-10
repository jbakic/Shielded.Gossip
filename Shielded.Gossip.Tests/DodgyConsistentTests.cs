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
    public class DodgyConsistentTests : GossipBackendThreeNodeTestsBase<ConsistentGossipBackend, DodgyTransport>
    {
        public class TestClass : IHasVectorClock
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Value { get; set; }
            public VectorClock Clock { get; set; }
        }

        protected override ConsistentGossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new ConsistentGossipBackend(transport, configuration);
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
        public void DodgyConsistent_Race()
        {
            const int transactions = 60;
            const int fieldCount = 20;

            foreach (var back in _backends.Values)
                back.Configuration.DirectMail = false;

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
                Distributed.Consistent(100, () =>
                {
                    var backend = _backends.Values.Skip(i % 3).First();
                    var id = (i % fieldCount);
                    var key = "key" + id;
                    var newVal = backend.TryGetMultiple<TestClass>(key).SingleOrDefault() ??
                        new TestClass { Id = id, Clock = new VectorClock() };
                    newVal.Value = newVal.Value + 1;
                    newVal.Clock = newVal.Clock.Next(backend.Transport.OwnId);
                    backend.SetVc(key, newVal);
                }))).Result;
            var expected = bools.Count(b => b);
            Assert.AreEqual(transactions, expected);

            Thread.Sleep(1000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Distributed.Run(() =>
                    Enumerable.Range(0, fieldCount).Sum(i =>
                        _backends[B].TryGetMultiple<TestClass>("key" + i).SingleOrDefault()?.Value))
                .Result;

            if (read < expected)
                Assert.Inconclusive($"Servers did not achieve sync within given time. Expected {expected}, read {read}");
            else if (read > expected)
                Assert.Fail();
        }

        [TestMethod]
        public void DodgyConsistent_RaceAsymmetric()
        {
            const int transactions = 60;
            const int fieldCount = 21;

            Shield.InTransaction(() =>
            {
                ((DodgyTransport)_backends[A].Transport).ServerIPs.Remove(C);
                ((DodgyTransport)_backends[C].Transport).ServerIPs.Remove(A);
            });
            foreach (var back in _backends.Values)
                back.Configuration.DirectMail = false;

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
                Distributed.Consistent(100, () =>
                {
                    // updates only on A and B
                    var backend = _backends.Skip(i % 2).First().Value;
                    var id = (i % fieldCount);
                    var key = "key" + id;
                    var newVal = backend.TryGetMultiple<TestClass>(key).SingleOrDefault() ??
                        new TestClass { Id = id, Clock = new VectorClock() };
                    newVal.Value = newVal.Value + 1;
                    newVal.Clock = newVal.Clock.Next(backend.Transport.OwnId);
                    backend.SetVc(key, newVal);
                }))).Result;
            var expected = bools.Count(b => b);
            Assert.AreEqual(transactions, expected);

            Thread.Sleep(3000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Distributed.Run(() =>
                    Enumerable.Range(0, fieldCount).Sum(i =>
                        _backends[B].TryGetMultiple<TestClass>("key" + i).SingleOrDefault()?.Value))
                .Result;

            if (read < expected)
                Assert.Inconclusive($"Servers did not achieve sync within given time. Expected {expected}, read {read}");
            else if (read > expected)
                Assert.Fail();
        }
    }
}
