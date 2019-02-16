using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
            return new ConsistentGossipBackend(transport, configuration, new List<string> { A, B, C });
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
                back.Configuration.DirectMail = DirectMailType.StartGossip;

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
            {
                var backend = _backends.Values.Skip(i % 3).First();
                var id = (i % fieldCount);
                var key = "key" + id;
                return backend.RunConsistent(() =>
                {
                    var newVal = backend.TryGetMultiple<TestClass>(key).SingleOrDefault() ??
                        new TestClass { Id = id, Clock = new VectorClock() };
                    newVal.Value = newVal.Value + 1;
                    newVal.Clock = newVal.Clock.Next(backend.Transport.OwnId);
                    backend.SetVc(key, newVal);
                }, 100);
            })).Result;
            var expected = bools.Count(b => b);
            Assert.AreEqual(transactions, expected);

            CheckProtocols();

            var (success, value) = _backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    _backends[B].TryGetMultiple<TestClass>("key" + i).SingleOrDefault()?.Value)).Result;

            Assert.IsTrue(success);
            Assert.AreEqual(expected, value);
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
                back.Configuration.DirectMail = DirectMailType.StartGossip;

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
            {
                // updates only on A and B
                var backend = _backends.Skip(i % 2).First().Value;
                var id = (i % fieldCount);
                var key = "key" + id;
                return backend.RunConsistent(() =>
                {
                    var newVal = backend.TryGetMultiple<TestClass>(key).SingleOrDefault() ??
                        new TestClass { Id = id, Clock = new VectorClock() };
                    newVal.Value = newVal.Value + 1;
                    newVal.Clock = newVal.Clock.Next(backend.Transport.OwnId);
                    backend.SetVc(key, newVal);
                }, 100);
            })).Result;
            var expected = bools.Count(b => b);
            Assert.AreEqual(transactions, expected);

            CheckProtocols();

            var (success, value) = _backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    _backends[B].TryGetMultiple<TestClass>("key" + i).SingleOrDefault()?.Value)).Result;

            Assert.IsTrue(success);
            Assert.AreEqual(expected, value);
        }
    }
}
