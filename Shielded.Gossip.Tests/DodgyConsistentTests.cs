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
        public class TestClass : IHasVersionVector
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Value { get; set; }
            public VersionVector Version { get; set; }
        }

        protected override ConsistentGossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new ConsistentGossipBackend(transport, configuration)
            {
                TransactionParticipants = new string[] { A, B, C }
            };
        }

        protected override DodgyTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers)
        {
            var transport = new TcpTransport(ownId, servers);
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
                    var newVal = backend.TryGetHasVec<TestClass>(key).SingleOrDefault() ??
                        new TestClass { Id = id, Version = new VersionVector() };
                    newVal.Value = newVal.Value + 1;
                    newVal.Version = newVal.Version.Next(backend.Transport.OwnId);
                    backend.SetHasVec(key, newVal);
                }, 100);
            })).Result;
            var expected = bools.Count(b => b);
            Assert.AreEqual(transactions, expected);

            CheckProtocols();

            var (success, value) = _backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    _backends[B].TryGetHasVec<TestClass>("key" + i).SingleOrDefault()?.Value)).Result;

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
                    var newVal = backend.TryGetHasVec<TestClass>(key).SingleOrDefault() ??
                        new TestClass { Id = id, Version = new VersionVector() };
                    newVal.Value = newVal.Value + 1;
                    newVal.Version = newVal.Version.Next(backend.Transport.OwnId);
                    backend.SetHasVec(key, newVal);
                }, 100);
            })).Result;
            var expected = bools.Count(b => b);
            Assert.AreEqual(transactions, expected);

            CheckProtocols();

            var (success, value) = _backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    _backends[B].TryGetHasVec<TestClass>("key" + i).SingleOrDefault()?.Value)).Result;

            Assert.IsTrue(success);
            Assert.AreEqual(expected, value);
        }
    }
}
