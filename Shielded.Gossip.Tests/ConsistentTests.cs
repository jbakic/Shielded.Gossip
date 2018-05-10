using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ConsistentTests : GossipBackendThreeNodeTestsBase<ConsistentGossipBackend, TcpTransport>
    {
        public class TestClass
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Counter { get; set; }
        }

        protected override ConsistentGossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
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
        public void Consistent_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            Assert.IsTrue(Distributed.Consistent(() => { _backends[A].SetVc("key", testEntity.Clock(A)); }).Result);

            Thread.Sleep(100);
            CheckProtocols();

            var read = Distributed.Consistent(() => _backends[B].TryGetClocked<TestClass>("key"))
                .Result.Value.Single();

            Assert.AreEqual(testEntity.Id, read.Value.Id);
            Assert.AreEqual(testEntity.Name, read.Value.Name);
            Assert.AreEqual((A, 1), read.Clock);
        }

        [TestMethod]
        public void Consistent_Race()
        {
            const int transactions = 500;
            const int fieldCount = 50;

            foreach (var back in _backends.Values)
            {
                back.Configuration.DirectMail = false;
            }

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
                Distributed.Consistent(100, () =>
                {
                    var backend = _backends.Values.Skip(i % 3).First();
                    var id = (i % fieldCount);
                    var key = "key" + id;
                    var newVal = backend.TryGetClocked<TestClass>(key)
                        .SingleOrDefault()
                        .NextVersion(backend.Transport.OwnId);
                    if (newVal.Value == null)
                        newVal.Value = new TestClass { Id = id };
                    newVal.Value.Counter = newVal.Value.Counter + 1;
                    backend.SetVc(key, newVal);
                }))).Result;
            var expected = bools.Count(b => b);

            Thread.Sleep(300);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Distributed.Run(() =>
                    Enumerable.Range(0, fieldCount).Sum(i =>
                        _backends[B].TryGetClocked<TestClass>("key" + i).SingleOrDefault().Value?.Counter))
                .Result;

            Assert.AreEqual(expected, read);
            Assert.AreEqual(transactions, read);
        }

        [TestMethod]
        public void Consistent_Versioned()
        {
            var testEntity = new TestClass { Id = 1, Name = "New entity" };

            Distributed.Consistent(() => { _backends[A].Set("key", testEntity.Version()); }).Wait();

            Thread.Sleep(100);

            Versioned<TestClass> read = default, next = default;
            Distributed.Consistent(() =>
            {
                read = _backends[B].TryGetVersioned<TestClass>("key");
                Assert.AreEqual(testEntity.Name, read.Value.Name);

                next = read.NextVersion();
                next.Value = new TestClass { Id = 1, Name = "Version 2" };
                _backends[B].Set("key", next);
            }).Wait();

            Thread.Sleep(100);

            read = Distributed.Run(() => _backends[C].TryGetVersioned<TestClass>("key")).Result;
            Assert.AreEqual(next.Value.Name, read.Value.Name);
        }
    }
}
