﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
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
            const int transactions = 50;
            const int fieldCount = 20;

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
                    var val = backend.TryGet(key, out Multiple<TestClass> v) ? v : new TestClass { Id = id, Clock = new VectorClock() };
                    if (val.Items.Length > 1)
                        Assert.Fail("Conflict detected.");
                    var newVal = val.Items[0];
                    newVal.Value = newVal.Value + 1;
                    newVal.Clock = newVal.Clock.Next(backend.Transport.OwnId);
                    backend.SetVersion(key, newVal);
                }))).Result;
            var expected = bools.Count(b => b);

            Thread.Sleep(1000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Distributed.Run(() =>
                    Enumerable.Range(0, fieldCount).Sum(i => _backends[B].TryGet("key" + i, out Multiple<TestClass> v) ? v.Single().Value : 0))
                .Result;

            Assert.AreEqual(expected, read);
            Assert.AreEqual(transactions, read);
        }
    }
}