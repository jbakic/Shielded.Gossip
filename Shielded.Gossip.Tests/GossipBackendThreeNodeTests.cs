using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class GossipBackendThreeNodeTests
    {
        [Serializable]
        public class TestClass : IHasVectorClock
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public VectorClock Clock { get; set; }
        }

        private const string A = "A";
        private const string B = "B";
        private const string C = "C";

        private static readonly Dictionary<string, IPEndPoint> _addresses = new Dictionary<string, IPEndPoint>(StringComparer.OrdinalIgnoreCase)
        {
            { A, new IPEndPoint(IPAddress.Loopback, 2001) },
            { B, new IPEndPoint(IPAddress.Loopback, 2002) },
            { C, new IPEndPoint(IPAddress.Loopback, 2003) },
        };

        private GossipBackend[] _backends;

        [TestInitialize]
        public void Init()
        {
            _backends = _addresses.Select(kvp =>
                new GossipBackend(new GossipProtocol(kvp.Key, kvp.Value,
                    new Dictionary<string, IPEndPoint>(_addresses.Where(inner => inner.Key != kvp.Key), StringComparer.OrdinalIgnoreCase))))
                .ToArray();
        }

        [TestCleanup]
        public void Cleanup()
        {
            foreach (var back in _backends)
                back.Protocol.Dispose();
            _backends = null;
        }

        private void CheckProtocols()
        {
            foreach (var back in _backends)
                Assert.IsNull(back.Protocol.ListenerException);
        }

        [TestMethod]
        public void GossipBackendMultiple_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };

            Distributed.Run(() => _backends[0].SetVersion("key", testEntity)).Wait();

            Thread.Sleep(100);
            CheckProtocols();

            var read = Distributed.Run(() => _backends[1].TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }
    }
}
