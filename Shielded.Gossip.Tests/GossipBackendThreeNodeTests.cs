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
        public class TestClass : IHasVectorClock<TestClass>
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public VectorClock Clock { get; set; }

            public Multiple<TestClass> Wrap() => (Multiple<TestClass>)this;
            public Multiple<TestClass> MergeWith(TestClass other) => this.DefaultMerge(other);
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

        private GossipProtocol[] _protocols;
        private Node[] _nodes;

        [TestInitialize]
        public void Init()
        {
            var tuples = _addresses.Select(kvp =>
            {
                var proto = new GossipProtocol(kvp.Key, kvp.Value,
                    new Dictionary<string, IPEndPoint>(_addresses.Where(inner => inner.Key != kvp.Key), StringComparer.OrdinalIgnoreCase));
                return (proto, node: new Node(kvp.Key, new GossipBackend(proto)));
            }).ToArray();

            _protocols = tuples.Select(t => t.proto).ToArray();
            _nodes = tuples.Select(t => t.node).ToArray();
        }

        [TestCleanup]
        public void Cleanup()
        {
            foreach (var proto in _protocols)
                proto.Dispose();
            _protocols = null;
            _nodes = null;
        }

        private void CheckProtocols()
        {
            foreach (var proto in _protocols)
                Assert.IsNull(proto.ListenerException);
        }

        [TestMethod]
        public void GossipBackendMultiple_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };

            _nodes[0].Run(() => _nodes[0].Set("key", testEntity)).Wait();

            Thread.Sleep(100);
            CheckProtocols();

            var read = _nodes[1].Run(() => _nodes[1].TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }
    }
}
