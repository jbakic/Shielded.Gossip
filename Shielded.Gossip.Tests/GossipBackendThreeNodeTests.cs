using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using System;
using System.Collections.Concurrent;
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

        private IDictionary<string, GossipBackend> _backends;

        [TestInitialize]
        public void Init()
        {
            _backends = _addresses.Select(kvp =>
            {
                var back = new GossipBackend(
                    new TcpTransport(kvp.Key, kvp.Value,
                        new Dictionary<string, IPEndPoint>(_addresses.Where(inner => inner.Key != kvp.Key), StringComparer.OrdinalIgnoreCase)),
                    new GossipConfiguration
                    {
                        GossipInterval = 250,
                    });
                back.Transport.Error += OnListenerError;
                return back;
            }).ToDictionary(b => b.Transport.OwnId, StringComparer.OrdinalIgnoreCase);
        }

        [TestCleanup]
        public void Cleanup()
        {
            foreach (var back in _backends.Values)
                back.Dispose();
            _backends = null;
            _listenerExceptions.Clear();
        }

        private ConcurrentQueue<Exception> _listenerExceptions = new ConcurrentQueue<Exception>();

        private void OnListenerError(object sender, Exception ex)
        {
            _listenerExceptions.Enqueue(ex);
        }

        private void CheckProtocols()
        {
            Assert.IsTrue(_listenerExceptions.IsEmpty);
        }

        [TestMethod]
        public void GossipBackendMultiple_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };

            Distributed.Run(() => _backends[A].SetVersion("key", testEntity)).Wait();

            Thread.Sleep(100);
            CheckProtocols();

            var read = Distributed.Run(() => _backends[B].TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }

        [TestMethod]
        public void GossipBackendMultiple_SeriallyConnected()
        {
            ((TcpTransport)_backends[A].Transport).ServerIPs.Remove(C);
            ((TcpTransport)_backends[C].Transport).ServerIPs.Remove(A);

            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };

            Distributed.Run(() => _backends[A].SetVersion("key", testEntity)).Wait();

            Thread.Sleep(500);
            CheckProtocols();

            var read = Distributed.Run(() => _backends[C].TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }
    }
}
