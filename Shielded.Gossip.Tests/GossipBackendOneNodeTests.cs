using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class GossipBackendOneNodeTests
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

        private GossipProtocol _protocol;
        private Node _node;

        [TestInitialize]
        public void Init()
        {
            _protocol = new GossipProtocol("Node", new Dictionary<string, IPEndPoint>(), new IPEndPoint(IPAddress.Loopback, 2001));
            _node = new Node("Node", new GossipBackend(_protocol));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _protocol.Dispose();
            _protocol = null;
            _node = null;
        }

        [TestMethod]
        public void GossipBackend_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            _node.Run(() => _node.Set("key", testEntity)).Wait();

            var read = _node.Run(() => _node.TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }

        [TestMethod]
        public void GossipBackend_Merge()
        {
            var testEntity1 = new TestClass { Id = 1, Name = "One", Clock = (A, 2) };
            _node.Run(() => _node.Set("key", testEntity1)).Wait();

            {
                var testEntity2Fail = new TestClass { Id = 2, Name = "Two", Clock = (A, 1) };
                _node.Run(() => _node.Set("key", testEntity2Fail)).Wait();

                var read = _node.Run(() => _node.TryGet("key", out Multiple<TestClass> t) ? t : null)
                    .Result.Single();

                Assert.AreEqual(testEntity1.Id, read.Id);
                Assert.AreEqual(testEntity1.Name, read.Name);
                Assert.AreEqual(testEntity1.Clock, read.Clock);
            }

            var testEntity2Succeed = new TestClass { Id = 2, Name = "Two", Clock = (VectorClock)(A, 2) | (B, 1) };
            _node.Run(() => _node.Set("key", testEntity2Succeed)).Wait();

            {
                var read = _node.Run(() => _node.TryGet("key", out Multiple<TestClass> t) ? t : null)
                    .Result.Single();

                Assert.AreEqual(testEntity2Succeed.Id, read.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read.Name);
                Assert.AreEqual(testEntity2Succeed.Clock, read.Clock);
            }

            VectorClock mergedClock;

            {
                var testEntity3Conflict = new TestClass { Id = 3, Name = "Three", Clock = (A, 3) };
                _node.Run(() => _node.Set("key", testEntity3Conflict)).Wait();

                var read = _node.Run(() => _node.TryGet("key", out Multiple<TestClass> t) ? t : null).Result;
                mergedClock = read.MergedClock;

                var read2 = read.Single(t => t.Id == 2);
                var read3 = read.Single(t => t.Id == 3);

                Assert.AreEqual(testEntity2Succeed.Id, read2.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read2.Name);
                Assert.AreEqual(testEntity2Succeed.Clock, read2.Clock);
                Assert.AreEqual(testEntity3Conflict.Id, read3.Id);
                Assert.AreEqual(testEntity3Conflict.Name, read3.Name);
                Assert.AreEqual(testEntity3Conflict.Clock, read3.Clock);

                Assert.AreEqual(testEntity2Succeed.Clock | testEntity3Conflict.Clock, mergedClock);
            }
            {
                var testEntity4Resolve = new TestClass { Id = 4, Name = "Four", Clock = mergedClock.Next(B) };
                Assert.AreEqual((VectorClock)(A, 3) | (B, 2), testEntity4Resolve.Clock);
                _node.Run(() => _node.Set("key", testEntity4Resolve)).Wait();

                var read = _node.Run(() => _node.TryGet("key", out Multiple<TestClass> t) ? t : null)
                    .Result.Single();

                Assert.AreEqual(testEntity4Resolve.Id, read.Id);
                Assert.AreEqual(testEntity4Resolve.Name, read.Name);
                Assert.AreEqual(testEntity4Resolve.Clock, read.Clock);
            }
        }

        [TestMethod]
        public void GossipBackend_SetMultiple()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };
            var toSet = (Multiple<TestClass>)
                new TestClass { Id = 2, Name = "Two", Clock = (A, 2) } |
                new TestClass { Id = 3, Name = "Three", Clock = (VectorClock)(A, 1) | (B, 1) };

            _node.Run(() =>
            {
                _node.Set("key", testEntity);
                _node.Set("key", toSet);
            }).Wait();

            var read = _node.Run(() => _node.TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result;

            var read2 = read.Single(t => t.Id == 2);
            var read3 = read.Single(t => t.Id == 3);

            Assert.AreEqual((VectorClock)(A, 2) | (B, 1), read.MergedClock);
        }
    }
}
