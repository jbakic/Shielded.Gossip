using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class GossipBackendTests
    {
        public class TestClass : IHasVectorClock<TestClass>
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public VectorClock Clock { get; set; }

            public Multiple<TestClass> MergeWith(TestClass other) => this.DefaultMerge(other);
        }

        [TestMethod]
        public void GossipBackend_Basics()
        {
            var backend = new GossipBackend();
            var node = new Node("Node", backend);
            var testEntity = new TestClass { Id = 1, Name = "One" };

            node.Run(() => node.Set("key", testEntity)).Wait();

            var read = node.Run(() => node.TryGet("key", out TestClass res) ? res : null).Result;

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }
    }
}
