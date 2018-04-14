using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ConsistentTests : GossipBackendThreeNodeTestsBase
    {
        [TestMethod]
        public void Consistent_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };

            Assert.IsTrue(Distributed.Consistent(() => { _backends[A].SetVersion("key", testEntity); }).Result);

            CheckProtocols();

            var read = Distributed.Consistent(() => _backends[B].TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Value.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }
    }
}
