using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Cluster;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ConsistentTests : GossipBackendThreeNodeTestsBase<ConsistentGossipBackend>
    {
        protected override ConsistentGossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new ConsistentGossipBackend(transport, configuration);
        }

        [TestMethod]
        public void Consistent_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };

            Assert.IsTrue(Distributed.Consistent(() => { _backends[A].SetVersion("key", testEntity); }).Result);

            // Consistent completes as soon as it sends command to commit, so we must wait a bit for others to finish up.
            // this should maybe be changed...
            Thread.Sleep(100);
            CheckProtocols();

            var read = Distributed.Consistent(() => _backends[B].TryGet("key", out Multiple<TestClass> res) ? res : null)
                .Result.Value.Single();

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }

        [TestMethod]
        public void Consistent_Race()
        {
            const int transactions = 100;
            const int fieldCount = 50;

            foreach (var back in _backends.Values)
            {
                back.Configuration.DirectMail = false;
                back.Configuration.ConsistentPrepareTimeoutRange = (1000, 2000);
            }

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
                Distributed.Consistent(10, () =>
                {
                    var backend = _backends.Values.Skip(i % 3).First();
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }))).Result;
            var expected = bools.Count(b => b);

            Thread.Sleep(300);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Distributed.Run(() =>
                    Enumerable.Range(0, fieldCount).Sum(i => _backends[B].TryGet("key" + i, out CountVector v) ? v.Value : 0))
                .Result;

            Assert.AreEqual(expected, read);
            Assert.AreEqual(transactions, read);
        }
    }
}
