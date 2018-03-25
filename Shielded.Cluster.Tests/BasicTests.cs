using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Shielded.Cluster.Tests
{
    [TestClass]
    public class BasicTests
    {
        private (Node[], MockStrongBackend) CreateCluster(int n)
        {
            var backend = new MockStrongBackend();
            return (
                Enumerable.Repeat(0, n)
                    .Select(i => new Node(i.ToString(), backend))
                    .ToArray(),
                backend);
        }

        [TestMethod]
        public void TwoNodes_BasicSync()
        {
            var (nodes, backend) = CreateCluster(2);

            nodes[0].Run(() =>
            {
                nodes[0].Set("key1", "one");
            }).Wait();

            var read = nodes[1].Run(() => nodes[1].TryGet("key1", out string val) ? val : null).Result;

            Assert.AreEqual("one", read);
            Assert.IsTrue(backend.ConfirmCommits(2));
        }
    }
}
