using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Shielded.Cluster.Tests
{
    [TestClass]
    public class BasicTests
    {
        private MockStrongBackend[] CreateCluster(int n)
        {
            return Enumerable.Repeat(0, n)
                .Select(i => new MockStrongBackend())
                .ToArray();
        }

        [TestMethod]
        public void TwoNodes_BasicSync()
        {
            var backends = CreateCluster(2);

            Distributed.Run(() =>
            {
                backends[0].Set("key1", "one");
            }).Wait();

            var read = Distributed.Run(() => backends[1].TryGet("key1", out string val) ? val : null).Result;

            Assert.AreEqual("one", read);
            Assert.IsTrue(MockStrongBackend.ConfirmCommits(1));
        }
    }
}
