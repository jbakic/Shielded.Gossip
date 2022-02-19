using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Gossip.Mergeables;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class CountVectorTests
    {
        private const string A = "server A";
        private const string B = "server B";

        [TestMethod]
        public void CountVector_Constructor()
        {
            var a = new CountVector(A, 20);

            Assert.AreEqual(20, a.Value);
            Assert.AreEqual(20L, a);
            Assert.AreEqual(20, (a | new CountVector()).Value);
        }

        [TestMethod]
        public void CountVector_MergeWithAndOps()
        {
            var a = (CountVector)(A, 10);
            var b = (CountVector)(B, 20);

            var res = a | b;

            Assert.AreEqual(30, res.Value);

            // this is like two servers independently adding and removing
            var a2 = a.Increment(A, 20);
            var b2 = b.Decrement(B, 10);

            Assert.AreEqual(30, a2.Value);
            Assert.AreEqual(10, b2.Value);

            var res2 = a2 | b2;

            Assert.AreEqual(40, res2.Value);
        }
    }
}
