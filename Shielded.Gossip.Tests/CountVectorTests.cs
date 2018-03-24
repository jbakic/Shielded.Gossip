using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class CountVectorTests
    {
        private const string me = "server 1";
        private const string two = "server 2";

        [TestMethod]
        public void CountVector_Constructor()
        {
            var a = new CountVector(me, 20);

            Assert.AreEqual(20, a.Value);
            Assert.AreEqual(20, (long)a);
        }

        [TestMethod]
        public void CountVector_MergeWithAndOps()
        {
            var a = new CountVector(me, 10);
            var b = new CountVector(two, 20);

            var res = a | b;

            Assert.AreEqual(30, res.Value);

            // this is like two servers independently adding and removing
            var a2 = a.Increment(me, 20);
            var b2 = b.Decrement(two, 10);

            Assert.AreEqual(30, a2.Value);
            Assert.AreEqual(10, b2.Value);

            var res2 = a2 | b2;

            Assert.AreEqual(40, res2.Value);
        }
    }
}
