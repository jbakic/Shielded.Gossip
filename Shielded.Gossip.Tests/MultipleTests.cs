using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Gossip.Mergeables;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class MultipleTests
    {
        public class TestClass : IHasVersionVector
        {
            public VersionVector Version { get; set; }
        }

        private const string A = "A";
        private const string B = "B";

        [TestMethod]
        public void Multiple_CastAndMerge()
        {
            var mvA1 = new TestClass { Version = (A, 1) };
            var mvB1 = new TestClass { Version = (B, 1) };

            var merge1 = (Multiple<TestClass>)mvA1 | mvB1;

            Assert.AreEqual((VersionVector)(A, 1) | (B, 1), merge1.MergedClock);
            Assert.AreEqual(2, merge1.Count);
            Assert.AreNotEqual(merge1.MergedClock, merge1[0].Version);
            Assert.AreNotEqual(merge1.MergedClock, merge1[1].Version);

            var merge2 = merge1 | new TestClass { Version = mvA1.Version.Next(A) };

            Assert.AreEqual((VersionVector)(A, 2) | (B, 1), merge2.MergedClock);
            Assert.AreEqual(2, merge2.Count);

            var merge3 = merge2 | new TestClass { Version = merge2.MergedClock.Next(B) };

            Assert.AreEqual((VersionVector)(A, 2) | (B, 2), merge3.MergedClock);
            Assert.AreEqual(1, merge3.Count);
            Assert.AreEqual(merge3.MergedClock, merge3[0].Version);
        }

        [TestMethod]
        public void Multiple_NullCast()
        {
            var nullCast = (Multiple<TestClass>)null;

            Assert.IsNull(nullCast.Items);

            var empty = new Multiple<TestClass>();

            Assert.IsNotNull(empty.MergedClock);
            Assert.AreEqual(empty.MergedClock, (new Multiple<TestClass>() | nullCast).MergedClock);
        }
    }
}
