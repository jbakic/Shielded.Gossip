using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class MultipleTests
    {
        public class TestClass : IHasVectorClock
        {
            public VectorClock Clock { get; set; }
        }

        private const string A = "A";
        private const string B = "B";

        [TestMethod]
        public void Multiple_CastAndMerge()
        {
            var mvA1 = new TestClass { Clock = (A, 1) };
            var mvB1 = new TestClass { Clock = (B, 1) };

            var merge1 = (Multiple<TestClass>)mvA1 | mvB1;

            Assert.AreEqual((VectorClock)(A, 1) | (B, 1), merge1.MergedClock);
            Assert.AreEqual(2, merge1.Versions.Length);
            Assert.AreNotEqual(merge1.MergedClock, merge1.Versions[0].Clock);
            Assert.AreNotEqual(merge1.MergedClock, merge1.Versions[1].Clock);

            var merge2 = merge1 | new TestClass { Clock = mvA1.Clock.Next(A) };

            Assert.AreEqual((VectorClock)(A, 2) | (B, 1), merge2.MergedClock);
            Assert.AreEqual(2, merge2.Versions.Length);

            var merge3 = merge2 | new TestClass { Clock = merge2.MergedClock.Next(B) };

            Assert.AreEqual((VectorClock)(A, 2) | (B, 2), merge3.MergedClock);
            Assert.AreEqual(1, merge3.Versions.Length);
            Assert.AreEqual(merge3.MergedClock, merge3.Versions[0].Clock);
        }

        [TestMethod]
        public void Multiple_NullCast()
        {
            var nullCast = (Multiple<TestClass>)null;

            Assert.IsNull(nullCast.Versions);

            var empty = new Multiple<TestClass>();

            Assert.IsNotNull(empty.MergedClock);
            Assert.AreEqual(empty.MergedClock, (new Multiple<TestClass>() | nullCast).MergedClock);
        }
    }
}
