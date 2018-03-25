using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class MultiVersionTests
    {
        public class TestClass : IHasVectorClock<TestClass>
        {
            public VectorClock Clock { get; set; }

            public MultiVersion<TestClass> MergeWith(TestClass other) => this.DefaultMerge(other);
        }

        private const string A = "A";
        private const string B = "B";

        [TestMethod]
        public void MultiVersion_CastAndMerge()
        {
            var mvA1 = new TestClass { Clock = new VectorClock(A, 1) };
            var mvB1 = new TestClass { Clock = new VectorClock(B, 1) };

            var merge1 = (MultiVersion<TestClass>)mvA1 | mvB1;

            Assert.AreEqual(2, merge1.Versions.Length);
            Assert.AreEqual(
                new VectorClock(
                    new VectorItem<int>(A, 1),
                    new VectorItem<int>(B, 1)),
                merge1.MergedClock);
            Assert.AreNotEqual(merge1.MergedClock, merge1.Versions[0].Clock);
            Assert.AreNotEqual(merge1.MergedClock, merge1.Versions[1].Clock);

            var merge2 = merge1 | new TestClass { Clock = mvA1.Clock.Next(A) };

            Assert.AreEqual(2, merge2.Versions.Length);
            Assert.AreEqual(
                new VectorClock(
                    new VectorItem<int>(A, 2),
                    new VectorItem<int>(B, 1)),
                merge2.MergedClock);

            var merge3 = merge2 | new TestClass { Clock = merge2.MergedClock.Next(B) };

            Assert.AreEqual(1, merge3.Versions.Length);
            Assert.AreEqual(
                new VectorClock(
                    new VectorItem<int>(A, 2),
                    new VectorItem<int>(B, 2)),
                merge3.MergedClock);
            Assert.AreEqual(merge3.MergedClock, merge3.Versions[0].Clock);
        }
    }
}
