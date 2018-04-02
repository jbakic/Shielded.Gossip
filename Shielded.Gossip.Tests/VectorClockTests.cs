using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Linq;

namespace Shielded.Gossip.Tests
{
    /// <summary>
    /// This basically tests <see cref="VectorBase{TVec, T}"/>.
    /// </summary>
    [TestClass]
    public class VectorClockTests
    {
        private const string A = "server A";
        private const string B = "server B";
        private const string C = "server C";

        [TestMethod]
        public void VectorClock_ConstructorAndEquality()
        {
            var items = new []
            {
                new VectorItem<int>(A, 4),
                new VectorItem<int>(B, 6),
            };

            var a = new VectorClock(items);

            Assert.IsTrue(a.Items.SequenceEqual(items));

            var b = new VectorClock(
                new VectorItem<int>(A, 4),
                new VectorItem<int>(B, 6));

            Assert.AreEqual(a, b);
        }

        [TestMethod]
        public void VectorClock_VectorCompareAndNext()
        {
            var a = new VectorClock(A, 4);

            Assert.AreEqual(VectorRelationship.Equal, a.VectorCompare(a));

            var b = new VectorClock(B, 6);

            Assert.AreEqual(VectorRelationship.Conflict, a.VectorCompare(b));
            Assert.AreEqual(VectorRelationship.Conflict, b.VectorCompare(a));

            var cNext = a.Next(C);

            Assert.AreEqual(VectorRelationship.Equal, cNext.VectorCompare(cNext));
            Assert.AreEqual(VectorRelationship.Greater, cNext.VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Conflict, cNext.VectorCompare(b));

            var aNext = (cNext | a).Next(A);
            var bNext = (cNext | b).Next(B);

            Assert.AreEqual(VectorRelationship.Greater, aNext.VectorCompare(cNext));
            Assert.AreEqual(VectorRelationship.Greater, aNext.VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Conflict, aNext.VectorCompare(b));
            Assert.AreEqual(VectorRelationship.Less, cNext.VectorCompare(aNext));
            Assert.AreEqual(VectorRelationship.Less, a.VectorCompare(aNext));
            Assert.AreEqual(VectorRelationship.Conflict, b.VectorCompare(aNext));

            Assert.AreEqual(VectorRelationship.Greater, bNext.VectorCompare(cNext));
            Assert.AreEqual(VectorRelationship.Greater, bNext.VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Greater, bNext.VectorCompare(b));
            Assert.AreEqual(VectorRelationship.Less, cNext.VectorCompare(bNext));
            Assert.AreEqual(VectorRelationship.Less, a.VectorCompare(bNext));
            Assert.AreEqual(VectorRelationship.Less, b.VectorCompare(bNext));
        }

        [TestMethod]
        public void VectorClock_MergeWith_DefaultIsZero()
        {
            Assert.AreEqual(VectorRelationship.Equal,
                (new VectorClock() | new VectorClock()).VectorCompare(new VectorClock()));

            Assert.AreEqual(VectorRelationship.Equal,
                ((VectorClock)null | null).VectorCompare(null));

            var a = (VectorClock)(A, 3);

            Assert.AreEqual(VectorRelationship.Equal, (new VectorClock() | a).VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Equal, (null | a).VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Equal, (a | new VectorClock()).VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Equal, (a | null).VectorCompare(a));
        }

        [TestMethod]
        public void VectorClock_MergeWith_Idempotent()
        {
            var a = (VectorClock)(A, 4);
            var b = (VectorClock)(A, 4);

            var id = a | b;

            Assert.AreEqual(VectorRelationship.Equal, id.VectorCompare(a));
            Assert.AreEqual(VectorRelationship.Equal, id.VectorCompare(b));
        }

        [TestMethod]
        public void VectorClock_MergeWith_Commutative()
        {
            var a = (VectorClock)(A, 4);
            var b = (VectorClock)(B, 6);

            var ab = a | b;
            var ba = b | a;

            Assert.AreEqual(VectorRelationship.Equal, ab.VectorCompare(ba));
        }

        [TestMethod]
        public void VectorClock_MergeWith_Associative()
        {
            var a = (VectorClock)(A, 4);
            var b = (VectorClock)(B, 6);
            var c = (VectorClock)(A, 2) | (C, 3);

            var aLast = a | (b | c);
            var cLast = (a | b) | c;

            Assert.AreEqual(VectorRelationship.Equal, aLast.VectorCompare(cLast));
        }

        [TestMethod]
        public void VectorClock_EqualityAndHashCode()
        {
            var a = (VectorClock)(A, 1);
            var b = (VectorClock)(B, 2);

            Assert.AreNotEqual(a, b);
            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());

            var merge = a | b;
            var manual = (VectorClock)(A, 1) | (B, 2);

            Assert.AreEqual(manual, merge);
            Assert.AreEqual(manual.GetHashCode(), merge.GetHashCode());

            var lower = (VectorClock)("name", 1);
            var upper = (VectorClock)("NAME", 1);

            Assert.AreEqual(lower, upper);
            Assert.AreEqual(lower.GetHashCode(), upper.GetHashCode());
        }

        [TestMethod]
        public void VectorClock_Modify()
        {
            Assert.ThrowsException<ArgumentNullException>(() => new VectorClock(null, 1));
            Assert.ThrowsException<ArgumentNullException>(() => (VectorClock)(null, 1));

            var a = (VectorClock)(A, 1);

            a = a.Modify(B, 2);

            Assert.AreEqual((VectorClock)(A, 1) | (B, 2), a);

            Assert.ThrowsException<ArgumentNullException>(() => a.Modify(null, 3));
            Assert.ThrowsException<ArgumentNullException>(() => a.Modify(" ", 3));
        }

        [TestMethod]
        public void VectorClock_Overflow()
        {
            var a = (VectorClock)(A, int.MaxValue);

            Assert.ThrowsException<OverflowException>(() => a.Next(A));
        }
    }
}
