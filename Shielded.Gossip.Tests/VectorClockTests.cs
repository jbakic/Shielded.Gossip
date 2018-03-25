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
        public void VectorClock_CompareWithAndNext()
        {
            var a = new VectorClock(A, 4);

            Assert.AreEqual(VectorRelationship.Equal, a.CompareWith(a));

            var b = new VectorClock(B, 6);

            Assert.AreEqual(VectorRelationship.Conflict, a.CompareWith(b));
            Assert.AreEqual(VectorRelationship.Conflict, b.CompareWith(a));

            var cNext = a.Next(C);

            Assert.AreEqual(VectorRelationship.Equal, cNext.CompareWith(cNext));
            Assert.AreEqual(VectorRelationship.Greater, cNext.CompareWith(a));
            Assert.AreEqual(VectorRelationship.Conflict, cNext.CompareWith(b));

            var aNext = (cNext | a).Next(A);
            var bNext = (cNext | b).Next(B);

            Assert.AreEqual(VectorRelationship.Greater, aNext.CompareWith(cNext));
            Assert.AreEqual(VectorRelationship.Greater, aNext.CompareWith(a));
            Assert.AreEqual(VectorRelationship.Conflict, aNext.CompareWith(b));
            Assert.AreEqual(VectorRelationship.Less, cNext.CompareWith(aNext));
            Assert.AreEqual(VectorRelationship.Less, a.CompareWith(aNext));
            Assert.AreEqual(VectorRelationship.Conflict, b.CompareWith(aNext));

            Assert.AreEqual(VectorRelationship.Greater, bNext.CompareWith(cNext));
            Assert.AreEqual(VectorRelationship.Greater, bNext.CompareWith(a));
            Assert.AreEqual(VectorRelationship.Greater, bNext.CompareWith(b));
            Assert.AreEqual(VectorRelationship.Less, cNext.CompareWith(bNext));
            Assert.AreEqual(VectorRelationship.Less, a.CompareWith(bNext));
            Assert.AreEqual(VectorRelationship.Less, b.CompareWith(bNext));
        }

        [TestMethod]
        public void VectorClock_MergeWith_DefaultIsZero()
        {
            Assert.AreEqual(VectorRelationship.Equal,
                (new VectorClock() | new VectorClock()).CompareWith(new VectorClock()));

            Assert.AreEqual(VectorRelationship.Equal,
                ((VectorClock)null | null).CompareWith(null));

            var a = new VectorClock(A, 3);
            Assert.AreEqual(VectorRelationship.Equal, (new VectorClock() | a).CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, (null | a).CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, (a | new VectorClock()).CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, (a | null).CompareWith(a));
        }

        [TestMethod]
        public void VectorClock_MergeWith_Idempotent()
        {
            var a = new VectorClock(A, 4);
            var b = new VectorClock(A, 4);

            var id = a | b;

            Assert.AreEqual(VectorRelationship.Equal, id.CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, id.CompareWith(b));
        }

        [TestMethod]
        public void VectorClock_MergeWith_Commutative()
        {
            var a = new VectorClock(A, 4);
            var b = new VectorClock(B, 6);

            var ab = a | b;
            var ba = b | a;

            Assert.AreEqual(VectorRelationship.Equal, ab.CompareWith(ba));
        }

        [TestMethod]
        public void VectorClock_MergeWith_Associative()
        {
            var a = new VectorClock(A, 4);
            var b = new VectorClock(B, 6);
            var c = new VectorClock(
                new VectorItem<int>(A, 2),
                new VectorItem<int>(C, 3));

            var aLast = a | (b | c);
            var cLast = (a | b) | c;

            Assert.AreEqual(VectorRelationship.Equal, aLast.CompareWith(cLast));
        }

        [TestMethod]
        public void VectorClock_EqualityAndHashCode()
        {
            var a = new VectorClock(A, 1);
            var b = new VectorClock(B, 2);

            Assert.AreNotEqual(a, b);
            Assert.AreNotEqual(a.GetHashCode(), b.GetHashCode());

            var merge = a | b;
            var manual = new VectorClock(
                new VectorItem<int>(A, 1),
                new VectorItem<int>(B, 2));

            Assert.AreEqual(manual, merge);
            Assert.AreEqual(manual.GetHashCode(), merge.GetHashCode());
        }

        [TestMethod]
        public void VectorClock_Modify()
        {
            Assert.ThrowsException<ArgumentNullException>(() => new VectorClock(null, 1));

            var a = new VectorClock(A, 1);

            a = a.Modify(B, 2);

            Assert.AreEqual(new VectorClock(A, 1) | new VectorClock(B, 2), a);

            Assert.ThrowsException<ArgumentNullException>(() =>
                a.Modify(null, 3));
            Assert.ThrowsException<ArgumentNullException>(() =>
                a.Modify(" ", 3));
        }
    }
}
