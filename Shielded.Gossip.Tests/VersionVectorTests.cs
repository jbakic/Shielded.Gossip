using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq;

namespace Shielded.Gossip.Tests
{
    /// <summary>
    /// This basically tests <see cref="VectorBase{TVec, T}"/>.
    /// </summary>
    [TestClass]
    public class VersionVectorTests
    {
        private const string A = "server A";
        private const string B = "server B";
        private const string C = "server C";

        [TestMethod]
        public void VersionVector_Constructor()
        {
            var items = new []
            {
                new VectorItem<int>(A, 4),
                new VectorItem<int>(B, 6),
            };

            var a = new VersionVector(items);

            Assert.IsTrue(a.Items.SequenceEqual(items));
        }

        [TestMethod]
        public void VersionVector_CompareWithAndNext()
        {
            var a = new VersionVector(A, 4);

            Assert.AreEqual(VectorRelationship.Equal, a.CompareWith(a));

            var b = new VersionVector(B, 6);

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
        public void VersionVector_MergeWith_DefaultIsZero()
        {
            Assert.AreEqual(VectorRelationship.Equal,
                (new VersionVector() | new VersionVector()).CompareWith(new VersionVector()));

            Assert.AreEqual(VectorRelationship.Equal,
                ((VersionVector)null | null).CompareWith(null));

            var a = new VersionVector(A, 3);
            Assert.AreEqual(VectorRelationship.Equal, (new VersionVector() | a).CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, (null | a).CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, (a | new VersionVector()).CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, (a | null).CompareWith(a));
        }

        [TestMethod]
        public void VersionVector_MergeWith_Idempotent()
        {
            var a = new VersionVector(A, 4);
            var b = new VersionVector(A, 4);

            var id = a | b;

            Assert.AreEqual(VectorRelationship.Equal, id.CompareWith(a));
            Assert.AreEqual(VectorRelationship.Equal, id.CompareWith(b));
        }

        [TestMethod]
        public void VersionVector_MergeWith_Commutative()
        {
            var a = new VersionVector(A, 4);
            var b = new VersionVector(B, 6);

            var ab = a | b;
            var ba = b | a;

            Assert.AreEqual(VectorRelationship.Equal, ab.CompareWith(ba));
        }

        [TestMethod]
        public void VersionVector_MergeWith_Associative()
        {
            var a = new VersionVector(A, 4);
            var b = new VersionVector(B, 6);
            var c = new VersionVector(
                new VectorItem<int>(A, 2),
                new VectorItem<int>(C, 3));

            var aLast = a | (b | c);
            var cLast = (a | b) | c;

            Assert.AreEqual(VectorRelationship.Equal, aLast.CompareWith(cLast));
        }
    }
}
