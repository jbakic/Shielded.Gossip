using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class GossipBackendOneNodeTests
    {
        public class TestClass : IHasVectorClock, IDeletable
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public VectorClock Clock { get; set; }
            public bool CanDelete { get; set; }
        }

        private const string A = "A";
        private const string B = "B";

        private GossipBackend _backend;

        [TestInitialize]
        public void Init()
        {
            var transport = new TcpTransport("Node", new IPEndPoint(IPAddress.Loopback, 2001), new Dictionary<string, IPEndPoint>());
            transport.StartListening();
            _backend = new GossipBackend(transport, new GossipConfiguration { DeletableCleanUpInterval = 200 });
        }

        [TestCleanup]
        public void Cleanup()
        {
            _backend.Dispose();
            _backend = null;
        }

        [TestMethod]
        public void GossipBackend_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            Assert.AreEqual(VectorRelationship.Greater,
                Shield.InTransaction(() => _backend.SetVc("key", testEntity)));

            var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Single());

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Clock, read.Clock);
        }

        [TestMethod]
        public void GossipBackend_Merge()
        {
            var testEntity1 = new TestClass { Id = 1, Name = "One", Clock = (A, 2) };
            Assert.AreEqual(VectorRelationship.Greater,
                Shield.InTransaction(() => _backend.SetVc("key", testEntity1)));

            {
                var testEntity2Fail = new TestClass { Id = 2, Name = "Two", Clock = (A, 1) };
                Assert.AreEqual(VectorRelationship.Less,
                    Shield.InTransaction(() => _backend.SetVc("key", testEntity2Fail)));

                var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Single());

                Assert.AreEqual(testEntity1.Id, read.Id);
                Assert.AreEqual(testEntity1.Name, read.Name);
                Assert.AreEqual(testEntity1.Clock, read.Clock);
            }

            var testEntity2Succeed = new TestClass { Id = 2, Name = "Two", Clock = (VectorClock)(A, 2) | (B, 1) };
            Assert.AreEqual(VectorRelationship.Greater,
                Shield.InTransaction(() => _backend.SetVc("key", testEntity2Succeed)));

            {
                var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Single());

                Assert.AreEqual(testEntity2Succeed.Id, read.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read.Name);
                Assert.AreEqual(testEntity2Succeed.Clock, read.Clock);
            }

            {
                var testEntity2SameClock = new TestClass { Id = 1002, Name = "Another Two", Clock = testEntity2Succeed.Clock };
                Assert.AreEqual(VectorRelationship.Equal,
                    Shield.InTransaction(() => _backend.SetVc("key", testEntity2SameClock)));

                var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Single());

                // we keep the first entity we see...
                Assert.AreEqual(testEntity2Succeed.Id, read.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read.Name);
                Assert.AreEqual(testEntity2Succeed.Clock, read.Clock);
            }

            VectorClock mergedClock;

            {
                var testEntity3Conflict = new TestClass { Id = 3, Name = "Three", Clock = (A, 3) };
                Assert.AreEqual(VectorRelationship.Conflict,
                    Shield.InTransaction(() => _backend.SetVc("key", testEntity3Conflict)));

                var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key"));
                mergedClock = read.MergedClock;

                var read2 = read.Single(t => t.Id == 2);
                var read3 = read.Single(t => t.Id == 3);

                Assert.AreEqual(testEntity2Succeed.Id, read2.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read2.Name);
                Assert.AreEqual(testEntity2Succeed.Clock, read2.Clock);
                Assert.AreEqual(testEntity3Conflict.Id, read3.Id);
                Assert.AreEqual(testEntity3Conflict.Name, read3.Name);
                Assert.AreEqual(testEntity3Conflict.Clock, read3.Clock);

                Assert.AreEqual(testEntity2Succeed.Clock | testEntity3Conflict.Clock, mergedClock);
            }
            {
                var testEntity4Resolve = new TestClass { Id = 4, Name = "Four", Clock = mergedClock.Next(B) };
                Assert.AreEqual((VectorClock)(A, 3) | (B, 2), testEntity4Resolve.Clock);
                Assert.AreEqual(VectorRelationship.Greater,
                    Shield.InTransaction(() => _backend.SetVc("key", testEntity4Resolve)));

                var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Single());

                Assert.AreEqual(testEntity4Resolve.Id, read.Id);
                Assert.AreEqual(testEntity4Resolve.Name, read.Name);
                Assert.AreEqual(testEntity4Resolve.Clock, read.Clock);
            }
        }

        [TestMethod]
        public void GossipBackend_SetMultiple()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Clock = (A, 1) };
            var toSet = (Multiple<TestClass>)
                new TestClass { Id = 2, Name = "Two", Clock = (A, 2) } |
                new TestClass { Id = 3, Name = "Three", Clock = (VectorClock)(A, 1) | (B, 1) };

            Shield.InTransaction(() =>
            {
                _backend.SetVc("key", testEntity);
                _backend.Set("key", toSet);
            });

            var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key"));

            var read2 = read.Single(t => t.Id == 2);
            var read3 = read.Single(t => t.Id == 3);

            Assert.AreEqual((VectorClock)(A, 2) | (B, 1), read.MergedClock);
        }

        [TestMethod]
        public void GossipBackend_MultipleDeletable()
        {
            var testEntity = new TestClass { Id = 1, Name = "New entity", Clock = (A, 1) };

            Shield.InTransaction(() => { _backend.SetVc("key", testEntity); });

            // clean-up is every 200 ms, so this is enough.
            Thread.Sleep(500);

            var read = Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Single());
            Assert.AreEqual(testEntity.Id, read.Id);

            testEntity.CanDelete = true;
            testEntity.Clock = testEntity.Clock.Next(_backend.Transport.OwnId);
            Shield.InTransaction(() => { _backend.SetVc("key", testEntity); });

            Thread.Sleep(500);

            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGet("key", out Multiple<TestClass> _)));
            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGetMultiple<TestClass>("key").Any()));
        }

        [TestMethod]
        public void GossipBackend_LastWriteWins()
        {
            var testEntity = new TestClass { Id = 1, Name = "New entity" };

            Shield.InTransaction(() => { _backend.Set("key", testEntity.Lww()); });

            // also checking IDeletable impl...
            Thread.Sleep(500);

            var read = Shield.InTransaction(() => _backend.TryGetLww<TestClass>("key"));
            Assert.AreEqual(testEntity.Name, read.Value.Name);

            var next = read.NextVersion();
            next.Value = new TestClass { Id = 1, Name = "Version 2" };
            Shield.InTransaction(() => { _backend.Set("key", next); });

            read = Shield.InTransaction(() => _backend.TryGetLww<TestClass>("key"));
            Assert.AreEqual(next.Value.Name, read.Value.Name);

            next = read.NextVersion();
            next.Value.CanDelete = true;
            Shield.InTransaction(() => { _backend.Set("key", next); });

            Thread.Sleep(500);

            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGet("key", out Lww<TestClass> _)));
            read = Shield.InTransaction(() => _backend.TryGetLww<TestClass>("key"));
            Assert.IsNull(read.Value);
            Assert.AreEqual(DateTimeOffset.MinValue, read.Time);
        }

        [TestMethod]
        public void GossipBackend_Versioned()
        {
            // Versioned is not safe to use with a non-consistent backend. we'll just test the basics.
            var testEntity = new TestClass { Id = 1, Name = "New entity" };

            Shield.InTransaction(() => { _backend.Set("key", testEntity.Version()); });

            // also checking IDeletable impl...
            Thread.Sleep(500);

            var read = Shield.InTransaction(() => _backend.TryGetVersioned<TestClass>("key"));
            Assert.AreEqual(testEntity.Name, read.Value.Name);

            var next = read.NextVersion();
            next.Value = new TestClass { Id = 1, Name = "Version 2" };
            Shield.InTransaction(() => { _backend.Set("key", next); });

            read = Shield.InTransaction(() => _backend.TryGetVersioned<TestClass>("key"));
            Assert.AreEqual(next.Value.Name, read.Value.Name);

            next = read.NextVersion();
            next.Value.CanDelete = true;
            Shield.InTransaction(() => { _backend.Set("key", next); });

            Thread.Sleep(500);

            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGet("key", out Versioned<TestClass> _)));
            read = Shield.InTransaction(() => _backend.TryGetVersioned<TestClass>("key"));
            Assert.IsNull(read.Value);
            Assert.AreEqual(0, read.Version);
        }
    }
}
