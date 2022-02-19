using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Gossip.Backend;
using Shielded.Gossip.Mergeables;
using Shielded.Gossip.Transport;
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
        public class TestClass : IHasVersionVector, IDeletable
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public VersionVector Version { get; set; }
            public bool CanDelete { get; set; }
        }

        private const string A = "A";
        private const string B = "B";

        private GossipBackend _backend;

        [TestInitialize]
        public void Init()
        {
            var transport = new TcpTransport("Node", new Dictionary<string, IPEndPoint> { { "Node", new IPEndPoint(IPAddress.Loopback, 2001) } });
            transport.StartListening();
            _backend = new GossipBackend(transport, new GossipConfiguration { CleanUpInterval = 100, RemovableItemLingerMs = 200 });
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

            Assert.IsFalse(_backend.ContainsKey("key"));
            Assert.IsFalse(_backend.ContainsKeyWithInfo("key"));
            Assert.IsFalse(_backend.Keys.Any());
            Assert.IsFalse(_backend.KeysWithInfo.Any());

            Assert.AreEqual(VectorRelationship.Greater,
                Shield.InTransaction(() => _backend.SetHasVec("key", testEntity)));

            Assert.IsTrue(_backend.ContainsKey("key"));
            Assert.IsTrue(_backend.ContainsKeyWithInfo("key"));
            Assert.IsTrue(_backend.Keys.SequenceEqual(new[] { "key" }));
            Assert.IsTrue(_backend.KeysWithInfo.SequenceEqual(new[] { "key" }));

            var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Single());

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Version, read.Version);
        }

        [TestMethod]
        public void GossipBackend_Merge()
        {
            var testEntity1 = new TestClass { Id = 1, Name = "One", Version = (A, 2) };
            Assert.AreEqual(VectorRelationship.Greater,
                Shield.InTransaction(() => _backend.SetHasVec("key", testEntity1)));

            {
                var testEntity2Fail = new TestClass { Id = 2, Name = "Two", Version = (A, 1) };
                Assert.AreEqual(VectorRelationship.Less,
                    Shield.InTransaction(() => _backend.SetHasVec("key", testEntity2Fail)));

                var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Single());

                Assert.AreEqual(testEntity1.Id, read.Id);
                Assert.AreEqual(testEntity1.Name, read.Name);
                Assert.AreEqual(testEntity1.Version, read.Version);
            }

            var testEntity2Succeed = new TestClass { Id = 2, Name = "Two", Version = (VersionVector)(A, 2) | (B, 1) };
            Assert.AreEqual(VectorRelationship.Greater,
                Shield.InTransaction(() => _backend.SetHasVec("key", testEntity2Succeed)));

            {
                var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Single());

                Assert.AreEqual(testEntity2Succeed.Id, read.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read.Name);
                Assert.AreEqual(testEntity2Succeed.Version, read.Version);
            }

            {
                var testEntity2SameClock = new TestClass { Id = 1002, Name = "Another Two", Version = testEntity2Succeed.Version };
                Assert.AreEqual(VectorRelationship.Equal,
                    Shield.InTransaction(() => _backend.SetHasVec("key", testEntity2SameClock)));

                var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Single());

                // we keep the first entity we see...
                Assert.AreEqual(testEntity2Succeed.Id, read.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read.Name);
                Assert.AreEqual(testEntity2Succeed.Version, read.Version);
            }

            VersionVector mergedClock;

            {
                var testEntity3Conflict = new TestClass { Id = 3, Name = "Three", Version = (A, 3) };
                Assert.AreEqual(VectorRelationship.Conflict,
                    Shield.InTransaction(() => _backend.SetHasVec("key", testEntity3Conflict)));

                var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key"));
                mergedClock = read.MergedClock;

                var read2 = read.Single(t => t.Id == 2);
                var read3 = read.Single(t => t.Id == 3);

                Assert.AreEqual(testEntity2Succeed.Id, read2.Id);
                Assert.AreEqual(testEntity2Succeed.Name, read2.Name);
                Assert.AreEqual(testEntity2Succeed.Version, read2.Version);
                Assert.AreEqual(testEntity3Conflict.Id, read3.Id);
                Assert.AreEqual(testEntity3Conflict.Name, read3.Name);
                Assert.AreEqual(testEntity3Conflict.Version, read3.Version);

                Assert.AreEqual(testEntity2Succeed.Version | testEntity3Conflict.Version, mergedClock);
            }
            {
                var testEntity4Resolve = new TestClass { Id = 4, Name = "Four", Version = mergedClock.Next(B) };
                Assert.AreEqual((VersionVector)(A, 3) | (B, 2), testEntity4Resolve.Version);
                Assert.AreEqual(VectorRelationship.Greater,
                    Shield.InTransaction(() => _backend.SetHasVec("key", testEntity4Resolve)));

                var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Single());

                Assert.AreEqual(testEntity4Resolve.Id, read.Id);
                Assert.AreEqual(testEntity4Resolve.Name, read.Name);
                Assert.AreEqual(testEntity4Resolve.Version, read.Version);
            }
        }

        [TestMethod]
        public void GossipBackend_SetMultiple()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Version = (A, 1) };
            var toSet = (Multiple<TestClass>)
                new TestClass { Id = 2, Name = "Two", Version = (A, 2) } |
                new TestClass { Id = 3, Name = "Three", Version = (VersionVector)(A, 1) | (B, 1) };

            Shield.InTransaction(() =>
            {
                _backend.SetHasVec("key", testEntity);
                _backend.Set("key", toSet);
            });

            var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key"));

            var read2 = read.Single(t => t.Id == 2);
            var read3 = read.Single(t => t.Id == 3);

            Assert.AreEqual((VersionVector)(A, 2) | (B, 1), read.MergedClock);
        }

        [TestMethod]
        public void GossipBackend_MultipleDeletable()
        {
            var testEntity = new TestClass { Id = 1, Name = "New entity", Version = (A, 1) };

            Shield.InTransaction(() => { _backend.SetHasVec("key", testEntity); });

            // linger interval is 200 ms, so this is enough.
            Thread.Sleep(400);

            Assert.IsTrue(_backend.ContainsKey("key"));
            Assert.IsTrue(_backend.Keys.SequenceEqual(new[] { "key" }));
            var read = Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Single());
            Assert.AreEqual(testEntity.Id, read.Id);

            testEntity.CanDelete = true;
            testEntity.Version = testEntity.Version.Next(_backend.Transport.OwnId);
            Shield.InTransaction(() => { _backend.SetHasVec("key", testEntity); });

            Assert.IsFalse(_backend.ContainsKey("key"));
            Assert.IsTrue(_backend.ContainsKeyWithInfo("key"));
            Assert.IsFalse(_backend.Keys.Any());
            Assert.IsTrue(_backend.KeysWithInfo.SequenceEqual(new[] { "key" }));

            Thread.Sleep(400);

            Assert.IsFalse(_backend.ContainsKey("key"));
            Assert.IsFalse(_backend.ContainsKeyWithInfo("key"));
            Assert.IsFalse(_backend.Keys.Any());
            Assert.IsFalse(_backend.KeysWithInfo.Any());
            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGet("key", out Multiple<TestClass> _)));
            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGetHasVec<TestClass>("key").Any()));
        }

        [TestMethod]
        public void GossipBackend_LastWriteWins()
        {
            var testEntity = new TestClass { Id = 1, Name = "New entity" };

            Shield.InTransaction(() => { _backend.Set("key", testEntity.Lww()); });

            // also checking IDeletable impl...
            Thread.Sleep(300);

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

            Thread.Sleep(300);

            Assert.IsFalse(_backend.ContainsKeyWithInfo("key"));
            Assert.IsFalse(_backend.KeysWithInfo.Any());
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

            Shield.InTransaction(() => { _backend.Set("key", testEntity.Version(1)); });

            // also checking IDeletable impl...
            Thread.Sleep(300);

            var read = Shield.InTransaction(() => _backend.TryGetIntVersioned<TestClass>("key"));
            Assert.AreEqual(testEntity.Name, read.Value.Name);

            var next = read.NextVersion();
            next.Value = new TestClass { Id = 1, Name = "Version 2" };
            Shield.InTransaction(() => { _backend.Set("key", next); });

            read = Shield.InTransaction(() => _backend.TryGetIntVersioned<TestClass>("key"));
            Assert.AreEqual(next.Value.Name, read.Value.Name);

            next = read.NextVersion();
            next.Value.CanDelete = true;
            Shield.InTransaction(() => { _backend.Set("key", next); });

            Thread.Sleep(400);

            Assert.IsFalse(_backend.ContainsKeyWithInfo("key"));
            Assert.IsFalse(_backend.KeysWithInfo.Any());
            Assert.IsFalse(Shield.InTransaction(() => _backend.TryGet("key", out IntVersioned<TestClass> _)));
            read = Shield.InTransaction(() => _backend.TryGetIntVersioned<TestClass>("key"));
            Assert.IsNull(read.Value);
            Assert.AreEqual(0, read.Version);
        }
    }
}
