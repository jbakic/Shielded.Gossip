using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Gossip.Backend;
using Shielded.Gossip.Mergeables;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class CleanUpTests
    {
        private const string A = "A";

        public class DummyDeletable : IDeletable
        {
            public bool CanDelete { get; set; }
        }

        [TestMethod]
        public void CleanUpNonDeletable()
        {
            // should not be cleaned up, of course.
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 200,
            }))
            {
                backend.Set("test", new DummyDeletable { CanDelete = false }.Version(1));

                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<DummyDeletable>>("test"));
                Thread.Sleep(350);
                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<DummyDeletable>>("test"));
            }
        }

        [TestMethod]
        public void CleanUpDeletable()
        {
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 200,
            }))
            {
                // must first set a non-deletable one, cause Set rejects deletable writes if it has no
                // existing value for that key.
                backend.Set("test", new DummyDeletable { CanDelete = false }.Version(1));
                Assert.AreEqual(VectorRelationship.Greater,
                    backend.Set("test", new DummyDeletable { CanDelete = true }.Version(2)));

                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<DummyDeletable>>("test"));
                Thread.Sleep(100);
                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<DummyDeletable>>("test"));
                Thread.Sleep(250);
                Assert.IsNull(backend.TryGetWithInfo<IntVersioned<DummyDeletable>>("test"));
            }
        }

        [TestMethod]
        public void CleanUpDeleted()
        {
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 200,
            }))
            {
                backend.Set("test", "something".Version(1));
                Assert.IsTrue(backend.Remove("test"));

                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(100);
                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(250);
                Assert.IsNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
            }
        }

        [TestMethod]
        public void CleanUpExpired()
        {
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 200,
            }))
            {
                backend.Set("test", "something".Version(1), 100);

                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(250);
                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(200);
                Assert.IsNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
            }
        }

        [TestMethod]
        public void CleanUpRevived()
        {
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 200,
            }))
            {
                backend.Set("test", "something".Version(1), 100);

                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(250);
                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                // we will revive it now
                Assert.AreEqual(VectorRelationship.Equal,
                    backend.Set("test", "something".Version(1), 100));

                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(250);
                Assert.IsNotNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
                Thread.Sleep(250);
                Assert.IsNull(backend.TryGetWithInfo<IntVersioned<string>>("test"));
            }
        }
    }
}
