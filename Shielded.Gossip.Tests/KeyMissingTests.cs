using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class KeyMissingTests
    {
        private const string A = "A";

        [TestMethod]
        public void KeyMissing_Basics()
        {
            var currValue = "stored value".Version(7);
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 100,
            }))
            {
                void Provider(object sender, KeyMissingEventArgs args) => args.UseValue(currValue, expiresInMs: 100);

                Shield.InTransaction(() =>
                {
                    backend.KeyMissing.Subscribe(Provider);
                    backend.Changed.Subscribe((_, args) => Shield.SideEffect(() =>
                        currValue = (IntVersioned<string>)args.NewValue));
                });

                var resWithInfo = backend.TryGetWithInfo<IntVersioned<string>>("key");

                Assert.AreEqual("stored value", resWithInfo.Value.Value);
                Assert.AreEqual(7, resWithInfo.Value.Version);
                Assert.AreEqual(100, resWithInfo.ExpiresInMs);

                // enough time for a full clean-up of the key. since the provider is still subscribed, the
                // clean-up might accidentally refetch the key. this will test whether it does.
                Thread.Sleep(400);

                // confirm clean-up
                Shield.InTransaction(() => backend.KeyMissing.Unsubscribe(Provider));
                Assert.IsNull(backend.TryGetWithInfo<IntVersioned<string>>("key"));
                Shield.InTransaction(() => backend.KeyMissing.Subscribe(Provider));

                Assert.AreEqual(VectorRelationship.Less, backend.Set("key", "failed write".Version(2), 100));

                Thread.Sleep(50);

                resWithInfo = backend.TryGetWithInfo<IntVersioned<string>>("key");

                Assert.AreEqual("stored value", resWithInfo.Value.Value);
                Assert.AreEqual(7, resWithInfo.Value.Version);
                // it seems the Thread.Sleep above lasts slightly less than 50 ms? cause ExpiresInMs is sometimes still > 50...
                Assert.IsTrue(resWithInfo.ExpiresInMs <= 60);

                Assert.AreEqual(VectorRelationship.Greater, backend.Set("key", "successful write".Version(8), 100));

                Thread.Sleep(150);

                resWithInfo = backend.TryGetWithInfo<IntVersioned<string>>("key");

                Assert.AreEqual("successful write", resWithInfo.Value.Value);
                Assert.AreEqual(8, resWithInfo.Value.Version);
                Assert.AreEqual(100, resWithInfo.ExpiresInMs);
            }
        }

        [TestMethod]
        public void KeyMissing_Tombstone()
        {
            var currValue = "deleted value".Version(7);
            var isDeleted = true;
            var transport = new MockTransport(A, new string[0]);
            using (var backend = new GossipBackend(transport, new GossipConfiguration
            {
                CleanUpInterval = 100,
                RemovableItemLingerMs = 100,
            }))
            {
                void Provider(object sender, KeyMissingEventArgs args) => args.UseValue(currValue, isDeleted);

                Shield.InTransaction(() =>
                {
                    backend.KeyMissing.Subscribe(Provider);
                    backend.Changed.Subscribe((_, args) => Shield.SideEffect(() =>
                    {
                        currValue = (IntVersioned<string>)args.NewValue;
                        isDeleted = args.Deleted;
                    }));
                });

                Assert.AreEqual(VectorRelationship.Less, backend.Set("key", "failed write".Version(5)));

                var resWithInfo = backend.TryGetWithInfo<IntVersioned<string>>("key");

                Assert.AreEqual("deleted value", resWithInfo.Value.Value);
                Assert.AreEqual(7, resWithInfo.Value.Version);
                Assert.IsTrue(resWithInfo.Deleted);

                // enough time for a full clean-up of the key
                Thread.Sleep(400);

                // confirm clean-up
                Shield.InTransaction(() => backend.KeyMissing.Unsubscribe(Provider));
                Assert.IsNull(backend.TryGetWithInfo<IntVersioned<string>>("key"));
                Shield.InTransaction(() => backend.KeyMissing.Subscribe(Provider));

                // this will still fail, because of the KeyMissing event restoring the tombstone.
                Assert.AreEqual(VectorRelationship.Less, backend.Set("key", "failed write".Version(5)));

                // but this will work
                Assert.AreEqual(VectorRelationship.Greater, backend.Set("key", "successful write".Version(8)));

                resWithInfo = backend.TryGetWithInfo<IntVersioned<string>>("key");

                Assert.AreEqual("successful write", resWithInfo.Value.Value);
                Assert.AreEqual(8, resWithInfo.Value.Version);
            }
        }
    }
}
