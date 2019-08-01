using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ConsistentTests : GossipBackendThreeNodeTestsBase<ConsistentGossipBackend, TcpTransport>
    {
        public class TestClass
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Counter { get; set; }
        }

        protected override ConsistentGossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new ConsistentGossipBackend(transport, configuration);
        }

        protected override TcpTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers)
        {
            var transport = new TcpTransport(ownId, servers);
            transport.Error += OnListenerError;
            transport.StartListening();
            return transport;
        }

        [TestMethod]
        public void Consistent_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            Assert.IsTrue(_backends[A].RunConsistent(() =>
            {
                Assert.IsFalse(_backends[A].ContainsKey("key"));
                Assert.IsFalse(_backends[A].ContainsKeyWithInfo("key"));

                _backends[A].SetHasVec("key", testEntity.Version(A));

                Assert.IsTrue(_backends[A].ContainsKey("key"));
                Assert.IsTrue(_backends[A].ContainsKeyWithInfo("key"));
            }).Result);

            Assert.IsTrue(_backends[A].ContainsKey("key"));
            Assert.IsTrue(_backends[A].ContainsKeyWithInfo("key"));

            CheckProtocols();

            var (success, multi) = _backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<TestClass>("key"))
                .Result;

            Assert.IsTrue(success);
            var read = multi.Single();
            Assert.AreEqual(testEntity.Id, read.Value.Id);
            Assert.AreEqual(testEntity.Name, read.Value.Name);
            Assert.AreEqual((A, 1), read.Version);
        }

        [TestMethod]
        public void Consistent_AddAndRemove()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            Assert.IsTrue(_backends[A].RunConsistent(() =>
            {
                Assert.IsFalse(_backends[A].ContainsKey("key"));
                Assert.IsFalse(_backends[A].ContainsKeyWithInfo("key"));

                _backends[A].SetHasVec("key", testEntity.Version(A));

                Assert.IsTrue(_backends[A].ContainsKey("key"));
                Assert.IsTrue(_backends[A].ContainsKeyWithInfo("key"));

                _backends[A].Remove("key");

                Assert.IsFalse(_backends[A].ContainsKey("key"));
                Assert.IsTrue(_backends[A].ContainsKeyWithInfo("key"));
            }).Result);

            Assert.IsFalse(_backends[A].ContainsKey("key"));
            // no info either - the transaction never commits the add operation.
            Assert.IsFalse(_backends[A].ContainsKeyWithInfo("key"));

            CheckProtocols();

            var (success, multi) = _backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<TestClass>("key"))
                .Result;

            Assert.IsTrue(success);
            Assert.IsFalse(multi.Any());
        }

        [TestMethod]
        public void Consistent_SingleNode()
        {
            Shield.InTransaction(() =>
            {
                ((TcpTransport)_backends[A].Transport).ServerIPs.Clear();
                ((TcpTransport)_backends[B].Transport).ServerIPs.Remove(A);
                ((TcpTransport)_backends[C].Transport).ServerIPs.Remove(A);
            });

            var testEntity = new TestClass { Id = 1, Name = "One" };

            Assert.IsTrue(_backends[A].RunConsistent(() => { _backends[A].SetHasVec("key", testEntity.Version(A)); }).Result);

            CheckProtocols();

            var (success, multi) = _backends[A].RunConsistent(() => _backends[A].TryGetVecVersioned<TestClass>("key"))
                .Result;

            Assert.IsTrue(success);
            var read = multi.Single();
            Assert.AreEqual(testEntity.Id, read.Value.Id);
            Assert.AreEqual(testEntity.Name, read.Value.Name);
            Assert.AreEqual((A, 1), read.Version);

            (success, multi) = _backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<TestClass>("key"))
                .Result;
            Assert.IsTrue(success);
            Assert.IsFalse(multi.Any());
        }

        [TestMethod]
        public void Consistent_ConflictIsGreaterToo()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            Assert.IsTrue(_backends[A].RunConsistent(() => { _backends[A].SetHasVec("key", testEntity.Version(A)); }).Result);

            var testEntity2 = new TestClass { Id = 1, Name = "One, Bs version" };

            // even though this is a conflicting edit, the merged data - a Multiple which will contain both
            // versions of the data - gets actually saved and transmitted, and it is Greater than both versions.
            var (success, comp) = _backends[B].RunConsistent(() => _backends[B].SetHasVec("key", testEntity2.Version(B))).Result;

            CheckProtocols();

            Assert.IsTrue(success);
            Assert.AreEqual(VectorRelationship.Conflict, comp);

            var saved = _backends[B].TryGetVecVersioned<TestClass>("key");

            Assert.AreEqual(VectorRelationship.Greater, saved.VectorCompare(testEntity.Version(A)));
            Assert.AreEqual(VectorRelationship.Greater, saved.VectorCompare(testEntity2.Version(B)));
            Assert.AreEqual((VersionVector)(A, 1) | (B, 1), saved.MergedClock);
        }

        [TestMethod]
        public void Consistent_PrepareAndRollback()
        {
            var testEntity = new TestClass { Id = 1, Name = "One" };

            using (var cont = _backends[A].Prepare(() => { _backends[A].SetHasVec("key", testEntity.Version(A)); }).Result)
            {
                Assert.IsNotNull(cont);
                Assert.IsFalse(cont.Completed);

                Assert.IsTrue(cont.TryRollback());
                Assert.IsTrue(cont.Completed);
                Assert.IsFalse(cont.Committed);
            }

            var (success, multi) = _backends[A].RunConsistent(() => _backends[A].TryGetVecVersioned<TestClass>("key"))
                .Result;

            Assert.IsTrue(success);
            Assert.IsFalse(multi.Any());

            (success, multi) = _backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<TestClass>("key"))
                .Result;

            CheckProtocols();

            Assert.IsTrue(success);
            Assert.IsFalse(multi.Any());
        }

        [TestMethod]
        public void Consistent_Race()
        {
            const int transactions = 1000;
            const int fieldCount = 100;

            foreach (var back in _backends.Values)
            {
                back.Configuration.DirectMail = DirectMailType.StartGossip;
            }

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
            {
                var backend = _backends.Values.Skip(i % 3).First();
                var id = (i % fieldCount);
                var key = "key" + id;
                return backend.RunConsistent(() =>
                {
                    var newVal = backend.TryGetVecVersioned<TestClass>(key)
                        .SingleOrDefault()
                        .NextVersion(backend.Transport.OwnId);
                    if (newVal.Value == null)
                        newVal.Value = new TestClass { Id = id };
                    newVal.Value.Counter = newVal.Value.Counter + 1;
                    backend.SetHasVec(key, newVal);
                }, 100);
            })).Result;
            var expected = bools.Count(b => b);

            CheckProtocols();

            var read = _backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    _backends[B].TryGetVecVersioned<TestClass>("key" + i).SingleOrDefault().Value?.Counter)).Result;

            Assert.IsTrue(read.Success);
            Assert.AreEqual(expected, read.Value);
            Assert.AreEqual(transactions, read.Value);
        }

        [TestMethod]
        public void Consistent_Versioned()
        {
            var testEntity = new TestClass { Id = 1, Name = "New entity" };

            _backends[A].RunConsistent(() => { _backends[A].Set("key", testEntity.Version(1)); }).Wait();

            IntVersioned<TestClass> read = default, next = default;
            var (success, readName) = _backends[B].RunConsistent(() =>
            {
                read = _backends[B].TryGetIntVersioned<TestClass>("key");
                var res = read.Value?.Name;

                next = read.NextVersion();
                next.Value = new TestClass { Id = 1, Name = "Version 2" };
                _backends[B].Set("key", next);
                return res;
            }).Result;
            Assert.IsTrue(success);
            Assert.AreEqual(testEntity.Name, readName);

            read = _backends[C].RunConsistent(() => _backends[C].TryGetIntVersioned<TestClass>("key")).Result.Value;
            Assert.AreEqual(next.Value.Name, read.Value.Name);
        }
    }
}
