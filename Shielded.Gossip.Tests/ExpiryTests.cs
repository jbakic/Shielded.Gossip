using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class ExpiryTests : GossipBackendThreeNodeTestsBase<ConsistentGossipBackend, TcpTransport>
    {
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
        public void Expiry_Basics()
        {
            _backends[A].SetHasVec("test", true.Version(A, 1), 100);

            Assert.IsTrue(_backends[A].ContainsKey("test"));
            Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);

            Thread.Sleep(50);
            Assert.IsTrue(_backends[A].ContainsKey("test"));
            Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].ContainsKey("test"));
            Assert.IsFalse(_backends[A].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_NoExpiryDuringSetTransaction()
        {
            Shield.InTransaction(() =>
            {
                _backends[A].SetHasVec("test", true.Version(A, 1), 50);

                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);
            });
            Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_NoExpiryDuringReadTransaction()
        {
            _backends[A].SetHasVec("test", true.Version(A, 1), 50);
            Shield.InTransaction(() =>
            {
                Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);

                // a Shielded transaction guarantees repeatable reads, and the expiry mechanism must respect that.
                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);
            });
            Assert.IsFalse(_backends[A].TryGetVecVersioned<bool>("test").Any());
        }

        //[TestMethod]
        //public async Task Expiry_NoExpiryDuringConsistentPrepare()
        //{
        //    using (var cont = await _backends[A].Prepare(() =>
        //    {
        //        _backends[A].SetHasVec("test", true.Version(A, 1), 50);

        //        Thread.Sleep(70);
        //        Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);
        //    }))
        //    {
        //        Assert.IsNotNull(cont);

        //        // the expiry clock is not running until we commit.
        //        Thread.Sleep(70);
        //        cont.Commit();
        //    }
        //    Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);

        //    Thread.Sleep(70);
        //    Assert.IsFalse(_backends[A].TryGetVecVersioned<bool>("test").Any());
        //}

        [TestMethod]
        public void Expiry_NoExpiryDuringConsistentTransaction()
        {
            _backends[A].RunConsistent(() =>
            {
                _backends[A].SetHasVec("test", true.Version(A, 1), 50);

                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);
            }).Wait();
            Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_CrossServerExpiry()
        {
            _backends[A].SetHasVec("test", true.Version(A, 1), 100);

            // have to give direct mail some time
            Thread.Sleep(50);
            Assert.IsTrue(_backends[B].TryGetVecVersioned<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[B].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_CrossServerExpiryChange()
        {
            _backends[A].SetHasVec("test", true.Version(A, 1));

            Thread.Sleep(50);
            _backends[A].SetHasVec("test", true.Version(A, 1), 100);

            Thread.Sleep(50);
            Assert.IsTrue(_backends[B].TryGetVecVersioned<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[B].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_CrossServerConsistentExpiry()
        {
            _backends[A].RunConsistent(() => _backends[A].SetHasVec("test", true.Version(A, 1), 100)).Wait();

            Assert.IsTrue(_backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<bool>("test"))
                // well, this is special.
                .Result.Value.Single().Value);

            Thread.Sleep(120);
            Assert.IsFalse(_backends[B].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_CrossServerConsistentExpiryChange()
        {
            _backends[A].RunConsistent(() => _backends[A].SetHasVec("test", true.Version(A, 1))).Wait();

            Thread.Sleep(50);
            _backends[A].RunConsistent(() => _backends[A].SetHasVec("test", true.Version(A, 1), 100)).Wait();

            Thread.Sleep(50);
            Assert.IsTrue(_backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<bool>("test"))
                .Result.Value.Single().Value);

            // we can also extend the expiry by a consistent transaction. due to compare precision, we must use 200 now.
            _backends[A].RunConsistent(() => _backends[A].SetHasVec("test", true.Version(A, 1), 200)).Wait();

            Thread.Sleep(100);
            Assert.IsTrue(_backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<bool>("test"))
                .Result.Value.Single().Value);

            Thread.Sleep(150);
            Assert.IsFalse(_backends[B].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_DeleteByExpiry()
        {
            _backends[A].RunConsistent(() => _backends[A].SetHasVec("test", true.Version(A, 1))).Wait();

            Assert.IsTrue(_backends[B].RunConsistent(() => _backends[B].TryGetVecVersioned<bool>("test"))
                .Result.Value.Single().Value);

            _backends[A].SetHasVec("test", true.Version(A, 2), 1);

            Thread.Sleep(100);

            Assert.IsFalse(_backends[B].TryGetVecVersioned<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_ReviveSameVersion()
        {
            _backends[A].SetHasVec("test", true.Version(A, 1), 1);
            Thread.Sleep(20);
            Assert.IsFalse(_backends[A].TryGetVecVersioned<bool>("test").Any());

            Assert.AreEqual(VectorRelationship.Equal, _backends[A].SetHasVec("test", true.Version(A, 1), 100));

            Thread.Sleep(20);
            Assert.IsTrue(_backends[A].TryGetVecVersioned<bool>("test").Single().Value);
        }
    }
}
