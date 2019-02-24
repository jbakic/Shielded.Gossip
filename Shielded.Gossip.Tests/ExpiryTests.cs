﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
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

        protected override TcpTransport CreateTransport(string ownId, IPEndPoint localEndpoint,
            IEnumerable<KeyValuePair<string, IPEndPoint>> servers)
        {
            var transport = new TcpTransport(ownId, localEndpoint,
                new ShieldedDict<string, IPEndPoint>(servers, null, StringComparer.InvariantCultureIgnoreCase));
            transport.Error += OnListenerError;
            transport.StartListening();
            return transport;
        }

        [TestMethod]
        public void Expiry_Basics()
        {
            _backends[A].SetVc("test", true.Clock(A, 1), 100);

            Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);

            Thread.Sleep(50);
            Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_NoExpiryDuringSetTransaction()
        {
            Shield.InTransaction(() =>
            {
                _backends[A].SetVc("test", true.Clock(A, 1), 50);

                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);
            });
            Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_NoExpiryDuringReadTransaction()
        {
            _backends[A].SetVc("test", true.Clock(A, 1), 50);
            Shield.InTransaction(() =>
            {
                Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);

                // a Shielded transaction guarantees repeatable reads, and the expiry mechanism must respect that.
                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);
            });
            Assert.IsFalse(_backends[A].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public async Task Expiry_NoExpiryDuringConsistentPrepare()
        {
            using (var cont = await _backends[A].Prepare(() =>
            {
                _backends[A].SetVc("test", true.Clock(A, 1), 50);

                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);
            }))
            {
                Assert.IsNotNull(cont);

                // the expiry clock is not running until we commit.
                Thread.Sleep(70);
                cont.Commit();
            }
            Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_NoExpiryDuringConsistentTransaction()
        {
            _backends[A].RunConsistent(() =>
            {
                _backends[A].SetVc("test", true.Clock(A, 1), 50);

                Thread.Sleep(70);
                Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);
            }).Wait();
            Assert.IsTrue(_backends[A].TryGetClocked<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[A].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_CrossServerExpiry()
        {
            _backends[A].SetVc("test", true.Clock(A, 1), 100);

            // have to give direct mail some time
            Thread.Sleep(50);
            Assert.IsTrue(_backends[B].TryGetClocked<bool>("test").Single().Value);

            Thread.Sleep(70);
            Assert.IsFalse(_backends[B].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_CrossServerConsistentExpiry()
        {
            _backends[A].RunConsistent(() => _backends[A].SetVc("test", true.Clock(A, 1), 100)).Wait();

            Assert.IsTrue(_backends[B].RunConsistent(() => _backends[B].TryGetClocked<bool>("test"))
                // well, this is special.
                .Result.Value.Single().Value);

            Thread.Sleep(120);
            Assert.IsFalse(_backends[B].TryGetClocked<bool>("test").Any());
        }

        [TestMethod]
        public void Expiry_DeleteByExpiry()
        {
            _backends[A].RunConsistent(() => _backends[A].SetVc("test", true.Clock(A, 1))).Wait();

            Assert.IsTrue(_backends[B].RunConsistent(() => _backends[B].TryGetClocked<bool>("test"))
                .Result.Value.Single().Value);

            _backends[A].SetVc("test", true.Clock(A, 2), 1);

            Thread.Sleep(100);

            Assert.IsFalse(_backends[B].TryGetClocked<bool>("test").Any());
        }
    }
}
