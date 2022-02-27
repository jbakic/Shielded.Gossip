using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shielded.Gossip.Backend;
using Shielded.Gossip.Mergeables;
using Shielded.Gossip.Transport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    [TestClass]
    public class CausalityTests : GossipBackendThreeNodeTestsBase<GossipBackend, TcpTransport>
    {
        protected override GossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new GossipBackend(transport, new GossipConfiguration());
        }

        protected override TcpTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers)
        {
            var transport = new TcpTransport(ownId, servers);
            transport.Error += OnListenerError;
            transport.StartListening();
            return transport;
        }

        public class Reference
        {
            public string Key { get; set; }
            public VersionVector WitnessedVersion { get; set; }
        }

        [TestMethod]
        public void Causality_Race()
        {
            const int transactions = 10000;
            const int fieldCount = 500;
            int lastTick;

            EventHandler<ChangedEventArgs> handler = (sender, args) =>
            {
                var r = (Multiple<VecVersioned<Reference>>)args.NewValue;
                var backend = (GossipBackend)sender;
                CheckReferences(backend, r);

                lastTick = Environment.TickCount;
            };

            foreach (var back in _backends.Values)
            {
                back.Configuration.DirectMail = DirectMailType.StartGossip;
                Shield.InTransaction(() =>
                    back.Changed.Subscribe(handler));
            }

            var rnd = new Random();
            ParallelEnumerable.Range(1, transactions).ForAll(i =>
            {
                var backend = _backends.Values.Skip(i % 3).First();
                int num1, num2;
                lock (rnd)
                {
                    num1 = rnd.Next(fieldCount);
                    num2 = rnd.Next(fieldCount);
                    if (num2 == num1) num2 = (num2 + 1) % fieldCount;
                }
                var key1 = "key" + num1;
                var key2 = "key" + num2;
                Shield.InTransaction(() =>
                {
                    var val1 = backend.TryGetVecVersioned<Reference>(key1);
                    CheckReferences(backend, val1);
                    var val2 = backend.TryGetVecVersioned<Reference>(key2);
                    CheckReferences(backend, val2);
                    // we will write only in key2, and we will reference the version of key1 that we now saw.
                    backend.SetHasVec(key2, new Reference { Key = key1, WitnessedVersion = val1.MergedClock }.Version(val2.MergedClock.Next(backend.Transport.OwnId)));
                });
            });
            lastTick = Environment.TickCount;

            // wait until some time passes since the last Changed event.
            const int waitFor = 3000;
            do
            {
                Thread.Sleep(Math.Max(0, waitFor - (Environment.TickCount - lastTick)));
            } while (Environment.TickCount - lastTick < waitFor);

            OnMessage(null, "Done waiting.");
            CheckProtocols();

            foreach (var backend in _backends.Values)
                Shield.InTransaction(() =>
                {
                    foreach (var i in Enumerable.Range(0, fieldCount))
                    {
                        var val = backend.TryGetVecVersioned<Reference>("key" + i);
                        CheckReferences(backend, val);
                    }
                });

            Assert.IsTrue(true);
        }

        private static void CheckReferences(GossipBackend backend, Multiple<VecVersioned<Reference>> val)
        {
            foreach (var r in val)
            {
                var target = backend.TryGetVecVersioned<Reference>(r.Value.Key);
                // target's version must be >= to the version val witnessed when it was written.
                Assert.AreEqual(VectorRelationship.Greater, target.MergedClock.VectorCompare(r.Value.WitnessedVersion) | VectorRelationship.Greater);
            }
        }
    }
}
