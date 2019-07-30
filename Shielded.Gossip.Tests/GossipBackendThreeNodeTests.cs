using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Tests
{
    public abstract class GossipBackendThreeNodeTestsBase<TBackend, TTransport>
        where TBackend : IGossipBackend, IDisposable
        where TTransport : ITransport
    {
        protected const string A = "A";
        protected const string B = "B";
        protected const string C = "C";

        protected static readonly Dictionary<string, IPEndPoint> _addresses = new Dictionary<string, IPEndPoint>(StringComparer.InvariantCultureIgnoreCase)
        {
            { A, new IPEndPoint(IPAddress.Loopback, 2001) },
            { B, new IPEndPoint(IPAddress.Loopback, 2002) },
            { C, new IPEndPoint(IPAddress.Loopback, 2003) },
        };

        protected IDictionary<string, TBackend> _backends;

        protected abstract TBackend CreateBackend(ITransport transport, GossipConfiguration configuration);
        protected abstract TTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers);

        [TestInitialize]
        public void Init()
        {
            RandomizePorts();
            _backends = new Dictionary<string, TBackend>(_addresses.Select(kvp =>
            {
                var transport = CreateTransport(kvp.Key, _addresses);
                var backend = CreateBackend(transport, new GossipConfiguration
                {
                    GossipInterval = 250,
                    AntiEntropyIdleTimeout = 2000,
                });

                var backendHandler = transport.MessageHandler;
                transport.MessageHandler = msg =>
                {
                    OnMessage(kvp.Key, msg);
                    return backendHandler(msg);
                };

                return new KeyValuePair<string, TBackend>(kvp.Key, backend);
            }), StringComparer.InvariantCultureIgnoreCase);
        }

        private void RandomizePorts()
        {
            var rnd = new Random();
            var used = new HashSet<int>();
            foreach (var server in _addresses.Keys.ToArray())
            {
                int port;
                do
                {
                    port = 2000 + rnd.Next(1000);
                } while (!used.Add(port));
                _addresses[server] = new IPEndPoint(IPAddress.Loopback, port);
            }
        }

        [TestCleanup]
        public void Cleanup()
        {
            foreach (var back in _backends.Values)
                back.Dispose();
            _backends = null;
            _listenerExceptions.Clear();
        }

        protected ConcurrentQueue<Exception> _listenerExceptions = new ConcurrentQueue<Exception>();

        protected void OnListenerError(object sender, Exception ex)
        {
            _listenerExceptions.Enqueue(ex);
        }

        private ConcurrentQueue<(string, string, object)> _messages = new ConcurrentQueue<(string, string, object)>();

        protected void OnMessage(string server, object msg)
        {
#if DEBUG
            if (msg is DirectMail dm && dm.Items != null && dm.Items.Length == 1)
                _messages.Enqueue((DateTime.Now.ToString("hh:mm:ss.fff"), server, dm.Items[0]));
            else
                _messages.Enqueue((DateTime.Now.ToString("hh:mm:ss.fff"), server, msg));
#endif
        }

        protected void CheckProtocols()
        {
            if (!_listenerExceptions.IsEmpty)
                throw new AggregateException(_listenerExceptions);
        }
    }

    [TestClass]
    public class GossipBackendThreeNodeTests : GossipBackendThreeNodeTestsBase<GossipBackend, TcpTransport>
    {
        public class TestClass : IHasVersionVector
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public VersionVector Version { get; set; }
        }

        protected override GossipBackend CreateBackend(ITransport transport, GossipConfiguration configuration)
        {
            return new GossipBackend(transport, configuration);
        }

        protected override TcpTransport CreateTransport(string ownId, IDictionary<string, IPEndPoint> servers)
        {
            var transport = new TcpTransport(ownId, servers);
            transport.Error += OnListenerError;
            transport.StartListening();
            return transport;
        }

        [TestMethod]
        public void GossipBackendMultiple_Basics()
        {
            var testEntity = new TestClass { Id = 1, Name = "One", Version = (A, 1) };

            Shield.InTransaction(() => _backends[A].SetHasVec("key", testEntity));

            Thread.Sleep(100);
            CheckProtocols();

            var read = Shield.InTransaction(() => _backends[B].TryGetHasVec<TestClass>("key").Single());

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Version, read.Version);
        }

        [TestMethod]
        public void GossipBackendMultiple_Race()
        {
            const int transactions = 1000;
            const int fieldCount = 50;

            foreach (var back in _backends.Values)
                back.Configuration.DirectMail = DirectMailType.StartGossip;

            Task.WaitAll(Enumerable.Range(1, transactions).Select(i =>
                Task.Run(() => Shield.InTransaction(() =>
                {
                    var backend = _backends.Values.Skip(i % 3).First();
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }))).ToArray());

            Thread.Sleep(1000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Shield.InTransaction(() =>
                Enumerable.Range(0, fieldCount).Sum(i => _backends[B].TryGet("key" + i, out CountVector v) ? v.Value : 0));

            Assert.AreEqual(transactions, read);
        }

        [TestMethod]
        public void GossipBackendMultiple_SeriallyConnected()
        {
            Shield.InTransaction(() =>
            {
                ((TcpTransport)_backends[A].Transport).ServerIPs.Remove(C);
                ((TcpTransport)_backends[C].Transport).ServerIPs.Remove(A);
            });

            var testEntity = new TestClass { Id = 1, Name = "One", Version = (A, 1) };

            Shield.InTransaction(() => _backends[A].SetHasVec("key", testEntity));

            Thread.Sleep(500);
            CheckProtocols();

            var read = Shield.InTransaction(() => _backends[C].TryGetHasVec<TestClass>("key").Single());

            Assert.AreEqual(testEntity.Id, read.Id);
            Assert.AreEqual(testEntity.Name, read.Name);
            Assert.AreEqual(testEntity.Version, read.Version);
        }

        [TestMethod]
        public void GossipBackendMultiple_RaceSeriallyConnected()
        {
            const int transactions = 1000;
            const int fieldCount = 50;

            Shield.InTransaction(() =>
            {
                ((TcpTransport)_backends[A].Transport).ServerIPs.Remove(C);
                ((TcpTransport)_backends[C].Transport).ServerIPs.Remove(A);
            });
            foreach (var back in _backends.Values)
                back.Configuration.DirectMail = DirectMailType.StartGossip;

            Task.WaitAll(Enumerable.Range(1, transactions).Select(i =>
                Task.Run(() => Shield.InTransaction(() =>
                {
                    var backend = _backends.Values.Skip(i % 3).First();
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }))).ToArray());

            Thread.Sleep(1000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Shield.InTransaction(() =>
                Enumerable.Range(0, fieldCount).Sum(i => _backends[C].TryGet("key" + i, out CountVector v) ? v.Value : 0));

            Assert.AreEqual(transactions, read);
        }

        [TestMethod]
        public void GossipBackendMultiple_RaceAsymmetric()
        {
            const int transactions = 1000;
            const int fieldCount = 50;

            Shield.InTransaction(() =>
            {
                ((TcpTransport)_backends[A].Transport).ServerIPs.Remove(C);
                ((TcpTransport)_backends[C].Transport).ServerIPs.Remove(A);
            });
            foreach (var back in _backends.Values)
                back.Configuration.DirectMail = DirectMailType.StartGossip;

            Task.WaitAll(Enumerable.Range(1, transactions).Select(i =>
                Task.Run(() => Shield.InTransaction(() =>
                {
                    // run all updates on A, to cause asymmetry in the amount of data they have to gossip about.
                    var backend = _backends[A];
                    var key = "key" + (i % fieldCount);
                    var val = backend.TryGet(key, out CountVector v) ? v : new CountVector();
                    backend.Set(key, val.Increment(backend.Transport.OwnId));
                }))).ToArray());

            Thread.Sleep(1000);
            OnMessage(null, "Done waiting.");
            CheckProtocols();

            var read = Shield.InTransaction(() =>
                Enumerable.Range(0, fieldCount).Sum(i => _backends[C].TryGet("key" + i, out CountVector v) ? v.Value : 0));

            Assert.AreEqual(transactions, read);
        }
    }
}
