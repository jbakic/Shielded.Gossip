using Microsoft.Extensions.Logging;
using Shielded.Gossip;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace ConsoleTests
{
    public class Program
    {
        private const string A = "A";
        private const string B = "B";
        private const string C = "C";

        protected static readonly Dictionary<string, IPEndPoint> _addresses = new Dictionary<string, IPEndPoint>(StringComparer.InvariantCultureIgnoreCase)
        {
            { A, new IPEndPoint(IPAddress.Loopback, 2001) },
            { B, new IPEndPoint(IPAddress.Loopback, 2002) },
            { C, new IPEndPoint(IPAddress.Loopback, 2003) },
        };

        static ILoggerFactory _loggerFactory;

        static void Main(string[] args)
        {
            _loggerFactory = LoggerFactory.Create(builder =>
            {
                //builder.SetMinimumLevel(LogLevel.Debug);
                //builder.AddConsole(options => options.IncludeScopes = true);
                builder.AddProvider(new MyConsoleLoggerProvider());
            });
            var backends = PrepareBackends();

            foreach (var _ in Enumerable.Repeat(0, 100))
                RunConsistentRace(backends);

            DisposeBackends(backends.Values);
            _loggerFactory.Dispose();
        }


        public class TestClass
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Counter { get; set; }
        }

        public static void RunConsistentRace(Dictionary<string, ConsistentGossipBackend> backends)
        {
            const int transactions = 1000;
            const int fieldCount = 100;
            const int prime1 = 113;
            const int prime2 = 149;
            var backendsArray = backends.Values.ToArray();

            var bools = Task.WhenAll(ParallelEnumerable.Range(0, transactions).Select(i =>
            {
                var backend = backendsArray.Skip(i % backends.Count).First();
                var key1 = "key" + (i * prime1 % fieldCount);
                var key2 = "key" + ((i + 1) * prime2 % fieldCount);
                if (key1 == key2)
                    key2 = "key" + (((i + 1) * prime2 + 1) % fieldCount);
                return backend.RunConsistent(() =>
                {
                    var val1 = backend.TryGetVecVersioned<int>(key1)
                        .SingleOrDefault()
                        .NextVersion(backend.Transport.OwnId);
                    val1.Value = val1.Value + 1;

                    var val2 = backend.TryGetVecVersioned<int>(key2)
                        .SingleOrDefault()
                        .NextVersion(backend.Transport.OwnId);
                    val2.Value = val2.Value - 1;
                });
            })).Result;
            var expected = bools.Count(b => b);

            var read = backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    backends[B].TryGetVecVersioned<TestClass>("key" + i).SingleOrDefault().Value?.Counter)).Result;

            _loggerFactory.CreateLogger("Program").LogInformation("Completed with success {Success}, and total {Total}", read.Success && expected == transactions, read.Value);
        }

        private static Dictionary<string, ConsistentGossipBackend> PrepareBackends()
        {
            return _addresses.ToDictionary(kvp => kvp.Key, kvp =>
            {
                var transport = new TcpTransport(kvp.Key, _addresses, _loggerFactory.CreateLogger("Transport" + kvp.Key));
                transport.StartListening();
                return new ConsistentGossipBackend(transport, new GossipConfiguration
                {
                    DirectMail = DirectMailType.StartGossip
                }, _loggerFactory.CreateLogger("Backend" + kvp.Key));
            });
        }

        private static void DisposeBackends(IEnumerable<IGossipBackend> backends)
        {
            foreach (var backend in backends)
                backend.Dispose();
        }
    }
}
