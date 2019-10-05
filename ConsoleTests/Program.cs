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
                builder.SetMinimumLevel(LogLevel.Debug);
                //builder.AddConsole(options => options.IncludeScopes = true);
                builder.AddProvider(new MyConsoleLoggerProvider());
            });

            RunConsistentRace();

            _loggerFactory.Dispose();
        }


        public class TestClass
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public int Counter { get; set; }
        }

        public static void RunConsistentRace()
        {
            const int transactions = 100;
            const int fieldCount = 10;

            var backends = _addresses.ToDictionary(kvp => kvp.Key, kvp =>
            {
                var transport = new TcpTransport(kvp.Key, _addresses, _loggerFactory.CreateLogger("Transport" + kvp.Key));
                transport.StartListening();
                return new ConsistentGossipBackend(transport, new GossipConfiguration
                {
                    DirectMail = DirectMailType.StartGossip
                }, _loggerFactory.CreateLogger("Backend" + kvp.Key));
            });

            var bools = Task.WhenAll(ParallelEnumerable.Range(1, transactions).Select(i =>
            {
                var backend = backends.Values.Skip(i % 3).First();
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

            var read = backends[B].RunConsistent(() =>
                Enumerable.Range(0, fieldCount).Sum(i =>
                    backends[B].TryGetVecVersioned<TestClass>("key" + i).SingleOrDefault().Value?.Counter)).Result;

            _loggerFactory.CreateLogger("Program").LogInformation("Completed with success {Success}, and total {Total}", read.Success, read.Value);
        }
    }
}
