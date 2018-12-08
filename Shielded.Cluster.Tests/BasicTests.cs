//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using Shielded.Standard;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading;
//using System.Threading.Tasks;

//namespace Shielded.Cluster.Tests
//{
//    [TestClass]
//    public class BasicTests
//    {
//        private MockStrongBackend[] CreateCluster(int n)
//        {
//            return Enumerable.Repeat(0, n)
//                .Select(i => new MockStrongBackend())
//                .ToArray();
//        }

//        [TestMethod]
//        public void TwoNodes_BasicSync()
//        {
//            var backends = CreateCluster(2);

//            Distributed.Run(() =>
//            {
//                backends[0].Set("key1", "one");
//            }).Wait();

//            var read = Distributed.Run(() => backends[1].TryGet("key1", out string val) ? val : null).Result;

//            Assert.AreEqual("one", read);
//            Assert.IsTrue(MockStrongBackend.ConfirmCommits(1));
//        }

//        [TestMethod]
//        public void Nesting_Basic()
//        {
//            var a = new Shielded<int>();

//            foreach (var i in Enumerable.Range(0, 100))
//            {
//                Assert.AreEqual(20,
//                    Distributed.Run(() =>
//                    {
//                        a.Value = 10;
//                        return Distributed.Run(() => a.Value = a + 10).Result;
//                    }).Result);

//                Assert.AreEqual(20,
//                    Distributed.Consistent(() =>
//                    {
//                        a.Value = 10;
//                        return Distributed.Consistent(() => a.Value = a + 10).Result.Value;
//                    }).Result.Value);

//                Assert.ThrowsException<AggregateException>(() =>
//                    Assert.AreEqual(20,
//                        Distributed.Run(() =>
//                        {
//                            a.Value = 10;
//                            return Distributed.Consistent(() => a.Value = a + 10).Result.Value;
//                        }).Result));

//                Assert.AreEqual(20,
//                    Distributed.Consistent(() =>
//                    {
//                        a.Value = 10;
//                        return Distributed.Run(() => a.Value = a + 10).Result;
//                    }).Result.Value);
//            }
//        }

//        private void RunOffThread(Action act)
//        {
//            var t = new Thread(_ => act());
//            t.Start();
//            t.Join();
//        }

//        [TestMethod]
//        public void Nesting_TransException()
//        {
//            // nested calls will throw AggregateExceptions, including if the original exception
//            // was a TransException. Shield class now handles this.
//            var a = new Shielded<int>();
//            bool done = false;
//            Assert.AreEqual(20,
//                Distributed.Run(() =>
//                {
//                    a.Value = 10;
//                    return Distributed.Run(() =>
//                    {
//                        // to get a doubly wrapped AggregateException.
//                        return Distributed.Run(() =>
//                        {
//                            if (!done)
//                            {
//                                RunOffThread(() => Shield.InTransaction(() => a.Value = -1));
//                                done = true;
//                            }
//                            return a.Value = a + 10;
//                        }).Result;
//                    }).Result;
//                }).Result);
//        }
//    }
//}
