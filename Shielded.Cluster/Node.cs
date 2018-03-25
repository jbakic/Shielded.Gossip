using Shielded.Standard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Cluster
{
    public partial class Node
    {
        public readonly string NodeId;
        public readonly IBackend Backend;

        public Node(string nodeId, IBackend backend)
        {
            NodeId = nodeId;
            Backend = backend;
        }

        public static void AssertInTransaction()
        {
            if (_current == null)
                throw new InvalidOperationException("Operation needs to be in a Node.Run call.");
        }

        public static Node Current => _current;

        [ThreadStatic]
        private static Node _current;

        public async Task Run(Action act)
        {
            if (_current == this)
            {
                act();
                return;
            }
            if (_current != null)
                throw new InvalidOperationException("No nesting between Nodes.");

            try
            {
                _current = this;
                using (var cont = Shield.RunToCommit(Timeout.Infinite, act))
                {
                    _current = null;
                    await Commit();
                    cont.Commit();
                }
            }
            catch
            {
                Rollback();
                throw;
            }
            finally
            {
                _current = null;
            }
        }

        public async Task<T> Run<T>(Func<T> func)
        {
            T res = default(T);
            await Run(() => { res = func(); });
            return res;
        }

        public void RunLocal(Action act)
        {
            throw new NotImplementedException();
        }

        public bool TryGet<TItem>(string key, out TItem item)
        {
            return Backend.TryGet(key, out item);
        }

        public void Set<TItem>(string key, TItem item)
        {
            Backend.Set(key, item);
        }

        public void Del<TItem>(string key, TItem item)
        {
            Backend.Del(key, item);
        }

        private Task Commit()
        {
            return Backend.Commit();
        }

        private void Rollback()
        {
            Backend.Rollback();
        }
    }
}
