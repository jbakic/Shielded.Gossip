using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Shielded.Gossip
{
    public interface ITransport : IDisposable
    {
        string OwnId { get; }
        ICollection<string> Servers { get; }
        void Broadcast(object msg);
        void Send(string server, object msg);
        event EventHandler<object> MessageReceived;
        event EventHandler<Exception> Error;
    }
}
