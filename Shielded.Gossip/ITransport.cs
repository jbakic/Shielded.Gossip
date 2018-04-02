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
        Task Broadcast(object msg);
        Task Send(string server, object msg);
        event EventHandler<object> MessageReceived;
        event EventHandler<Exception> ListenerError;
    }
}
