using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    public interface IHasVectorClock
    {
        VectorClock Clock { get; set; }
    }
}
