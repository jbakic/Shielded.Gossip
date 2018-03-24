using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    [Flags]
    public enum VectorRelationship
    {
        Equal = 0,
        Less = 1,
        Greater = 2,
        Conflict = Less | Greater,
    }
}
