using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Describes the relationship between two CRDTs.
    /// </summary>
    [Flags]
    public enum VectorRelationship
    {
        Equal = 0,
        Less = 1,
        Greater = 2,
        Conflict = Less | Greater,
    }
}
