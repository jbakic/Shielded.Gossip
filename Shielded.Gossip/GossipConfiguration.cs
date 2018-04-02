using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Shielded.Gossip
{
    public class GossipConfiguration
    {
        public int GossipInterval { get; set; } = 1000;
        public int AntiEntropyPackageSize { get; set; } = 10;
    }
}
