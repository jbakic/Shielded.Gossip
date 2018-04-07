using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace Shielded.Gossip
{
    public class GossipConfiguration
    {
        public bool DirectMail { get; set; } = true;

        public int GossipInterval { get; set; } = 1000;
        // TODO: should the package size grow the further we go?
        public int AntiEntropyPackageSize { get; set; } = 10;
        public int AntiEntropyPackageCutoff { get; set; } = 1000;
    }
}
