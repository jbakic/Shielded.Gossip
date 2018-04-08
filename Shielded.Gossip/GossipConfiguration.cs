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
        // these two are transaction* counts, not field. msgs thus depend on size of
        // largest transaction, which is under the control of the dev.
        public int AntiEntropyPackageSize { get; set; } = 20;
        public int AntiEntropyPackageCutoff { get; set; } = 1000;
        // the time after sending the last msg, after which we may send a new GossipStart to the same
        // server. flood control.
        public int AntiEntropyIdleTimeout { get; set; } = 2000;
        public int AntiEntropyHuntingLimit { get; set; } = 2;
    }
}
