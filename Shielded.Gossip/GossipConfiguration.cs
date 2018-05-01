namespace Shielded.Gossip
{
    public class GossipConfiguration
    {
        public bool DirectMail { get; set; } = true;

        public int GossipInterval { get; set; } = 1000;
        // number of transactions to add to the gossip window each round
        public int AntiEntropyPackageSize { get; set; } = 20;
        // max number of items, but we may send more to make sure only whole transactions are sent
        public int AntiEntropyPackageCutoff { get; set; } = 1000;
        // the time after sending the last msg, after which we may send a new GossipStart to the same
        // server. flood control.
        public int AntiEntropyIdleTimeout { get; set; } = 2000;
        public int AntiEntropyHuntingLimit { get; set; } = 2;

        public int DeletableCleanUpInterval { get; set; } = 5000;

        public (int Min, int Max) ConsistentPrepareTimeoutRange { get; set; } = (10000, 30000);
    }
}
