namespace Shielded.Gossip
{
    public enum DirectMailType
    {
        /// <summary>
        /// Send direct mail on every transaction.
        /// </summary>
        Always,
        /// <summary>
        /// Send direct mail on every transaction, but only to servers who we're not
        /// gossipping with right now.
        /// </summary>
        GossipSupressed,
        /// <summary>
        /// Push changes by starting gossip with servers.
        /// </summary>
        StartGossip,
        /// <summary>
        /// Direct mail is off.
        /// </summary>
        Off
    }

    public class GossipConfiguration
    {
        /// <summary>
        /// Whether to send direct mail, and how. Default is <see cref="DirectMailType.GossipSupressed"/>.
        /// </summary>
        public DirectMailType DirectMail { get; set; } = DirectMailType.GossipSupressed;

        /// <summary>
        /// Every this many milliseconds, the backend will try to start a new gossip session with a
        /// randomly selected other server.
        /// </summary>
        public int GossipInterval { get; set; } = 5000;

        /// <summary>
        /// Number of key/value pairs to send in the opening message when beginning a new gossip exchange.
        /// Must be > 0. The message sizes grow exponentially after that, doubling in size, until they
        /// reach the <see cref="AntiEntropyCutoff"/>. Messages will contain only whole transactions,
        /// so the actual size will not be exactly this number. Reply messages will also include all new
        /// changes, so they may be bigger when under load.
        /// </summary>
        public int AntiEntropyInitialSize { get; set; } = 10;

        /// <summary>
        /// Limit on the size, in key/value pairs, of any message. A message may grow bigger than this
        /// now and then, particularly if you perform transactions which change a large number of
        /// fields, because we transmit only whole transactions.
        /// </summary>
        public int AntiEntropyCutoff { get; set; } = 1000;

        /// <summary>
        /// If we hear no reply in this many milliseconds, we may start another gossip session with
        /// the same server. Also, if we receive a gossip reply with an RTT longer than this, we
        /// will not answer.
        /// </summary>
        public int AntiEntropyIdleTimeout { get; set; } = 5000;

        /// <summary>
        /// When choosing a server for gossiping, we might pick one who we're already in a conversation
        /// with. We will make this many attempts total to find an available server, or give up until
        /// the next round.
        /// </summary>
        public int AntiEntropyHuntingLimit { get; set; } = 2;

        /// <summary>
        /// When preparing a consistent transaction, we may get deadlocked with another server. For this
        /// reason, there is a timeout on prepare. This determines the min and max for this timeout,
        /// the backend will pick a random value between the two for every transaction attempt.
        /// </summary>
        public (int Min, int Max) ConsistentPrepareTimeoutRange { get; set; } = (10000, 30000);

        /// <summary>
        /// <see cref="IDeletable"/> implementors get cleaned up from the database every this many
        /// milliseconds, in a way that each deletable key/value will linger in the DB for at least
        /// this much time.
        /// </summary>
        public int DeletableCleanUpInterval { get; set; } = 5000;
    }
}
