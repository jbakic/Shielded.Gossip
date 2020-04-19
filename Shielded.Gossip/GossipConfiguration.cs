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
        /// reach the <see cref="AntiEntropyItemsCutoff"/>. Messages will contain only whole transactions,
        /// so the actual size will not be exactly this number. Reply messages will also include all new
        /// changes, so they may be bigger when under load.
        /// </summary>
        public int AntiEntropyInitialSize { get; set; } = 10;

        /// <summary>
        /// Limit on the size, in key/value pairs, of any message. A message may grow bigger than this
        /// now and then, particularly if you perform transactions which change a large number of
        /// fields, because we transmit only whole transactions.
        /// </summary>
        public int AntiEntropyItemsCutoff { get; set; } = 1000;

        /// <summary>
        /// Limit on the size, in bytes, of any message. The messages can get a little bigger than this,
        /// because of the size of metadata accompanying the items, and also because we only transmit
        /// whole transactions.
        /// </summary>
        public int AntiEntropyBytesCutoff { get; set; } = 10 * 1024 * 1024;

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
        /// How often should the garbage collection take place.
        /// </summary>
        public int CleanUpInterval { get; set; } = 2000;

        /// <summary>
        /// Items which are deletable, removed, or have expired will linger around in the database for
        /// this much time, after which the garbage collection will finally remove them. They must linger
        /// to communicate their removable state to other servers.
        /// </summary>
        public int RemovableItemLingerMs { get; set; } = 60000;

        /// <summary>
        /// When merging values, if their expiry times are within this many milliseconds of each other,
        /// they will be considered equal.
        /// </summary>
        public int ExpiryComparePrecision { get; set; } = 100;
    }
}
