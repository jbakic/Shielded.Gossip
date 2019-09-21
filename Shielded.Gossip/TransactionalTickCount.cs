using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Like Environment.TickCount, but freezes within a Shielded transaction.
    /// </summary>
    public static class TransactionalTickCount
    {
        private static ShieldedLocal<int> _transactionTickCount = new ShieldedLocal<int>();

        /// <summary>
        /// Get the current tick count.
        /// </summary>
        public static int Value
        {
            get
            {
                if (!Shield.IsInTransaction)
                    return Environment.TickCount;
                if (!_transactionTickCount.HasValue)
                    return _transactionTickCount.Value = Environment.TickCount;
                return _transactionTickCount.Value;
            }
        }
    }
}
