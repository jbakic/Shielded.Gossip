using System;
using System.Collections.Generic;
using System.Text;

namespace Shielded.Gossip
{
    /// <summary>
    /// Describes the outcome of a consistent transaction.
    /// </summary>
    public enum ConsistentOutcome
    {
        Success,
        /// <summary>
        /// Transaction was cancelled fully, no attempt left running in the background.
        /// </summary>
        Cancelled,
        /// <summary>
        /// Transaction was cancelled while running, the last attempt continues to run in the background.
        /// </summary>
        CancelledWhileRunning,
        MaxAttemptsFailed,
    }

    /// <summary>
    /// Result of a consistent transaction.
    /// </summary>
    public struct ConsistentResult<T>
    {
        public ConsistentOutcome Outcome { get; }

        private readonly T _value;
        /// <summary>
        /// Gets the result value, or throws if the <see cref="Outcome"/> is not <see cref="ConsistentOutcome.Success"/>.
        /// </summary>
        public T Value => Outcome == ConsistentOutcome.Success ? _value : throw new InvalidOperationException("Transaction has failed, unable to return value.");

        public ConsistentResult(ConsistentOutcome outcome, T value)
        {
            Outcome = outcome;
            _value = outcome == ConsistentOutcome.Success ? value : default;
        }
    }
}
