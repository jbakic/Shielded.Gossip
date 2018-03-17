using System;
using System.Collections.Generic;

namespace Shielded.Standard
{
    internal abstract class TransactionContext : CommitContinuation
    {
        public Dictionary<object, object> Storage;
    }
}

