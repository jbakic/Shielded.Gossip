using System;

namespace Shielded.Gossip
{
    /// <summary>
    /// Event raised when accessing a key for which <see cref="IGossipBackend"/> has no value. Using
    /// the <see cref="UseValue(IHasVersionBytes, bool, int?)"/> method you can provide the value which the
    /// backend will store in itself and return to the reader.
    /// </summary>
    public class KeyMissingEventArgs : EventArgs
    {
        /// <summary>
        /// Get the key which is being accessed.
        /// </summary>
        public readonly string Key;

        /// <summary>
        /// Get the value set by calling <see cref="UseValue(IHasVersionBytes, bool, int?)"/>.
        /// </summary>
        public IHasVersionBytes ValueToUse { get; private set; }

        /// <summary>
        /// Get a bool indicating whether the value set by calling <see cref="UseValue(IHasVersionBytes, bool, int?)"/> is deleted.
        /// </summary>
        public bool ValueDeleted { get; private set; }

        /// <summary>
        /// Get the expiry of the value set by calling <see cref="UseValue(IHasVersionBytes, bool, int?)"/>.
        /// </summary>
        public int? ExpiresInMs { get; private set; }

        public KeyMissingEventArgs(string key)
        {
            Key = key;
        }

        /// <summary>
        /// Set the value to return to the original reader. The value will also be stored in
        /// the backend for future read attempts.
        /// </summary>
        /// <param name="value">The value to return to the reader, must be != null.</param>
        /// <param name="deleted">Bool indicating if the value is deleted.</param>
        /// <param name="expiresInMs">The expiry for the value. If not null, must be > 0.</param>
        public void UseValue(IHasVersionBytes value, bool deleted = false, int? expiresInMs = null)
        {
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            if (expiresInMs <= 0)
                throw new ArgumentOutOfRangeException(nameof(expiresInMs));
            ValueToUse = value;
            ValueDeleted = deleted;
            ExpiresInMs = expiresInMs;
        }
    }
}
