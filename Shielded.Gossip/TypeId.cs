using System;
using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace Shielded.Gossip
{
    /// <summary>
    /// Helper for obtaining a fully qualified type name which does not contain the
    /// assembly version information, and for faster obtaining of Types from such IDs.
    /// Used by the <see cref="Serializer"/>.
    /// </summary>
    public static class TypeId
    {
        private static readonly Regex _regex = new Regex(", (Version|Culture|PublicKeyToken)=[^,\\]]*", RegexOptions.Compiled);

        private static readonly ConcurrentDictionary<Type, string> _ids = new ConcurrentDictionary<Type, string>();

        public static string GetId(Type type) =>
            _ids.GetOrAdd(type, t => _regex.Replace(t.AssemblyQualifiedName, string.Empty));

        private static readonly ConcurrentDictionary<string, Type> _types = new ConcurrentDictionary<string, Type>();

        public static Type GetType(string id) => _types.GetOrAdd(id, Type.GetType);
    }
}
