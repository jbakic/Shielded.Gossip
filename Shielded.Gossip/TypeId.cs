using System;
using System.Text.RegularExpressions;

namespace Shielded.Gossip
{
    /// <summary>
    /// Helper for obtaining a fully qualified type name which does not contain the
    /// assembly version information. Used by the <see cref="Serializer"/>.
    /// </summary>
    public static class TypeId
    {
        private static Regex _regex = new Regex(", (Version|Culture|PublicKeyToken)=[^,\\]]*", RegexOptions.Compiled);

        public static string Get(Type type)
        {
            return _regex.Replace(type.AssemblyQualifiedName, string.Empty);
        }
    }
}
