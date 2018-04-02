using System;
using System.Text.RegularExpressions;

namespace Shielded.Gossip
{
    public static class TypeId
    {
        private static Regex _regex = new Regex(", (Version|Culture|PublicKeyToken)=[^,\\]]*", RegexOptions.Compiled);

        public static string Get(Type type)
        {
            return _regex.Replace(type.AssemblyQualifiedName, string.Empty);
        }
    }
}
