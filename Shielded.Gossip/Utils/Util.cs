using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Shielded.Gossip.Utils
{
    internal static class Util
    {
        internal static bool RoughlyEqual(int? num1, int? num2, int precision)
        {
            // this covers the case if both are null.
            if (num1 == num2)
                return true;
            return num1.HasValue && num2.HasValue &&
                Math.Abs(num1.Value - num2.Value) < precision;
        }

        internal static bool IsByteEqual(byte[] one, byte[] two)
        {
            if (one == null && two == null)
                return true;
            if (one == null || two == null || one.Length != two.Length)
                return false;
            var len = one.Length;
            for (int i = 0; i < len; i++)
                if (one[i] != two[i])
                    return false;
            return true;
        }

        internal static async Task<T> WithTimeout<T>(this Task<T> task, int ms, T timeoutResult = default)
        {
            using (var cts = new CancellationTokenSource())
            {
                if (await Task.WhenAny(task, Task.Delay(ms, cts.Token)).ConfigureAwait(false) == task)
                {
                    cts.Cancel();
                    return await task;
                }
                else
                    return timeoutResult;
            }
        }
    }
}
