using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading;

namespace ConsoleTests
{
    public class MyConsoleLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName)
        {
            return new MyConsoleLogger(categoryName);
        }

        public void Dispose() { }
    }

    public class MyConsoleLogger : ILogger
    {
        private readonly string _categoryName;
        private readonly AsyncLocal<ImmutableList<object>> _scopes = new AsyncLocal<ImmutableList<object>>();

        public MyConsoleLogger(string categoryName)
        {
            _categoryName = categoryName;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            Console.WriteLine(
                $"{DateTime.Now:HH:mm:ss.fff} {FormatLogLevel(logLevel),-5} {_categoryName}: { GetScopes() }{formatter(state, exception)}");
        }

        private string FormatLogLevel(LogLevel logLevel) =>
            logLevel == LogLevel.Information ? "INFO" :
            logLevel == LogLevel.Warning ? "WARN" :
            logLevel == LogLevel.Critical ? "CRIT" :
            logLevel.ToString().ToUpper();

        private string GetScopes()
        {
            var scopes = _scopes.Value;
            if (scopes == null || scopes.IsEmpty)
                return "";
            return string.Concat(scopes.Select(s => "[" + s.ToString() + "]")) + " ";
        }

        public bool IsEnabled(LogLevel logLevel) => true;

        public IDisposable BeginScope<TState>(TState state)
        {
            _scopes.Value = (_scopes.Value ?? ImmutableList<object>.Empty).Add(state);
            return new Disposable(() => _scopes.Value = _scopes.Value.Remove(state));
        }
    }

    public class Disposable : IDisposable
    {
        private readonly Action _onDispose;

        public Disposable(Action onDispose) => _onDispose = onDispose;
        public void Dispose() => _onDispose();
    }
}
