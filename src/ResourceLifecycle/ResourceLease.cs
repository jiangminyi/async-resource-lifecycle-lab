using System;
using System.Threading;

namespace ResourceLifecycle
{
    /// <summary>A single owner reference. Dispose is idempotent and thread-safe.</summary>
    public sealed class ResourceLease<TResource> : IDisposable where TResource : class
    {
        private readonly TResource _value;
        private Action? _release;

        internal ResourceLease(TResource value, Action release)
        {
            _value = value;
            _release = release;
        }

        public bool IsDisposed => Volatile.Read(ref _release) is null;

        public TResource Value
        {
            get
            {
                if (IsDisposed)
                    throw new ObjectDisposedException(nameof(ResourceLease<TResource>));
                return _value;
            }
        }

        public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
    }
}
