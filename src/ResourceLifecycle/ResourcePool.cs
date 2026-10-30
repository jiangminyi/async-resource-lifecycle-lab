using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ResourceLifecycle
{
    /// <summary>
    /// Coalesces same-key loads while references exist. A reference is either a pending
    /// acquisition or an issued lease. An abandoned generation can never be revived.
    /// </summary>
    public sealed class ResourcePool<TKey, TResource> : IDisposable, IAsyncDisposable
        where TKey : notnull
        where TResource : class
    {
        private readonly object _gate = new object();
        private readonly IResourceProvider<TKey, TResource> _provider;
        private readonly Dictionary<TKey, Entry> _active;
        private readonly HashSet<Entry> _live = new HashSet<Entry>();
        private readonly List<Exception> _cleanupErrors = new List<Exception>();
        private readonly TaskCompletionSource<bool> _drained = NewCompletion<bool>();
        private bool _closed;

        private sealed class Entry
        {
            public Entry(TKey key) { Key = key; }
            public readonly TKey Key;
            public readonly CancellationTokenSource LoadCancellation = new CancellationTokenSource();
            public readonly TaskCompletionSource<TResource> Ready = NewCompletion<TResource>();
            public TResource? Resource;
            public int References;
            public bool Detached;
            public bool LoadFinished;
            public bool CleanupClaimed;
            public int CancellationDispatches;
        }

        public ResourcePool(IResourceProvider<TKey, TResource> provider, IEqualityComparer<TKey>? comparer = null)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _active = new Dictionary<TKey, Entry>(comparer);
        }

        /// <summary>
        /// Completes only after shutdown, all provider loads have returned, all leases
        /// have been disposed and every successful resource has received one release call.
        /// Faults if cancellation callbacks or release callbacks threw exceptions.
        /// </summary>
        public Task WhenDrained => _drained.Task;

        public PoolSnapshot Snapshot
        {
            get
            {
                lock (_gate)
                {
                    int pending = 0, references = 0;
                    foreach (Entry entry in _live)
                    {
                        if (!entry.LoadFinished) pending++;
                        references += entry.References;
                    }
                    return new PoolSnapshot(_closed, _active.Count, _live.Count,
                        pending, references, _cleanupErrors.Count);
                }
            }
        }

        public async Task<ResourceLease<TResource>> AcquireAsync(TKey key, CancellationToken cancellationToken = default)
        {
            if (key is null) throw new ArgumentNullException(nameof(key));
            cancellationToken.ThrowIfCancellationRequested();
            Entry entry;
            bool startLoad = false;
            lock (_gate)
            {
                ThrowIfClosed();
                if (!_active.TryGetValue(key, out entry!))
                {
                    entry = new Entry(key);
                    _active.Add(key, entry);
                    _live.Add(entry);
                    startLoad = true;
                }
                checked { entry.References++; }
            }

            // Never invoke the provider while holding the pool's state lock.
            if (startLoad) _ = LoadAndPublishAsync(entry);
            bool issuedLease = false;
            try
            {
                TResource resource = await AwaitWithCancellationAsync(entry.Ready.Task, cancellationToken).ConfigureAwait(false);
                lock (_gate)
                {
                    // This lock is the acquisition's success linearization point.
                    cancellationToken.ThrowIfCancellationRequested();
                    ThrowIfClosed();
                    if (entry.Detached) throw new InvalidOperationException("The resource generation was retired.");
                    var lease = new ResourceLease<TResource>(resource, () => DropReference(entry));
                    issuedLease = true;
                    return lease;
                }
            }
            finally
            {
                if (!issuedLease) DropReference(entry);
            }
        }

        private async Task LoadAndPublishAsync(Entry entry)
        {
            TResource? resource = null;
            Exception? failure = null;
            try
            {
                resource = await _provider.LoadAsync(entry.Key, entry.LoadCancellation.Token).ConfigureAwait(false);
                if (resource is null) throw new InvalidOperationException("The provider returned a null resource.");
            }
            catch (Exception exception)
            {
                failure = exception;
            }

            bool cleanup;
            lock (_gate)
            {
                entry.LoadFinished = true;
                entry.Resource = resource;
                if (failure is not null)
                {
                    DetachLocked(entry);
                    entry.Ready.TrySetException(failure);
                }
                else if (!entry.Detached && !_closed)
                {
                    entry.Ready.TrySetResult(resource!);
                }
                else
                {
                    // No consumer may observe a late, already-abandoned load result.
                    entry.Ready.TrySetCanceled();
                }
                cleanup = ClaimCleanupLocked(entry);
            }
            if (cleanup) Cleanup(entry);
        }

        private void DropReference(Entry entry)
        {
            bool cancel = false, cleanup;
            lock (_gate)
            {
                if (entry.References <= 0) throw new InvalidOperationException("Reference accounting underflow.");
                entry.References--;
                if (entry.References == 0)
                {
                    DetachLocked(entry);
                    entry.Ready.TrySetCanceled();
                    cancel = !entry.LoadFinished;
                    if (cancel) entry.CancellationDispatches++;
                }
                cleanup = ClaimCleanupLocked(entry);
            }
            if (cancel) RequestCancellation(entry);
            if (cleanup) Cleanup(entry);
        }

        private void DetachLocked(Entry entry)
        {
            if (entry.Detached) return;
            entry.Detached = true;
            if (_active.TryGetValue(entry.Key, out Entry? current) && ReferenceEquals(current, entry))
                _active.Remove(entry.Key);
        }

        private static bool ClaimCleanupLocked(Entry entry)
        {
            if (!entry.Detached || entry.References != 0 || !entry.LoadFinished
                || entry.CleanupClaimed || entry.CancellationDispatches != 0)
                return false;
            entry.CleanupClaimed = true;
            return true;
        }

        private void Cleanup(Entry entry)
        {
            try
            {
                if (entry.Resource is not null) _provider.Release(entry.Key, entry.Resource);
            }
            catch (Exception exception)
            {
                lock (_gate) _cleanupErrors.Add(exception);
            }
            finally
            {
                entry.LoadCancellation.Dispose();
                lock (_gate)
                {
                    _live.Remove(entry);
                    CompleteDrainLocked();
                }
            }
        }

        private void RequestCancellation(Entry entry)
        {
            try { entry.LoadCancellation.Cancel(); }
            catch (Exception exception)
            {
                lock (_gate) _cleanupErrors.Add(exception);
            }
            finally
            {
                bool cleanup;
                lock (_gate)
                {
                    entry.CancellationDispatches--;
                    cleanup = ClaimCleanupLocked(entry);
                }
                if (cleanup) Cleanup(entry);
            }
        }

        public void Dispose()
        {
            var cancel = new List<Entry>();
            var cleanup = new List<Entry>();
            lock (_gate)
            {
                if (_closed) return;
                _closed = true;
                foreach (Entry entry in _live)
                {
                    DetachLocked(entry);
                    if (entry.References > 0)
                        entry.Ready.TrySetException(new ObjectDisposedException(nameof(ResourcePool<TKey, TResource>)));
                    else
                        entry.Ready.TrySetCanceled();
                    if (!entry.LoadFinished)
                    {
                        entry.CancellationDispatches++;
                        cancel.Add(entry);
                    }
                    if (ClaimCleanupLocked(entry)) cleanup.Add(entry);
                }
                CompleteDrainLocked();
            }
            foreach (Entry entry in cancel) RequestCancellation(entry);
            foreach (Entry entry in cleanup) Cleanup(entry);
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return new ValueTask(WhenDrained);
        }

        private void CompleteDrainLocked()
        {
            if (!_closed || _live.Count != 0) return;
            if (_cleanupErrors.Count == 0) _drained.TrySetResult(true);
            else _drained.TrySetException(new AggregateException("Resource cleanup failed.", _cleanupErrors));
        }

        private void ThrowIfClosed()
        {
            if (_closed) throw new ObjectDisposedException(nameof(ResourcePool<TKey, TResource>));
        }

        private static TaskCompletionSource<T> NewCompletion<T>() =>
            new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

        private static async Task<T> AwaitWithCancellationAsync<T>(Task<T> task, CancellationToken token)
        {
            if (!token.CanBeCanceled) return await task.ConfigureAwait(false);
            var canceled = NewCompletion<bool>();
            using (token.Register(state => ((TaskCompletionSource<bool>)state!).TrySetResult(true), canceled))
            {
                await Task.WhenAny(task, canceled.Task).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                return await task.ConfigureAwait(false);
            }
        }
    }
}
