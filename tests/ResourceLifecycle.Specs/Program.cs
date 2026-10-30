using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ResourceLifecycle;

namespace ResourceLifecycle.Specs
{
    internal static class Program
    {
        private static async Task<int> Main()
        {
            (string Name, Func<Task> Run)[] cases =
            {
                ("same_key_64_callers_share_one_load", SharedLoad),
                ("one_canceled_waiter_preserves_other_waiters", IndependentCancellation),
                ("abandoned_generation_cannot_replace_new_generation", AbandonedGeneration),
                ("failed_load_is_not_cached_and_can_retry", FailureRetry),
                ("shutdown_cancels_waiters_and_releases_late_result", ShutdownPending),
                ("shutdown_preserves_live_leases_until_release", ShutdownWithLease),
                ("repeated_lease_and_pool_dispose_are_idempotent", IdempotentDispose),
                ("pre_canceled_acquire_does_not_start_provider", PreCanceled),
                ("keys_have_independent_ownership", IndependentKeys),
                ("synchronous_provider_exception_releases_reservation", SynchronousFailure),
                ("null_provider_result_fails_without_leak", NullResult),
                ("cancellation_after_grant_does_not_revoke_lease", CancellationAfterGrant),
                ("cancellation_completion_race_250_iterations", CompletionCancellationRace),
                ("parallel_dispose_releases_exactly_once", ParallelDispose),
                ("provider_release_failure_faults_shutdown_drain", ReleaseFailure),
                ("throwing_cancel_callback_is_reported_after_drain", CancellationCallbackFailure),
                ("same_key_reacquire_after_zero_references_loads_again", Reacquire),
                ("custom_key_comparer_controls_coalescing", CustomComparer),
                ("dispose_empty_pool_completes_drain", EmptyShutdown)
            };

            int passed = 0;
            foreach (var test in cases)
            {
                try
                {
                    // A watchdog catches hangs; test ordering uses TCS gates, not sleeps.
                    await test.Run().WaitAsync(TimeSpan.FromSeconds(12));
                    Console.WriteLine($"PASS {test.Name}");
                    passed++;
                }
                catch (Exception exception)
                {
                    Console.Error.WriteLine($"FAIL {test.Name}: {exception}");
                    return 1;
                }
            }
            Console.WriteLine($"{passed}/{cases.Length} specifications passed.");
            return 0;
        }

        private static async Task SharedLoad()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>>[] requests = Enumerable.Range(0, 64)
                .Select(_ => pool.AcquireAsync("shared")).ToArray();
            Equal(1, provider.LoadCount);
            Equal(64, pool.Snapshot.OutstandingReferences);
            var asset = new Asset("shared/1");
            provider.Attempt(0).Succeed(asset);
            ResourceLease<Asset>[] leases = await Task.WhenAll(requests);
            Check(leases.All(lease => ReferenceEquals(asset, lease.Value)), "All leases must share one instance.");
            for (int index = 0; index < leases.Length - 1; index++) leases[index].Dispose();
            Equal(0, provider.ReleaseCount);
            leases[63].Dispose();
            Equal(1, provider.ReleaseCount);
            await CloseAndAssertEmpty(pool);
        }

        private static async Task IndependentCancellation()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            using var canceled = new CancellationTokenSource();
            Task<ResourceLease<Asset>> first = pool.AcquireAsync("shared", canceled.Token);
            Task<ResourceLease<Asset>> second = pool.AcquireAsync("shared");
            canceled.Cancel();
            await ThrowsAsync<OperationCanceledException>(async () => { await first; });
            Check(!provider.Attempt(0).Token.IsCancellationRequested, "One canceled waiter must not cancel the shared provider.");
            provider.Attempt(0).Succeed(new Asset("shared/1"));
            (await second).Dispose();
            await CloseAndAssertEmpty(pool);
            Equal(1, provider.ReleaseCount);
        }

        private static async Task AbandonedGeneration()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            using var cancel = new CancellationTokenSource();
            Task<ResourceLease<Asset>> abandoned = pool.AcquireAsync("same", cancel.Token);
            cancel.Cancel();
            await ThrowsAsync<OperationCanceledException>(async () => { await abandoned; });
            Check(provider.Attempt(0).Token.IsCancellationRequested, "Final canceled reference requests cancellation.");
            Task<ResourceLease<Asset>> fresh = pool.AcquireAsync("same");
            Equal(2, provider.LoadCount);
            var oldAsset = new Asset("old");
            provider.Attempt(0).Succeed(oldAsset);
            await provider.WaitForReleaseCountAsync(1);
            Check(!fresh.IsCompleted, "A stale result must never satisfy the new generation.");
            var newAsset = new Asset("new");
            provider.Attempt(1).Succeed(newAsset);
            ResourceLease<Asset> lease = await fresh;
            Check(ReferenceEquals(newAsset, lease.Value), "New generation owns its own result.");
            lease.Dispose();
            await CloseAndAssertEmpty(pool);
            Equal(1, provider.ReleaseCountFor(oldAsset));
            Equal(1, provider.ReleaseCountFor(newAsset));
        }

        private static async Task FailureRetry()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> first = pool.AcquireAsync("retry");
            Task<ResourceLease<Asset>> second = pool.AcquireAsync("retry");
            provider.Attempt(0).Fail(new IOException("Injected network failure."));
            await ThrowsAsync<IOException>(async () => { await first; });
            await ThrowsAsync<IOException>(async () => { await second; });
            Equal(0, pool.Snapshot.OutstandingReferences);
            Task<ResourceLease<Asset>> retry = pool.AcquireAsync("retry");
            Equal(2, provider.LoadCount);
            provider.Attempt(1).Succeed(new Asset("recovered"));
            (await retry).Dispose();
            await CloseAndAssertEmpty(pool);
            Equal(1, provider.ReleaseCount);
        }

        private static async Task ShutdownPending()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> first = pool.AcquireAsync("late");
            Task<ResourceLease<Asset>> second = pool.AcquireAsync("late");
            pool.Dispose();
            await ThrowsAsync<ObjectDisposedException>(async () => { await first; });
            await ThrowsAsync<ObjectDisposedException>(async () => { await second; });
            Check(!pool.WhenDrained.IsCompleted, "Drain waits for a provider that ignores cancellation.");
            Check(provider.Attempt(0).Token.IsCancellationRequested, "Provider cancellation was requested.");
            var late = new Asset("late");
            provider.Attempt(0).Succeed(late);
            await pool.WhenDrained;
            Equal(1, provider.ReleaseCountFor(late));
            AssertEmpty(pool);
        }

        private static async Task ShutdownWithLease()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> request = pool.AcquireAsync("held");
            var asset = new Asset("held");
            provider.Attempt(0).Succeed(asset);
            ResourceLease<Asset> lease = await request;
            pool.Dispose();
            Equal(0, provider.ReleaseCount);
            Check(ReferenceEquals(asset, lease.Value), "Shutdown must not invalidate a granted lease.");
            Check(!pool.WhenDrained.IsCompleted, "Outstanding lease keeps the pool from draining.");
            await ThrowsAsync<ObjectDisposedException>(async () => { await pool.AcquireAsync("held"); });
            lease.Dispose();
            await pool.WhenDrained;
            Throws<ObjectDisposedException>(() => { _ = lease.Value; });
            Equal(1, provider.ReleaseCount);
            AssertEmpty(pool);
        }

        private static async Task IdempotentDispose()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> request = pool.AcquireAsync("once");
            provider.Attempt(0).Succeed(new Asset("once"));
            ResourceLease<Asset> lease = await request;
            lease.Dispose(); lease.Dispose(); lease.Dispose();
            pool.Dispose(); pool.Dispose();
            await pool.DisposeAsync();
            Equal(1, provider.ReleaseCount);
            AssertEmpty(pool);
        }

        private static async Task PreCanceled()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();
            await ThrowsAsync<OperationCanceledException>(async () => { await pool.AcquireAsync("unused", canceled.Token); });
            Equal(0, provider.LoadCount);
            await CloseAndAssertEmpty(pool);
        }

        private static async Task IndependentKeys()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> a = pool.AcquireAsync("a");
            Task<ResourceLease<Asset>> b = pool.AcquireAsync("b");
            Equal(2, provider.LoadCount);
            provider.Attempt(1).Succeed(new Asset("b"));
            ResourceLease<Asset> bLease = await b;
            Check(!a.IsCompleted, "Different key does not complete another request.");
            bLease.Dispose();
            provider.Attempt(0).Succeed(new Asset("a"));
            (await a).Dispose();
            await CloseAndAssertEmpty(pool);
            Equal(2, provider.ReleaseCount);
        }

        private static async Task SynchronousFailure()
        {
            var pool = new ResourcePool<string, Asset>(new SynchronousThrowProvider());
            await ThrowsAsync<IOException>(async () => { await pool.AcquireAsync("bad"); });
            await CloseAndAssertEmpty(pool);
        }

        private static async Task NullResult()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> request = pool.AcquireAsync("null");
            provider.Attempt(0).Succeed(null!);
            await ThrowsAsync<InvalidOperationException>(async () => { await request; });
            await CloseAndAssertEmpty(pool);
            Equal(0, provider.ReleaseCount);
        }

        private static async Task CancellationAfterGrant()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            using var cancel = new CancellationTokenSource();
            Task<ResourceLease<Asset>> request = pool.AcquireAsync("owned", cancel.Token);
            var asset = new Asset("owned");
            provider.Attempt(0).Succeed(asset);
            ResourceLease<Asset> lease = await request;
            cancel.Cancel();
            Check(ReferenceEquals(asset, lease.Value), "The cancellation token is scoped to acquisition, not lease lifetime.");
            Equal(0, provider.ReleaseCount);
            lease.Dispose();
            await CloseAndAssertEmpty(pool);
        }

        private static async Task CompletionCancellationRace()
        {
            for (int index = 0; index < 250; index++)
            {
                var provider = new ControlledProvider();
                var pool = new ResourcePool<string, Asset>(provider);
                using var cancel = new CancellationTokenSource();
                Task<ResourceLease<Asset>> request = pool.AcquireAsync("race", cancel.Token);
                var asset = new Asset("race");
                var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                Task cancelTask = Task.Run(async () => { await gate.Task; cancel.Cancel(); });
                Task completeTask = Task.Run(async () => { await gate.Task; provider.Attempt(0).Succeed(asset); });
                gate.SetResult(true);
                await Task.WhenAll(cancelTask, completeTask);
                try { (await request).Dispose(); }
                catch (OperationCanceledException) { }
                await CloseAndAssertEmpty(pool);
                Equal(1, provider.ReleaseCountFor(asset));
            }
        }

        private static async Task ParallelDispose()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>>[] requests = Enumerable.Range(0, 100).Select(_ => pool.AcquireAsync("parallel")).ToArray();
            provider.Attempt(0).Succeed(new Asset("parallel"));
            ResourceLease<Asset>[] leases = await Task.WhenAll(requests);
            await Task.WhenAll(leases.SelectMany(lease => new[] { Task.Run(lease.Dispose), Task.Run(lease.Dispose) }));
            await CloseAndAssertEmpty(pool);
            Equal(1, provider.ReleaseCount);
        }

        private static async Task ReleaseFailure()
        {
            var provider = new ControlledProvider { ThrowOnRelease = true };
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> request = pool.AcquireAsync("release-error");
            provider.Attempt(0).Succeed(new Asset("release-error"));
            (await request).Dispose();
            pool.Dispose();
            await ThrowsAsync<AggregateException>(async () => { await pool.WhenDrained; });
            Equal(1, pool.Snapshot.CleanupErrorCount);
            Equal(1, provider.ReleaseCount);
            AssertEmpty(pool);
        }

        private static async Task CancellationCallbackFailure()
        {
            var provider = new ControlledProvider { ThrowOnCancellation = true };
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> request = pool.AcquireAsync("cancel-error");
            pool.Dispose();
            await ThrowsAsync<ObjectDisposedException>(async () => { await request; });
            provider.Attempt(0).Succeed(new Asset("cancel-error"));
            await ThrowsAsync<AggregateException>(async () => { await pool.WhenDrained; });
            Equal(1, pool.Snapshot.CleanupErrorCount);
            Equal(1, provider.ReleaseCount);
            AssertEmpty(pool);
        }

        private static async Task Reacquire()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider);
            Task<ResourceLease<Asset>> first = pool.AcquireAsync("fresh");
            provider.Attempt(0).Succeed(new Asset("first"));
            (await first).Dispose();
            Task<ResourceLease<Asset>> second = pool.AcquireAsync("fresh");
            Equal(2, provider.LoadCount);
            provider.Attempt(1).Succeed(new Asset("second"));
            (await second).Dispose();
            await CloseAndAssertEmpty(pool);
            Equal(2, provider.ReleaseCount);
        }

        private static async Task CustomComparer()
        {
            var provider = new ControlledProvider();
            var pool = new ResourcePool<string, Asset>(provider, StringComparer.OrdinalIgnoreCase);
            Task<ResourceLease<Asset>> a = pool.AcquireAsync("ICON");
            Task<ResourceLease<Asset>> b = pool.AcquireAsync("icon");
            Equal(1, provider.LoadCount);
            provider.Attempt(0).Succeed(new Asset("icon"));
            (await a).Dispose(); (await b).Dispose();
            await CloseAndAssertEmpty(pool);
        }

        private static async Task EmptyShutdown()
        {
            var pool = new ResourcePool<string, Asset>(new ControlledProvider());
            Check(!pool.WhenDrained.IsCompleted, "A still-open pool has no shutdown result.");
            await CloseAndAssertEmpty(pool);
        }

        private static async Task CloseAndAssertEmpty(ResourcePool<string, Asset> pool)
        {
            pool.Dispose();
            await pool.WhenDrained;
            AssertEmpty(pool);
        }

        private static void AssertEmpty(ResourcePool<string, Asset> pool)
        {
            PoolSnapshot snapshot = pool.Snapshot;
            Check(snapshot.IsClosed, "Pool must be closed.");
            Equal(0, snapshot.ActiveKeys);
            Equal(0, snapshot.LiveGenerations);
            Equal(0, snapshot.PendingLoads);
            Equal(0, snapshot.OutstandingReferences);
        }

        private static void Equal(int expected, int actual)
        {
            if (expected != actual) throw new InvalidOperationException($"Expected {expected}; actual {actual}.");
        }

        private static void Check(bool condition, string message)
        {
            if (!condition) throw new InvalidOperationException(message);
        }

        private static void Throws<T>(Action action) where T : Exception
        {
            try { action(); }
            catch (T) { return; }
            throw new InvalidOperationException($"Expected {typeof(T).Name}.");
        }

        private static async Task ThrowsAsync<T>(Func<Task> action) where T : Exception
        {
            try { await action(); }
            catch (T) { return; }
            throw new InvalidOperationException($"Expected {typeof(T).Name}.");
        }

        private sealed class Asset
        {
            public Asset(string name) { Name = name; }
            public string Name { get; }
        }

        private sealed class Attempt
        {
            private readonly TaskCompletionSource<Asset> _completion = new TaskCompletionSource<Asset>(TaskCreationOptions.RunContinuationsAsynchronously);
            public Attempt(string key, CancellationToken token) { Key = key; Token = token; }
            public string Key { get; }
            public CancellationToken Token { get; }
            public Task<Asset> Task => _completion.Task;
            public void Succeed(Asset asset) => _completion.SetResult(asset);
            public void Fail(Exception exception) => _completion.SetException(exception);
        }

        private sealed class ControlledProvider : IResourceProvider<string, Asset>
        {
            private readonly object _gate = new object();
            private readonly List<Attempt> _attempts = new List<Attempt>();
            private readonly List<Asset> _releases = new List<Asset>();
            private readonly List<(int Target, TaskCompletionSource<bool> Completion)> _releaseWaiters = new List<(int, TaskCompletionSource<bool>)>();
            public bool ThrowOnRelease { get; set; }
            public bool ThrowOnCancellation { get; set; }
            public int LoadCount { get { lock (_gate) return _attempts.Count; } }
            public int ReleaseCount { get { lock (_gate) return _releases.Count; } }
            public Attempt Attempt(int index) { lock (_gate) return _attempts[index]; }
            public int ReleaseCountFor(Asset asset) { lock (_gate) return _releases.Count(value => ReferenceEquals(value, asset)); }

            public Task<Asset> LoadAsync(string key, CancellationToken cancellationToken)
            {
                var attempt = new Attempt(key, cancellationToken);
                if (ThrowOnCancellation) cancellationToken.Register(() => throw new InvalidOperationException("Injected cancellation callback failure."));
                lock (_gate) _attempts.Add(attempt);
                // Deliberately ignores cancellation to exercise late-result reclamation.
                return attempt.Task;
            }

            public void Release(string key, Asset resource)
            {
                lock (_gate)
                {
                    _releases.Add(resource);
                    foreach (var waiter in _releaseWaiters)
                        if (_releases.Count >= waiter.Target) waiter.Completion.TrySetResult(true);
                }
                if (ThrowOnRelease) throw new IOException("Injected release failure.");
            }

            public Task WaitForReleaseCountAsync(int target)
            {
                lock (_gate)
                {
                    if (_releases.Count >= target) return Task.CompletedTask;
                    var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _releaseWaiters.Add((target, completion));
                    return completion.Task;
                }
            }
        }

        private sealed class SynchronousThrowProvider : IResourceProvider<string, Asset>
        {
            public Task<Asset> LoadAsync(string key, CancellationToken cancellationToken) => throw new IOException("Injected synchronous failure.");
            public void Release(string key, Asset resource) => throw new InvalidOperationException("Nothing was allocated.");
        }
    }
}
