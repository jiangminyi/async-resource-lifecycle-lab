using System;
using System.Threading;
using System.Threading.Tasks;
using ResourceLifecycle;

namespace ResourceLifecycle.Demo
{
    internal static class Program
    {
        private static async Task Main()
        {
            var provider = new DemoProvider();
            var pool = new ResourcePool<string, DemoAsset>(provider);
            using var closedPanel = new CancellationTokenSource();
            Task<ResourceLease<DemoAsset>> panel = pool.AcquireAsync("ui/shared-atlas", closedPanel.Token);
            Task<ResourceLease<DemoAsset>> scene = pool.AcquireAsync("ui/shared-atlas");
            Console.WriteLine($"两个调用方请求同一键，底层加载次数：{provider.LoadCount}");
            closedPanel.Cancel();
            try { await panel; }
            catch (OperationCanceledException) { Console.WriteLine("面板离开：仅取消自己的等待，场景继续加载。"); }
            provider.Complete(new DemoAsset("原创示例图集（仅内存对象）"));
            ResourceLease<DemoAsset> lease = await scene;
            Console.WriteLine($"场景获得资源：{lease.Value.Name}");
            pool.Dispose();
            Console.WriteLine($"管理器关闭；已授予租约仍有效，释放次数：{provider.ReleaseCount}");
            lease.Dispose();
            await pool.WhenDrained;
            Console.WriteLine($"最后租约归还；释放次数：{provider.ReleaseCount}，存活代际：{pool.Snapshot.LiveGenerations}");
        }

        private sealed class DemoAsset
        {
            public DemoAsset(string name) { Name = name; }
            public string Name { get; }
        }

        private sealed class DemoProvider : IResourceProvider<string, DemoAsset>
        {
            private readonly TaskCompletionSource<DemoAsset> _completion = new TaskCompletionSource<DemoAsset>(TaskCreationOptions.RunContinuationsAsynchronously);
            public int LoadCount { get; private set; }
            public int ReleaseCount { get; private set; }
            public Task<DemoAsset> LoadAsync(string key, CancellationToken cancellationToken)
            {
                LoadCount++;
                return _completion.Task;
            }
            public void Complete(DemoAsset asset) => _completion.SetResult(asset);
            public void Release(string key, DemoAsset resource) { ReleaseCount++; }
        }
    }
}
