# async-resource-lifecycle-lab

一个聚焦**资源所有权与异步竞态**的纯 C# 演示：同键加载合并、独立取消、引用租约、失效代际隔离，以及可观察的关闭排空。

这不是“把异步加载包一层”的示例。它重点回答：两个界面共享一次加载，其中一个退出怎么办？最后一个等待者取消后，底层任务仍返回资源怎么办？管理器关闭时，已经授予的租约还能不能使用？

**来源说明**：2026-10-30 新建的独立演示项目，使用 AI 辅助设计、实现和测试。代码与示例数据从零编写，不是雇主代码、商业项目源码或过去已有的开源贡献。它尚未部署到生产环境。

## 快速运行

核心库目标为 .NET Standard 2.1；控制台示例与规格测试目标为 .NET 9。本机验证工具链是 .NET SDK 9.0.304；仓库没有第三方运行时或测试框架依赖。

在仓库根目录执行：

~~~sh
dotnet build AsyncResourceLifecycleLab.sln -c Release
dotnet run --project tests/ResourceLifecycle.Specs -c Release --no-build
dotnet run --project examples/ResourceLifecycle.Demo -c Release --no-build
~~~

规格测试是自包含控制台程序，失败时返回非零退出码；不要用只运行测试发现的命令代替上述测试命令。

实际示例输出：

~~~text
两个调用方请求同一键，底层加载次数：1
面板离开：仅取消自己的等待，场景继续加载。
场景获得资源：原创示例图集（仅内存对象）
管理器关闭；已授予租约仍有效，释放次数：0
最后租约归还；释放次数：1，存活代际：0
~~~

## 结构

~~~mermaid
flowchart LR
    A[面板请求 + 独立取消] --> P[ResourcePool]
    B[场景请求] --> P
    P --> E[每个键的当前加载代际]
    E --> V[IResourceProvider]
    V --> R[一个资源实例]
    E --> L1[ResourceLease]
    E --> L2[ResourceLease]
    L1 --> D[最后引用归还]
    L2 --> D
    D --> X[按实例释放一次]
    P --> S[Snapshot / WhenDrained]
~~~

| 路径 | 作用 |
| --- | --- |
| [ResourcePool.cs](src/ResourceLifecycle/ResourcePool.cs) | 状态锁、加载代际、引用转移与关闭排空 |
| [ResourceLease.cs](src/ResourceLifecycle/ResourceLease.cs) | 幂等归还单个所有权引用 |
| [IResourceProvider.cs](src/ResourceLifecycle/IResourceProvider.cs) | 可替换的加载／释放接口 |
| [规格测试](tests/ResourceLifecycle.Specs/Program.cs) | 手动完成任务与取消竞态验证 |
| [可运行示例](examples/ResourceLifecycle.Demo/Program.cs) | 两个使用方共享加载，分别结束生命周期 |
| [设计说明](docs/design.md) | 不变量、线性化位置和取舍 |

## 生命周期契约

1. 同一键尚有引用时，共享同一个加载代际。引用既包括正在等待的请求，也包括已经授予的租约。
2. 每个请求的取消令牌只控制该次获取。它不会撤销已授予的租约，也不会单独取消其他调用方的等待。
3. 最后一个引用消失后，代际立刻从当前键映射中移除。底层取消是尽力而为；不响应取消的加载结果，仍会在返回后被释放。
4. 旧代际无法重新进入当前映射，也无法覆盖同键的新加载。释放传入**实例**，不能只按键清理一个共享的“当前对象”。
5. 管理器关闭后拒绝新请求、使尚未授予的请求失败，并请求底层取消。已授予租约仍有效，归还时才释放资源。
6. 每次成功加载只调用一次提供器释放方法。释放方法若抛错，不能保证底层资源实际释放；错误会在关闭排空结果中报告，不自动重试有副作用的释放操作。

~~~csharp
var pool = new ResourcePool<string, Asset>(provider);
using (ResourceLease<Asset> lease = await pool.AcquireAsync("ui/icons", cancellationToken))
{
    Use(lease.Value);
}

pool.Dispose();
await pool.WhenDrained;
~~~

也可以使用 IAsyncDisposable，但必须在等待管理器关闭前归还全部租约。若提供器永不返回，或租约被遗漏，排空任务会一直等待；调用方可给自己的等待加超时，超时不会凭空收回仍被使用的资源。

## 验证范围

本地 Release 构建为 **0 警告、0 错误**，**19/19 项规格通过**。覆盖：

- 64 个并发调用方共享一次加载；100 个租约从多个任务重复归还，释放只发生一次。
- 一个等待者取消不影响其他调用方；全部取消后同键可以建立新代际，旧结果被单独回收。
- 异步／同步加载异常、空结果、失败重试、不同键隔离与自定义键比较器。
- 关闭时仍在加载、关闭时已有租约，以及取消回调和释放回调异常。
- 250 次取消与完成竞争，允许“已授予租约”或“请求被取消”两种合法结果，最终都必须满足一次释放与零存活引用。

测试使用 TaskCompletionSource 精确控制完成顺序，没有用 Thread.Sleep 猜测执行时机。每项测试的 12 秒超时只是防止死锁使进程无限挂起；竞态测试也不是对所有线程调度的穷尽证明。

## 边界与未实现项

- 这是**生命周期核心库**，没有实际 Unity、Addressables、AssetBundle、纹理或主线程适配器；没有验证游戏引擎兼容性。
- 提供器方法在锁外执行，但不承诺主线程。需要主线程的 API 必须由适配器明确调度。
- 同一个租约的 Value 访问和 Dispose 不应由调用方并发执行；租约防止重复归还，不替代资源本身的线程安全设计。
- 资源归零立即卸载，不做 LRU、内存预算、加载优先级、磁盘缓存或依赖图。库不会限制不同键的并发加载数量。
- 提供器必须对每次成功加载交付一个可独立归还的所有权；若复用底层对象，提供器自身需要维护正确的引用关系。
- Release 和取消回调应快速返回；库不会强制终止阻塞代码。生产适配仍需要集成测试、性能测量和平台验证。

## 许可

[MIT](LICENSE)。
