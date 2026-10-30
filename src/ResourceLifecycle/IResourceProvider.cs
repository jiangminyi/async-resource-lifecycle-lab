using System.Threading;
using System.Threading.Tasks;

namespace ResourceLifecycle
{
    /// <summary>A provider owns allocation and releases each successful load by instance.</summary>
    public interface IResourceProvider<TKey, TResource>
        where TKey : notnull
        where TResource : class
    {
        Task<TResource> LoadAsync(TKey key, CancellationToken cancellationToken);

        /// <summary>Must release this instance, not whatever is currently stored under the key.</summary>
        void Release(TKey key, TResource resource);
    }
}
