namespace ResourceLifecycle
{
    /// <summary>A lock-consistent diagnostic snapshot; never contains resource instances.</summary>
    public sealed class PoolSnapshot
    {
        internal PoolSnapshot(bool closed, int activeKeys, int generations, int loads, int references, int cleanupErrors)
        {
            IsClosed = closed;
            ActiveKeys = activeKeys;
            LiveGenerations = generations;
            PendingLoads = loads;
            OutstandingReferences = references;
            CleanupErrorCount = cleanupErrors;
        }

        public bool IsClosed { get; }
        public int ActiveKeys { get; }
        public int LiveGenerations { get; }
        public int PendingLoads { get; }
        public int OutstandingReferences { get; }
        public int CleanupErrorCount { get; }
    }
}
