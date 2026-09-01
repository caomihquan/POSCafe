namespace BuildingBlocks.Messaging;

public static class KafkaConsumerLagState
{
    private static readonly object Sync = new();
    private static readonly Dictionary<string, (long Lag, DateTime UpdatedAtUtc)> Values = new(StringComparer.Ordinal);

    public static void Record(string topic, int partition, long lag)
    {
        lock (Sync) Values[$"{topic}:{partition}"] = (lag, DateTime.UtcNow);
    }

    public static IReadOnlyCollection<(string Key, long Lag, DateTime UpdatedAtUtc)> Snapshot()
    {
        lock (Sync) return Values.Select(x => (x.Key, x.Value.Lag, x.Value.UpdatedAtUtc)).ToArray();
    }
}
