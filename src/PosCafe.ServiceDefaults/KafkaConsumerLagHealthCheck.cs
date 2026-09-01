using BuildingBlocks.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace PosCafe.ServiceDefaults;

public sealed class KafkaConsumerLagHealthCheck(IConfiguration configuration) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var maxLag = Math.Max(0, configuration.GetValue("Messaging:ConsumerLag:MaxLag", 1000));
        var maxAgeSeconds = Math.Max(5, configuration.GetValue("Messaging:ConsumerLag:MaxSnapshotAgeSeconds", 120));
        var snapshot = KafkaConsumerLagState.Snapshot();
        if (snapshot.Count == 0)
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka consumer has not reported a lag snapshot yet."));

        var stale = snapshot.Where(x => DateTime.UtcNow - x.UpdatedAtUtc > TimeSpan.FromSeconds(maxAgeSeconds)).ToArray();
        var high = snapshot.Where(x => x.Lag > maxLag).ToArray();
        if (high.Length > 0) return Task.FromResult(HealthCheckResult.Unhealthy($"Kafka consumer lag exceeded {maxLag}: {string.Join(", ", high.Select(x => $"{x.Key}={x.Lag}"))}"));
        if (stale.Length > 0) return Task.FromResult(HealthCheckResult.Degraded($"Kafka consumer lag snapshot is stale: {string.Join(", ", stale.Select(x => x.Key))}"));
        return Task.FromResult(HealthCheckResult.Healthy());
    }
}
