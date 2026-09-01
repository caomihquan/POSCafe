using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace PosCafe.Payment.Infrastructure.Messaging;

public sealed class KafkaHealthCheck(IAdminClient adminClient) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try { adminClient.GetMetadata(TimeSpan.FromSeconds(2)); return Task.FromResult(HealthCheckResult.Healthy()); }
        catch (Exception exception) { return Task.FromResult(HealthCheckResult.Unhealthy("Kafka broker is unavailable.", exception)); }
    }
}
