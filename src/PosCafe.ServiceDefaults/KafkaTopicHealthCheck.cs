using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace PosCafe.ServiceDefaults;

public sealed class KafkaTopicHealthCheck(IAdminClient adminClient, IConfiguration configuration) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var topics = configuration.GetSection("Messaging:RequiredTopics").Get<string[]>() ?? [];
        var minimumPartitions = Math.Max(1, configuration.GetValue("Messaging:MinimumTopicPartitions", 1));
        if (topics.Length == 0) return Task.FromResult(HealthCheckResult.Degraded("No required Kafka topics are configured."));

        try
        {
            var missing = new List<string>();
            var underPartitioned = new List<string>();
            foreach (var topic in topics.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Trim()).Distinct(StringComparer.Ordinal))
            {
                var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(3)).Topics.SingleOrDefault();
                if (metadata is null || metadata.Error.IsError) { missing.Add(topic); continue; }
                if (metadata.Partitions.Count < minimumPartitions) underPartitioned.Add($"{topic}={metadata.Partitions.Count}");
            }

            if (missing.Count > 0 || underPartitioned.Count > 0)
                return Task.FromResult(HealthCheckResult.Unhealthy($"Kafka topic verification failed. Missing: {string.Join(",", missing)}; partitions below {minimumPartitions}: {string.Join(",", underPartitioned)}."));
            return Task.FromResult(HealthCheckResult.Healthy());
        }
        catch (Exception exception)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka topic metadata is unavailable or ACL denied.", exception));
        }
    }
}
