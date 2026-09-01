using BuildingBlocks.Observability;
using Confluent.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;

namespace PosCafe.ApiGateway;

public sealed class OpsKafkaHealthCheck(IAdminClient adminClient) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try { adminClient.GetMetadata(TimeSpan.FromSeconds(2)); return Task.FromResult(HealthCheckResult.Healthy()); }
        catch (Exception exception) { return Task.FromResult(HealthCheckResult.Unhealthy("Kafka broker is unavailable.", exception)); }
    }
}

public sealed class AuditArchiveConfigurationHealthCheck(AuditArchiveOptions options) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        Task.FromResult(!options.Enabled || !string.IsNullOrWhiteSpace(options.ConnectionString)
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy("Audit archive is enabled but its connection string is missing."));
}

public sealed class PendingReplayHealthCheck(OpsDbContext db, IConfiguration configuration) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var maxAgeMinutes = Math.Max(1, configuration.GetValue("Dlq:ReplayHistory:PendingMaxAgeMinutes", 10));
        var oldest = await db.DlqReplays.AsNoTracking().Where(x => x.Status == "Pending").OrderBy(x => x.CreatedAtUtc).Select(x => (DateTime?)x.CreatedAtUtc).FirstOrDefaultAsync(cancellationToken);
        if (oldest is null) return HealthCheckResult.Healthy("No pending DLQ replay claims.");
        var age = DateTime.UtcNow - oldest.Value;
        return age > TimeSpan.FromMinutes(maxAgeMinutes)
            ? HealthCheckResult.Unhealthy($"Oldest pending DLQ replay is {age.TotalMinutes:F1} minutes old.")
            : HealthCheckResult.Healthy($"Oldest pending DLQ replay is {age.TotalMinutes:F1} minutes old.");
    }
}

public sealed class AuditRetentionFreshnessHealthCheck(OpsDbContext db, IConfiguration configuration) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var maxAgeDays = Math.Max(1, configuration.GetValue("Audit:RetentionMaxAgeDays", 2));
        var oldest = await db.AuditEntries.AsNoTracking().OrderBy(x => x.OccurredAtUtc).Select(x => (DateTime?)x.OccurredAtUtc).FirstOrDefaultAsync(cancellationToken);
        if (oldest is null) return HealthCheckResult.Healthy("No Gateway audit entries require retention.");
        var age = DateTime.UtcNow - oldest.Value;
        return age > TimeSpan.FromDays(maxAgeDays)
            ? HealthCheckResult.Degraded($"Oldest Gateway audit entry is {age.TotalDays:F1} days old.")
            : HealthCheckResult.Healthy();
    }
}
