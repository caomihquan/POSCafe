using BuildingBlocks.Observability;
using BuildingBlocks.Messaging;
using Microsoft.EntityFrameworkCore;

namespace PosCafe.ApiGateway;

public sealed class OpsAuditRetentionOptions
{
    public int RetentionDays { get; set; } = 365;
    public int IntervalHours { get; set; } = 24;
    public int BatchSize { get; set; } = 1000;
}

public sealed class OpsAuditRetentionService(IServiceScopeFactory scopeFactory, IConfiguration configuration, AuditArchiveClient archive, ILogger<OpsAuditRetentionService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = configuration.GetSection("Audit").Get<OpsAuditRetentionOptions>() ?? new();
        using var timer = new PeriodicTimer(TimeSpan.FromHours(Math.Max(1, options.IntervalHours)));
        do
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<OpsDbContext>();
                var cutoff = DateTime.UtcNow.AddDays(-Math.Max(1, options.RetentionDays));
                var entries = await db.AuditEntries.Where(x => x.OccurredAtUtc < cutoff).OrderBy(x => x.OccurredAtUtc).Take(Math.Clamp(options.BatchSize, 1, 10000)).ToListAsync(stoppingToken);
                await archive.ArchiveAsync(entries, stoppingToken);
                var ids = entries.Select(x => x.Id).ToArray();
                var deleted = ids.Length == 0 ? 0 : await db.AuditEntries.Where(x => ids.Contains(x.Id)).ExecuteDeleteAsync(stoppingToken);
                if (deleted > 0) MessagingMetrics.AuditPurgedRecords.Add(deleted, new KeyValuePair<string, object?>("service", "gateway"));
                if (deleted > 0) logger.LogInformation("Archived and deleted {Count} expired Gateway audit entries", deleted);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception) { MessagingMetrics.RetentionFailures.Add(1, new KeyValuePair<string, object?>("service", "gateway")); logger.LogError(exception, "Gateway audit retention failed; retrying next cycle"); }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
