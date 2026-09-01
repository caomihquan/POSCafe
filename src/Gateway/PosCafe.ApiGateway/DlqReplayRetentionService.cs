using BuildingBlocks.Messaging;
using Microsoft.EntityFrameworkCore;

namespace PosCafe.ApiGateway;

public sealed class DlqReplayRetentionOptions
{
    public int RetentionDays { get; set; } = 90;
    public int IntervalHours { get; set; } = 24;
    public int BatchSize { get; set; } = 1000;
}

public sealed class DlqReplayRetentionService(IServiceScopeFactory scopeFactory, IConfiguration configuration, ILogger<DlqReplayRetentionService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = configuration.GetSection("Dlq:ReplayHistory").Get<DlqReplayRetentionOptions>() ?? new();
        using var timer = new PeriodicTimer(TimeSpan.FromHours(Math.Max(1, options.IntervalHours)));
        do
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<OpsDbContext>();
                var cutoff = DateTime.UtcNow.AddDays(-Math.Max(1, options.RetentionDays));
                var expiredPending = await db.DlqReplays.Where(x => x.Status == "Pending" && x.LeaseUntilUtc < DateTime.UtcNow).ExecuteUpdateAsync(setters => setters
                    .SetProperty(x => x.Status, "Failed")
                    .SetProperty(x => x.Error, "Replay lease expired; the Gateway may have terminated before completion.")
                    .SetProperty(x => x.LeaseUntilUtc, (DateTime?)null), stoppingToken);
                if (expiredPending > 0) logger.LogWarning("Marked {Count} expired DLQ replay leases as failed", expiredPending);
                var ids = await db.DlqReplays.Where(x => x.CreatedAtUtc < cutoff).OrderBy(x => x.CreatedAtUtc).Take(Math.Clamp(options.BatchSize, 1, 10000)).Select(x => x.Id).ToListAsync(stoppingToken);
                if (ids.Count > 0)
                {
                    var deleted = await db.DlqReplays.Where(x => ids.Contains(x.Id)).ExecuteDeleteAsync(stoppingToken);
                    logger.LogInformation("Deleted {Count} expired DLQ replay history records", deleted);
                }
                var state = await db.DlqReplays.AsNoTracking().GroupBy(x => x.Status).Select(group => new { group.Key, Count = group.LongCount() }).ToListAsync(stoppingToken);
                MessagingMetrics.UpdateDlqState(state.SingleOrDefault(x => x.Key == "Pending")?.Count ?? 0, state.SingleOrDefault(x => x.Key == "Failed")?.Count ?? 0, state.SingleOrDefault(x => x.Key == "NotFound")?.Count ?? 0, state.SingleOrDefault(x => x.Key == "Completed")?.Count ?? 0);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception)
            {
                MessagingMetrics.DlqReplayRetentionFailures.Add(1);
                logger.LogError(exception, "DLQ replay history retention failed; retrying next cycle");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
