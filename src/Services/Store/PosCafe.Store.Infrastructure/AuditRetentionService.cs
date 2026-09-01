using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using BuildingBlocks.Observability;
using BuildingBlocks.Messaging;

namespace PosCafe.Store.Infrastructure;
public sealed class AuditRetentionOptions { public int RetentionDays { get; set; } = 365; public int IntervalHours { get; set; } = 24; }
public sealed class AuditRetentionService(IServiceScopeFactory scopeFactory, IOptions<AuditRetentionOptions> options, AuditArchiveClient archive, ILogger<AuditRetentionService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(Math.Max(1, options.Value.IntervalHours)));
        do
        {
            try { using var scope = scopeFactory.CreateScope(); var db = scope.ServiceProvider.GetRequiredService<StoreDbContext>(); var entries = await db.AuditEntries.Where(x => x.OccurredAtUtc < DateTime.UtcNow.AddDays(-Math.Max(1, options.Value.RetentionDays))).OrderBy(x => x.OccurredAtUtc).Take(1000).ToListAsync(stoppingToken); await archive.ArchiveAsync(entries, stoppingToken); var ids = entries.Select(x => x.Id).ToArray(); var deleted = ids.Length == 0 ? 0 : await db.AuditEntries.Where(x => ids.Contains(x.Id)).ExecuteDeleteAsync(stoppingToken); if (deleted > 0) logger.LogInformation("Archived and deleted {Count} expired Store audit entries", deleted); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception) { MessagingMetrics.RetentionFailures.Add(1, new KeyValuePair<string, object?>("service", "store")); logger.LogError(exception, "Store audit retention failed; retrying next cycle"); }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
