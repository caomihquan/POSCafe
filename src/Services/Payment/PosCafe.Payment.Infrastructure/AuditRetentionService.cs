using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PosCafe.Payment.Infrastructure.Persistence;
using BuildingBlocks.Observability;
using BuildingBlocks.Messaging;

namespace PosCafe.Payment.Infrastructure;
public sealed class AuditRetentionOptions { public int RetentionDays { get; set; } = 365; public int IntervalHours { get; set; } = 24; }
public sealed class AuditRetentionService(IServiceScopeFactory scopeFactory, IOptions<AuditRetentionOptions> options, AuditArchiveClient archive, ILogger<AuditRetentionService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(Math.Max(1, options.Value.IntervalHours)));
        do
        {
            try { using var scope = scopeFactory.CreateScope(); var db = scope.ServiceProvider.GetRequiredService<PaymentDbContext>(); var cutoff = DateTime.UtcNow.AddDays(-Math.Max(1, options.Value.RetentionDays)); var entries = await db.AuditEntries.Where(x => x.OccurredAtUtc < cutoff).OrderBy(x => x.OccurredAtUtc).Take(1000).ToListAsync(stoppingToken); await archive.ArchiveAsync(entries, stoppingToken); var ids = entries.Select(x => x.Id).ToArray(); var deleted = ids.Length == 0 ? 0 : await db.AuditEntries.Where(x => ids.Contains(x.Id)).ExecuteDeleteAsync(stoppingToken); var idempotencyDeleted = await db.PaymentIdempotencyRecords.Where(x => x.CreatedAtUtc < cutoff).Take(1000).ExecuteDeleteAsync(stoppingToken); if (idempotencyDeleted > 0) MessagingMetrics.IdempotencyPurgedRecords.Add(idempotencyDeleted, new KeyValuePair<string, object?>("service", "payment")); if (deleted > 0) logger.LogInformation("Archived and deleted {Count} expired Payment audit entries", deleted); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception) { MessagingMetrics.RetentionFailures.Add(1, new KeyValuePair<string, object?>("service", "payment")); logger.LogError(exception, "Payment audit retention failed; retrying next cycle"); }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
