using BuildingBlocks.Messaging;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PosCafe.Store.Infrastructure;

public sealed class IdempotencyRetentionOptions
{
    public int RetentionDays { get; set; } = 7;
    public int IntervalHours { get; set; } = 6;
    public int BatchSize { get; set; } = 1000;
}

public sealed class StoreIdempotencyRetentionService(IServiceScopeFactory scopeFactory, IOptions<IdempotencyRetentionOptions> options, ILogger<StoreIdempotencyRetentionService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(Math.Max(1, options.Value.IntervalHours)));
        do
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<StoreDbContext>();
                var cutoff = DateTime.UtcNow.AddDays(-Math.Max(1, options.Value.RetentionDays));
                var deleted = await db.StoreIdempotencyRecords.Where(x => x.CreatedAtUtc < cutoff).OrderBy(x => x.CreatedAtUtc).Take(Math.Clamp(options.Value.BatchSize, 100, 10000)).ExecuteDeleteAsync(stoppingToken);
                if (deleted > 0) MessagingMetrics.IdempotencyPurgedRecords.Add(deleted, new KeyValuePair<string, object?>("service", "store"));
                if (deleted > 0) logger.LogInformation("Deleted {Count} expired Store idempotency records", deleted);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                MessagingMetrics.RetentionFailures.Add(1, new KeyValuePair<string, object?>("service", "store-idempotency"));
                logger.LogError(exception, "Store idempotency retention failed; retrying next cycle");
            }
        } while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
