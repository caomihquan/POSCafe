using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using BuildingBlocks.Messaging;

namespace PosCafe.Identity.Infrastructure;

public sealed class RefreshTokenCleanupOptions { public int IntervalHours { get; set; } = 6; public int RevokedRetentionDays { get; set; } = 7; }
public sealed class RefreshTokenCleanupService(IServiceScopeFactory scopeFactory, IOptions<RefreshTokenCleanupOptions> options, ILogger<RefreshTokenCleanupService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromHours(Math.Max(1, options.Value.IntervalHours)));
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<IdentityDbContext>();
                var cutoff = DateTime.UtcNow;
                var deleted = await db.RefreshTokens
                    .Where(x => x.ExpiresAtUtc < cutoff || x.RevokedAtUtc < cutoff.AddDays(-Math.Max(1, options.Value.RevokedRetentionDays)))
                    .ExecuteDeleteAsync(stoppingToken);
                var idempotencyDeleted = await db.IdentityIdempotencyRecords
                    .Where(x => x.CreatedAtUtc < cutoff.AddDays(-Math.Max(1, options.Value.RevokedRetentionDays)))
                    .Take(1000)
                    .ExecuteDeleteAsync(stoppingToken);
                if (idempotencyDeleted > 0) MessagingMetrics.IdempotencyPurgedRecords.Add(idempotencyDeleted, new KeyValuePair<string, object?>("service", "identity"));
                if (deleted > 0) logger.LogInformation("Removed {Count} expired or revoked refresh tokens", deleted);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception) { logger.LogError(exception, "Refresh token cleanup failed"); }
        }
    }
}
