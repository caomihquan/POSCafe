using System.Text;
using BuildingBlocks.Messaging;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PosCafe.Inventory.Infrastructure.Messaging;

public sealed class InventoryOutboxPublisher(
    IServiceScopeFactory scopeFactory,
    IProducer<string, string> producer,
    IOptions<OutboxOptions> options,
    ILogger<InventoryOutboxPublisher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await PublishBatchAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception) { logger.LogError(exception, "Inventory outbox publisher failed"); }
            await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, options.Value.PollSeconds)), stoppingToken);
        }
    }

    private async Task PublishBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
        var now = DateTime.UtcNow;
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var messages = await db.OutboxMessages.FromSqlRaw("""
            SELECT "Id", "EventType", "AggregateId", "Payload", "OccurredOnUtc", "CorrelationId", "Attempts", "ProcessedOnUtc", "LockedUntilUtc", "DeadLetteredOnUtc", "Error"
            FROM outbox_messages
            WHERE "ProcessedOnUtc" IS NULL AND "DeadLetteredOnUtc" IS NULL
              AND "Attempts" < {0} AND ("LockedUntilUtc" IS NULL OR "LockedUntilUtc" < {1})
            ORDER BY "OccurredOnUtc"
            LIMIT {2}
            FOR UPDATE SKIP LOCKED
            """, options.Value.MaxAttempts, now, options.Value.BatchSize).AsTracking().ToListAsync(cancellationToken);
        foreach (var message in messages) { message.LockedUntilUtc = now.AddSeconds(options.Value.LeaseSeconds); message.Attempts++; }
        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        foreach (var message in messages)
        {
            try
            {
                await producer.ProduceAsync(options.Value.Topic, new Message<string, string>
                {
                    Key = message.AggregateId,
                    Value = message.Payload,
                    Headers = new Headers
                    {
                        { "event-type", Encoding.UTF8.GetBytes(message.EventType) },
                        { "event-id", Encoding.UTF8.GetBytes(message.Id.ToString()) },
                        { "correlation-id", Encoding.UTF8.GetBytes(message.CorrelationId ?? message.Id.ToString("N")) },
                        { "causation-id", Encoding.UTF8.GetBytes(message.Id.ToString()) },
                        { "schema-version", Encoding.UTF8.GetBytes("1") },
                        { "schema-id", Encoding.UTF8.GetBytes(IntegrationSchemaIds.ForEventType(message.EventType)) }
                    }
                }, cancellationToken);
                message.ProcessedOnUtc = DateTime.UtcNow;
                message.LockedUntilUtc = null;
                message.Error = null;
            }
            catch (Exception exception)
            {
                message.Error = exception.Message;
                if (message.Attempts >= options.Value.MaxAttempts && !string.IsNullOrWhiteSpace(options.Value.DeadLetterTopic))
                {
                    try
                    {
                        await producer.ProduceAsync(options.Value.DeadLetterTopic, new Message<string, string>
                        {
                            Key = message.AggregateId,
                            Value = message.Payload,
                            Headers = new Headers
                            {
                                { "event-type", Encoding.UTF8.GetBytes(message.EventType) },
                                { "event-id", Encoding.UTF8.GetBytes(message.Id.ToString()) },
                                { "dead-letter-reason", Encoding.UTF8.GetBytes(exception.Message) },
                                { "original-topic", Encoding.UTF8.GetBytes(options.Value.Topic) }
                            }
                        }, cancellationToken);
                        message.DeadLetteredOnUtc = DateTime.UtcNow;
                        message.LockedUntilUtc = null;
                    }
                    catch (Exception deadLetterException)
                    {
                        message.LockedUntilUtc = DateTime.UtcNow.AddSeconds(Math.Min(300, Math.Pow(2, message.Attempts)));
                        logger.LogError(deadLetterException, "Failed to publish inventory outbox message {MessageId} to DLQ", message.Id);
                    }
                }
                else
                {
                    message.LockedUntilUtc = DateTime.UtcNow.AddSeconds(Math.Min(300, Math.Pow(2, message.Attempts)));
                }
                logger.LogError(exception, "Failed to publish inventory outbox message {MessageId}", message.Id);
            }
        }
        await db.SaveChangesAsync(cancellationToken);
    }
}
