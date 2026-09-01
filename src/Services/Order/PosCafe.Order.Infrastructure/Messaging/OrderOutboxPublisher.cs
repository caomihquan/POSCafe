using System.Text.Json;
using BuildingBlocks.Messaging;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PosCafe.Order.Infrastructure.Persistence;

namespace PosCafe.Order.Infrastructure.Messaging;

public sealed class OrderOutboxPublisher(
    IServiceScopeFactory scopeFactory,
    IProducer<string, string> producer,
    IOptions<OutboxOptions> options,
    ILogger<OrderOutboxPublisher> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try { await PublishBatchAsync(stoppingToken); }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
            catch (Exception exception) { logger.LogError(exception, "Order outbox publisher failed"); }
            await Task.Delay(TimeSpan.FromSeconds(options.Value.PollSeconds), stoppingToken);
        }
    }

    private async Task PublishBatchAsync(CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        var now = DateTime.UtcNow;
        await using var claimTransaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var messages = await db.OutboxMessages.FromSqlRaw("""
            SELECT "Id", "EventType", "AggregateId", "Payload", "OccurredOnUtc", "CorrelationId", "Attempts", "ProcessedOnUtc", "LockedUntilUtc", "DeadLetteredOnUtc", "Error"
            FROM outbox_messages
            WHERE "ProcessedOnUtc" IS NULL AND "DeadLetteredOnUtc" IS NULL
              AND "Attempts" < {0} AND ("LockedUntilUtc" IS NULL OR "LockedUntilUtc" < {1})
            ORDER BY "OccurredOnUtc"
            LIMIT {2}
            FOR UPDATE SKIP LOCKED
            """, options.Value.MaxAttempts, now, options.Value.BatchSize).AsTracking().ToListAsync(cancellationToken);
        foreach (var message in messages)
        {
            message.LockedUntilUtc = now.AddSeconds(options.Value.LeaseSeconds);
            message.Attempts++;
        }
        await db.SaveChangesAsync(cancellationToken);
        await claimTransaction.CommitAsync(cancellationToken);
        foreach (var message in messages)
        {
            try
            {
                using var activity = MessagingTelemetry.ActivitySource.StartActivity("messaging.publish", System.Diagnostics.ActivityKind.Producer);
                activity?.SetTag("messaging.system", "kafka");
                activity?.SetTag("messaging.destination.name", options.Value.Topic);
                activity?.SetTag("messaging.message.id", message.Id);
                activity?.SetTag("messaging.destination.partition.key", message.AggregateId);
                var schemaId = IntegrationSchemaIds.ForEventType(message.EventType);
                await producer.ProduceAsync(options.Value.Topic, new Message<string, string> { Key = message.AggregateId, Value = message.Payload, Headers = new Headers { { "event-type", System.Text.Encoding.UTF8.GetBytes(message.EventType) }, { "event-id", System.Text.Encoding.UTF8.GetBytes(message.Id.ToString()) }, { "correlation-id", System.Text.Encoding.UTF8.GetBytes(message.CorrelationId ?? message.Id.ToString("N")) }, { "causation-id", System.Text.Encoding.UTF8.GetBytes(message.Id.ToString()) }, { "traceparent", System.Text.Encoding.UTF8.GetBytes(activity?.Id ?? string.Empty) }, { "schema-version", System.Text.Encoding.UTF8.GetBytes("1") }, { "schema-id", System.Text.Encoding.UTF8.GetBytes(schemaId) } } }, cancellationToken);
                message.ProcessedOnUtc = DateTime.UtcNow;
                message.LockedUntilUtc = null;
                message.Error = null;
                MessagingMetrics.Published.Add(1, new KeyValuePair<string, object?>("service", "order"));
            }
            catch (Exception exception)
            {
                var reachedLimit = message.Attempts >= options.Value.MaxAttempts;
                if (reachedLimit && !string.IsNullOrWhiteSpace(options.Value.DeadLetterTopic))
                {
                    try
                    {
                        await producer.ProduceAsync(options.Value.DeadLetterTopic, new Message<string, string>
                        {
                            Key = message.AggregateId,
                            Value = message.Payload,
                            Headers = new Headers
                            {
                                { "event-type", System.Text.Encoding.UTF8.GetBytes(message.EventType) },
                                { "event-id", System.Text.Encoding.UTF8.GetBytes(message.Id.ToString()) },
                                { "error", System.Text.Encoding.UTF8.GetBytes(exception.Message) }
                            }
                        }, cancellationToken);
                        message.DeadLetteredOnUtc = DateTime.UtcNow;
                        message.LockedUntilUtc = null;
                        MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "order"));
                    }
                    catch (Exception deadLetterException)
                    {
                        logger.LogError(deadLetterException, "Failed to publish order outbox message {MessageId} to DLQ; retaining retry lease", message.Id);
                        message.LockedUntilUtc = DateTime.UtcNow.AddSeconds(Math.Min(300, Math.Pow(2, message.Attempts)));
                    }
                }
                else if (reachedLimit)
                {
                    message.DeadLetteredOnUtc = DateTime.UtcNow;
                    message.LockedUntilUtc = null;
                    MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "order"));
                }
                else
                {
                    message.LockedUntilUtc = DateTime.UtcNow.AddSeconds(Math.Min(300, Math.Pow(2, message.Attempts)));
                }

                message.Error = exception.Message;
                MessagingMetrics.PublishFailures.Add(1, new KeyValuePair<string, object?>("service", "order"));
                logger.LogWarning(exception, "Failed to publish order outbox message {MessageId}", message.Id);
            }
        }
        await db.SaveChangesAsync(cancellationToken);
    }
}
