using System.Text;
using System.Text.Json;
using BuildingBlocks.Exceptions;
using BuildingBlocks.Messaging;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PosCafe.Inventory.Infrastructure;

public sealed class InventoryMessagingOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string InputTopic { get; set; } = "pos.order.events";
    public string ConsumerGroup { get; set; } = "pos-inventory-order-events-v1";
    public int ConsumerRetrySeconds { get; set; } = 5;
    public int ConsumerMaxAttempts { get; set; } = 5;
    public int RetryMaxSeconds { get; set; } = 300;
    public string DeadLetterTopic { get; set; } = "pos.inventory.order-events.dlq";
}

public sealed class InventoryOrderEventsConsumer(IServiceScopeFactory scopeFactory, IProducer<string, string> producer, IOptions<InventoryMessagingOptions> options, IConfiguration configuration, ILogger<InventoryOrderEventsConsumer> logger) : BackgroundService
{
    private sealed record OrderLineSnapshot(Guid ProductId, decimal Quantity);
    private sealed record OrderConfirmedEvent(Guid OrderId, Guid StoreId, decimal Total, DateTimeOffset OccurredAt, IReadOnlyCollection<OrderLineSnapshot> Lines);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (string.IsNullOrWhiteSpace(settings.BootstrapServers) || string.IsNullOrWhiteSpace(settings.InputTopic) || string.IsNullOrWhiteSpace(settings.ConsumerGroup) || string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
            throw new InvalidOperationException("Inventory Kafka consumer requires BootstrapServers, InputTopic, ConsumerGroup, and DeadLetterTopic.");
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers, GroupId = settings.ConsumerGroup,
            EnableAutoCommit = false, AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false, EnablePartitionEof = false,
            IsolationLevel = IsolationLevel.ReadCommitted,
            MaxPollIntervalMs = Math.Clamp((settings.RetryMaxSeconds * 1000) + 120_000, 300_000, 1_800_000)
        };
        KafkaProducerConfiguration.ApplySecurity(consumerConfig, configuration.GetSection("Kafka:Security"));
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig)
        .SetPartitionsAssignedHandler((_, partitions) => { logger.LogInformation("Inventory consumer assigned {Partitions}", string.Join(",", partitions)); return partitions.Select(partition => new TopicPartitionOffset(partition, Offset.Unset)); })
        .SetPartitionsRevokedHandler((_, partitions) => logger.LogWarning("Inventory consumer partitions revoked: {Partitions}", string.Join(",", partitions)))
        .SetPartitionsLostHandler((_, partitions) => logger.LogError("Inventory consumer partitions lost: {Partitions}", string.Join(",", partitions)))
        .Build();
        consumer.Subscribe(options.Value.InputTopic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                RecordLag(consumer, result, "inventory", options.Value.InputTopic);
                var typeHeader = result.Message.Headers.FirstOrDefault(x => x.Key == "event-type");
                var eventType = typeHeader is null ? string.Empty : Encoding.UTF8.GetString(typeHeader.GetValueBytes());
                if (eventType != OrderFulfillmentSagaEventTypes.InventoryReservationRequested)
                {
                    consumer.Commit(result);
                    continue;
                }
                var idHeader = result.Message.Headers.FirstOrDefault(x => x.Key == "event-id");
                var schemaHeader = result.Message.Headers.FirstOrDefault(x => x.Key == "schema-version");
                var schemaIdHeader = result.Message.Headers.FirstOrDefault(x => x.Key == "schema-id");
                if (idHeader is null || typeHeader is null || schemaHeader is null || schemaIdHeader is null || Encoding.UTF8.GetString(schemaHeader.GetValueBytes()) != "1" || Encoding.UTF8.GetString(schemaIdHeader.GetValueBytes()) != "inventory-reservation-requested.v1")
                {
                    await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(result, "inventory", "Missing or unsupported event headers."), stoppingToken);
                    MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "inventory"));
                    logger.LogError("Moved poison message at {Offset} to DLQ: missing or unsupported event headers", result.TopicPartitionOffset);
                    consumer.Commit(result);
                    continue;
                }
                if (!Guid.TryParse(Encoding.UTF8.GetString(idHeader.GetValueBytes()), out var eventId))
                {
                    await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(result, "inventory", "Invalid event-id header."), stoppingToken);
                    MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "inventory"));
                    logger.LogError("Moved poison message at {Offset} to DLQ: invalid event-id", result.TopicPartitionOffset);
                    consumer.Commit(result);
                    continue;
                }
                var attempt = 0;
                InventoryReservationRequested? requested = null;
                try
                {
                    var traceparent = result.Message.Headers.FirstOrDefault(x => x.Key == "traceparent") is { } traceHeader ? Encoding.UTF8.GetString(traceHeader.GetValueBytes()) : null;
                    System.Diagnostics.ActivityContext.TryParse(traceparent, null, true, out var parentContext);
                    using var activity = MessagingTelemetry.ActivitySource.StartActivity("messaging.process", System.Diagnostics.ActivityKind.Consumer, parentContext);
                    using var scope = scopeFactory.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
                    attempt = await InboxProcessor.RegisterAttemptAsync(db, eventId, "inventory.order-events.v1", stoppingToken);
                    IntegrationPayloadValidator.Validate(eventType, result.Message.Value);
                    requested = JsonSerializer.Deserialize<InventoryReservationRequested>(result.Message.Value) ?? throw new InvalidOperationException("InventoryReservationRequested payload is invalid.");
                    await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);
                    if (!await InboxProcessor.TryStartAsync(db, eventId, "inventory.order-events.v1", stoppingToken))
                    {
                        await transaction.CommitAsync(stoppingToken);
                        consumer.Commit(result);
                        continue;
                    }
                    foreach (var line in requested.Lines)
                    {
                        var stock = await db.StockItems.SingleOrDefaultAsync(x => x.StoreId == requested.StoreId && x.ProductId == line.ProductId, stoppingToken) ?? throw new InvalidOperationException($"Stock is not configured for product {line.ProductId}.");
                        stock.Reserve(line.Quantity);
                    }
                    var correlationId = result.Message.Headers.FirstOrDefault(x => x.Key == "correlation-id") is { } correlationHeader ? Encoding.UTF8.GetString(correlationHeader.GetValueBytes()) : eventId.ToString("N");
                    db.OutboxMessages.Add(new OutboxMessage
                    {
                        Id = Guid.NewGuid(), AggregateId = requested.OrderId.ToString(), EventType = OrderFulfillmentSagaEventTypes.InventoryReserved,
                        Payload = JsonSerializer.Serialize(new InventoryReserved(requested.SagaId, requested.OrderId, requested.StoreId, DateTimeOffset.UtcNow)),
                        OccurredOnUtc = DateTime.UtcNow, CorrelationId = correlationId
                    });
                    await db.SaveChangesAsync(stoppingToken);
                    await InboxProcessor.MarkProcessedAsync(db, eventId, "inventory.order-events.v1", stoppingToken);
                    await transaction.CommitAsync(stoppingToken);
                    consumer.Commit(result);
                }
                catch (Exception exception) when (!stoppingToken.IsCancellationRequested)
                {
                    var correlationId = result.Message.Headers.FirstOrDefault(x => x.Key == "correlation-id") is { } correlationHeader ? Encoding.UTF8.GetString(correlationHeader.GetValueBytes()) : eventId.ToString("N");
                    if (requested is not null && (exception is ConflictException or InvalidOperationException))
                    {
                        await SaveReservationFailureAsync(requested, eventId, correlationId, exception.Message, stoppingToken);
                        consumer.Commit(result);
                    }
                    else if (attempt >= Math.Max(1, settings.ConsumerMaxAttempts) && !string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
                    {
                        var headers = new Headers();
                        foreach (var header in result.Message.Headers)
                            headers.Add(header.Key, header.GetValueBytes());
                        headers.Add("dead-letter-reason", Encoding.UTF8.GetBytes(exception.Message));
                        headers.Add("dead-lettered-by", Encoding.UTF8.GetBytes("inventory"));
                        headers.Add("dead-letter-attempt", Encoding.UTF8.GetBytes(attempt.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        headers.Add("original-topic", Encoding.UTF8.GetBytes(result.Topic));
                        headers.Add("original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        headers.Add("original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        await producer.ProduceAsync(settings.DeadLetterTopic, new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers }, stoppingToken);
                        using var deadLetterScope = scopeFactory.CreateScope();
                        var deadLetterDb = deadLetterScope.ServiceProvider.GetRequiredService<InventoryDbContext>();
                        await InboxProcessor.MarkDeadLetteredAsync(deadLetterDb, eventId, "inventory.order-events.v1", exception.Message, stoppingToken);
                        MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "inventory"));
                        logger.LogError(exception, "Moved order event {EventId} to DLQ after {Attempts} attempts", eventId, attempt);
                        consumer.Commit(result);
                    }
                    else
                    {
                        logger.LogError(exception, "Failed to process order event {EventId} on attempt {Attempt}; Kafka offset will not be committed", eventId, attempt);
                        var delaySeconds = Math.Min(Math.Max(1, settings.RetryMaxSeconds), Math.Max(1, settings.ConsumerRetrySeconds) * Math.Pow(2, Math.Min(Math.Max(attempt - 1, 0), 6)));
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        catch (Exception exception) { logger.LogError(exception, "Inventory order-event consumer stopped unexpectedly"); throw; }
        finally { consumer.Close(); }
    }

    private async Task SaveReservationFailureAsync(InventoryReservationRequested request, Guid eventId, string correlationId, string reason, CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        db.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.NewGuid(), AggregateId = request.OrderId.ToString(), EventType = OrderFulfillmentSagaEventTypes.InventoryReservationFailed,
            Payload = JsonSerializer.Serialize(new InventoryReservationFailed(request.SagaId, request.OrderId, request.StoreId, reason, DateTimeOffset.UtcNow)),
            OccurredOnUtc = DateTime.UtcNow, CorrelationId = correlationId
        });
        await InboxProcessor.MarkProcessedAsync(db, eventId, "inventory.order-events.v1", cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private static void RecordLag(IConsumer<string, string> consumer, ConsumeResult<string, string> result, string service, string topic)
    {
        try
        {
            var watermark = consumer.QueryWatermarkOffsets(result.TopicPartition, TimeSpan.FromSeconds(1));
            var lag = Math.Max(0, watermark.High.Value - result.Offset.Value - 1);
            KafkaConsumerLagState.Record(topic, result.Partition.Value, lag);
            MessagingMetrics.ConsumerLag.Record(lag, new KeyValuePair<string, object?>[] { new("service", service), new("topic", topic), new("partition", result.Partition.Value) });
        }
        catch { }
    }
}
