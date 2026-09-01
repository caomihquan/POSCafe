using System.Text;
using BuildingBlocks.Messaging;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PosCafe.Payment.Infrastructure.Persistence;

namespace PosCafe.Payment.Infrastructure.Messaging;

public sealed class OrderEventsConsumer(IServiceScopeFactory scopeFactory, IProducer<string, string> producer, IOptions<OutboxOptions> options, IConfiguration configuration, ILogger<OrderEventsConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (string.IsNullOrWhiteSpace(settings.BootstrapServers) || string.IsNullOrWhiteSpace(settings.InputTopic) || string.IsNullOrWhiteSpace(settings.ConsumerGroup) || string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
            throw new InvalidOperationException("Payment Kafka consumer requires BootstrapServers, InputTopic, ConsumerGroup, and DeadLetterTopic.");
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
        .SetPartitionsAssignedHandler((_, partitions) => { logger.LogInformation("Payment consumer assigned {Partitions}", string.Join(",", partitions)); return partitions.Select(partition => new TopicPartitionOffset(partition, Offset.Unset)); })
        .SetPartitionsRevokedHandler((_, partitions) => logger.LogWarning("Payment consumer partitions revoked: {Partitions}", string.Join(",", partitions)))
        .SetPartitionsLostHandler((_, partitions) => logger.LogError("Payment consumer partitions lost: {Partitions}", string.Join(",", partitions)))
        .Build();
        consumer.Subscribe(settings.InputTopic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                RecordLag(consumer, result, "payment", settings.InputTopic);
                var header = result.Message.Headers.FirstOrDefault(x => x.Key == "event-id");
                var schemaHeader = result.Message.Headers.FirstOrDefault(x => x.Key == "schema-version");
                if (schemaHeader is null || Encoding.UTF8.GetString(schemaHeader.GetValueBytes()) != "1")
                {
                    await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(result, "payment", "Missing or unsupported schema-version."), stoppingToken);
                    MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "payment"));
                    consumer.Commit(result);
                    continue;
                }
                if (header is null || !Guid.TryParse(Encoding.UTF8.GetString(header.GetValueBytes()), out var eventId))
                {
                    await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(result, "payment", "Missing or invalid event-id header."), stoppingToken);
                    MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "payment"));
                    logger.LogError("Moved poison message at {Offset} to DLQ: missing or invalid event-id", result.TopicPartitionOffset);
                    consumer.Commit(result);
                    continue;
                }
                var attempt = 0;
                try
                {
                    var traceparent = result.Message.Headers.FirstOrDefault(x => x.Key == "traceparent") is { } traceHeader ? Encoding.UTF8.GetString(traceHeader.GetValueBytes()) : null;
                    System.Diagnostics.ActivityContext.TryParse(traceparent, null, true, out var parentContext);
                    using var activity = MessagingTelemetry.ActivitySource.StartActivity("messaging.process", System.Diagnostics.ActivityKind.Consumer, parentContext);
                    activity?.SetTag("messaging.system", "kafka");
                    activity?.SetTag("messaging.source.name", settings.InputTopic);
                    activity?.SetTag("messaging.message.id", eventId);
                    using var scope = scopeFactory.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<PaymentDbContext>();
                    attempt = await InboxProcessor.RegisterAttemptAsync(db, eventId, "payment.order-events.v1", stoppingToken);
                    await using var transaction = await db.Database.BeginTransactionAsync(stoppingToken);
                    var eventType = result.Message.Headers.FirstOrDefault(x => x.Key == "event-type") is { } typeHeader ? Encoding.UTF8.GetString(typeHeader.GetValueBytes()) : string.Empty;
                    await new OrderEventHandler().HandleAsync(db, eventId, "payment.order-events.v1", eventType, result.Message.Value, stoppingToken);
                    logger.LogInformation("Processed order event {EventId} for Payment", eventId);
                    await transaction.CommitAsync(stoppingToken);
                    consumer.Commit(result);
                }
                catch (Exception exception) when (!stoppingToken.IsCancellationRequested)
                {
                    if (attempt >= Math.Max(1, options.Value.ConsumerMaxAttempts) && !string.IsNullOrWhiteSpace(options.Value.DeadLetterTopic))
                    {
                        var headers = new Headers();
                        foreach (var kafkaHeader in result.Message.Headers) headers.Add(kafkaHeader.Key, kafkaHeader.GetValueBytes());
                        headers.Add("dead-letter-reason", Encoding.UTF8.GetBytes(exception.Message));
                        headers.Add("dead-lettered-by", Encoding.UTF8.GetBytes("payment"));
                        headers.Add("dead-letter-attempt", Encoding.UTF8.GetBytes(attempt.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        headers.Add("original-topic", Encoding.UTF8.GetBytes(result.Topic));
                        headers.Add("original-partition", Encoding.UTF8.GetBytes(result.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        headers.Add("original-offset", Encoding.UTF8.GetBytes(result.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        await producer.ProduceAsync(options.Value.DeadLetterTopic, new Message<string, string> { Key = result.Message.Key, Value = result.Message.Value, Headers = headers }, stoppingToken);
                        using var deadLetterScope = scopeFactory.CreateScope();
                        var deadLetterDb = deadLetterScope.ServiceProvider.GetRequiredService<PaymentDbContext>();
                        await InboxProcessor.MarkDeadLetteredAsync(deadLetterDb, eventId, "payment.order-events.v1", exception.Message, stoppingToken);
                        MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "payment"));
                        logger.LogError(exception, "Moved order event {EventId} to DLQ after {Attempts} attempts", eventId, attempt);
                        consumer.Commit(result);
                    }
                    else
                    {
                        logger.LogError(exception, "Failed to process order event {EventId} on attempt {Attempt}; Kafka offset will not be committed", eventId, attempt);
                        var delaySeconds = Math.Min(Math.Max(1, options.Value.RetryMaxSeconds), Math.Max(1, options.Value.ConsumerRetrySeconds) * Math.Pow(2, Math.Min(Math.Max(attempt - 1, 0), 6)));
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally { consumer.Close(); }
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
