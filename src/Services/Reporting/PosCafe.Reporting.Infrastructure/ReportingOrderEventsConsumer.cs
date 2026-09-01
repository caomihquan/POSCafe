using System.Text;
using System.Text.Json;
using BuildingBlocks.Messaging;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace PosCafe.Reporting.Infrastructure;

public sealed class ReportingMessagingOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string InputTopic { get; set; } = "pos.order.events";
    public string ConsumerGroup { get; set; } = "pos-reporting-order-events-v1";
    public int ConsumerRetrySeconds { get; set; } = 5;
    public int ConsumerMaxAttempts { get; set; } = 5;
    public int RetryMaxSeconds { get; set; } = 300;
    public string DeadLetterTopic { get; set; } = "pos.reporting.order-events.dlq";
}

public sealed class ReportingOrderEventsConsumer(MongoReportingRepository repository, IProducer<string, string> producer, IOptions<ReportingMessagingOptions> options, IConfiguration configuration, ILogger<ReportingOrderEventsConsumer> logger) : BackgroundService
{
    private sealed record OrderConfirmedEvent(Guid OrderId, Guid StoreId, decimal Total, DateTimeOffset OccurredAt);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (string.IsNullOrWhiteSpace(settings.BootstrapServers) || string.IsNullOrWhiteSpace(settings.InputTopic) || string.IsNullOrWhiteSpace(settings.ConsumerGroup) || string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
            throw new InvalidOperationException("Reporting Kafka consumer requires BootstrapServers, InputTopic, ConsumerGroup, and DeadLetterTopic.");
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
        .SetPartitionsAssignedHandler((_, partitions) => { logger.LogInformation("Reporting consumer assigned {Partitions}", string.Join(",", partitions)); return partitions.Select(partition => new TopicPartitionOffset(partition, Offset.Unset)); })
        .SetPartitionsRevokedHandler((_, partitions) => logger.LogWarning("Reporting consumer partitions revoked: {Partitions}", string.Join(",", partitions)))
        .SetPartitionsLostHandler((_, partitions) => logger.LogError("Reporting consumer partitions lost: {Partitions}", string.Join(",", partitions)))
        .Build();
        consumer.Subscribe(options.Value.InputTopic);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var message = consumer.Consume(stoppingToken);
                RecordLag(consumer, message, "reporting", options.Value.InputTopic);
                var idHeader = message.Message.Headers.FirstOrDefault(x => x.Key == "event-id");
                var typeHeader = message.Message.Headers.FirstOrDefault(x => x.Key == "event-type");
                var eventType = typeHeader is null ? string.Empty : Encoding.UTF8.GetString(typeHeader.GetValueBytes());
                if (eventType != "OrderConfirmed.v1")
                {
                    consumer.Commit(message);
                    continue;
                }
                var schemaHeader = message.Message.Headers.FirstOrDefault(x => x.Key == "schema-version");
                var schemaIdHeader = message.Message.Headers.FirstOrDefault(x => x.Key == "schema-id");
                if (idHeader is null || typeHeader is null || schemaHeader is null || schemaIdHeader is null || Encoding.UTF8.GetString(schemaHeader.GetValueBytes()) != "1" || Encoding.UTF8.GetString(schemaIdHeader.GetValueBytes()) != "order-confirmed.v1" || !Guid.TryParse(Encoding.UTF8.GetString(idHeader.GetValueBytes()), out var eventId))
                {
                    await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(message, "reporting", "Missing or unsupported event headers."), stoppingToken);
                    MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "reporting"));
                    logger.LogError("Moved poison message at {Offset} to DLQ: invalid event headers", message.TopicPartitionOffset);
                    consumer.Commit(message);
                    continue;
                }
                var attempt = 0;
                try
                {
                    var traceparent = message.Message.Headers.FirstOrDefault(x => x.Key == "traceparent") is { } traceHeader ? Encoding.UTF8.GetString(traceHeader.GetValueBytes()) : null;
                    System.Diagnostics.ActivityContext.TryParse(traceparent, null, true, out var parentContext);
                    using var activity = MessagingTelemetry.ActivitySource.StartActivity("messaging.process", System.Diagnostics.ActivityKind.Consumer, parentContext);
                    attempt = await repository.RegisterAttemptAsync(eventId, stoppingToken);
                    IntegrationPayloadValidator.Validate("OrderConfirmed.v1", message.Message.Value);
                    var confirmed = JsonSerializer.Deserialize<OrderConfirmedEvent>(message.Message.Value) ?? throw new InvalidOperationException("OrderConfirmed payload is invalid.");
                    await repository.ApplyOrderConfirmedAsync(eventId, confirmed.StoreId, DateOnly.FromDateTime(confirmed.OccurredAt.UtcDateTime), confirmed.Total, stoppingToken);
                    consumer.Commit(message);
                }
                catch (Exception exception) when (!stoppingToken.IsCancellationRequested)
                {
                    if (attempt >= Math.Max(1, settings.ConsumerMaxAttempts) && !string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
                    {
                        var headers = new Headers();
                        foreach (var header in message.Message.Headers) headers.Add(header.Key, header.GetValueBytes());
                        headers.Add("dead-letter-reason", Encoding.UTF8.GetBytes(exception.Message));
                        headers.Add("dead-lettered-by", Encoding.UTF8.GetBytes("reporting"));
                        headers.Add("dead-letter-attempt", Encoding.UTF8.GetBytes(attempt.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        headers.Add("original-topic", Encoding.UTF8.GetBytes(message.Topic));
                        headers.Add("original-partition", Encoding.UTF8.GetBytes(message.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        headers.Add("original-offset", Encoding.UTF8.GetBytes(message.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                        await producer.ProduceAsync(settings.DeadLetterTopic, new Message<string, string> { Key = message.Message.Key, Value = message.Message.Value, Headers = headers }, stoppingToken);
                        await repository.MarkDeadLetteredAsync(eventId, exception.Message, stoppingToken);
                        MessagingMetrics.DeadLettered.Add(1, new KeyValuePair<string, object?>("service", "reporting"));
                        logger.LogError(exception, "Moved reporting event {EventId} to DLQ after {Attempts} attempts", eventId, attempt);
                        consumer.Commit(message);
                    }
                    else
                    {
                        logger.LogError(exception, "Failed to process reporting event {EventId} on attempt {Attempt}; Kafka offset will not be committed", eventId, attempt);
                        var delaySeconds = Math.Min(Math.Max(1, settings.RetryMaxSeconds), Math.Max(1, settings.ConsumerRetrySeconds) * Math.Pow(2, Math.Min(Math.Max(attempt - 1, 0), 6)));
                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        catch (Exception exception) { logger.LogError(exception, "Reporting order event consumer stopped unexpectedly"); throw; }
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
