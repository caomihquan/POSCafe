using System.Text;
using System.Text.Json;
using BuildingBlocks.Messaging;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using PosCafe.Order.Infrastructure.Persistence;

namespace PosCafe.Order.Infrastructure.Messaging;

public sealed class SagaMessagingOptions
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string[] InputTopics { get; set; } = ["pos.order.events", "pos.payment.events", "pos.inventory.events"];
    public string ConsumerGroup { get; set; } = "pos-order-fulfillment-saga-v1";
    public string DeadLetterTopic { get; set; } = "pos.order.fulfillment-saga.dlq";
    public string DefaultPaymentMethod { get; set; } = "cash";
    public int ConsumerRetrySeconds { get; set; } = 5;
    public int ConsumerMaxAttempts { get; set; } = 5;
}

public sealed class OrderFulfillmentSagaOrchestrator(
    IServiceScopeFactory scopeFactory,
    IProducer<string, string> producer,
    IOptions<SagaMessagingOptions> options,
    IConfiguration configuration,
    ILogger<OrderFulfillmentSagaOrchestrator> logger) : BackgroundService
{
    private const string ConsumerName = "order-fulfillment-saga.v1";

    private sealed record OrderConfirmedEvent(Guid OrderId, Guid StoreId, decimal Total, DateTimeOffset OccurredAt, IReadOnlyCollection<OrderLine> Lines);
    private sealed record OrderLine(Guid ProductId, decimal Quantity);
    private sealed record PaymentAuthorizedEvent(Guid PaymentId, Guid OrderId, decimal Amount, DateTimeOffset OccurredAt);
    private sealed record PaymentRefundedEvent(Guid PaymentId, Guid OrderId, decimal Amount, DateTimeOffset OccurredAt);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        if (string.IsNullOrWhiteSpace(settings.BootstrapServers) || settings.InputTopics.Length == 0 || string.IsNullOrWhiteSpace(settings.ConsumerGroup))
            throw new InvalidOperationException("Saga orchestrator requires BootstrapServers, InputTopics and ConsumerGroup.");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            GroupId = settings.ConsumerGroup,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
            IsolationLevel = IsolationLevel.ReadCommitted
        };
        KafkaProducerConfiguration.ApplySecurity(consumerConfig, configuration.GetSection("Kafka:Security"));

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(settings.InputTopics);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var result = consumer.Consume(stoppingToken);
                var eventType = Header(result.Message.Headers, "event-type");
                if (!IsSagaEvent(eventType))
                {
                    consumer.Commit(result);
                    continue;
                }

                if (!Guid.TryParse(Header(result.Message.Headers, "event-id"), out var eventId))
                {
                    await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(result, "order-saga", "Missing or invalid event-id header."), stoppingToken);
                    consumer.Commit(result);
                    continue;
                }

                try
                {
                    await ProcessAsync(result, eventId, eventType!, stoppingToken);
                    consumer.Commit(result);
                }
                catch (Exception exception) when (!stoppingToken.IsCancellationRequested)
                {
                    logger.LogError(exception, "Saga orchestrator failed for event {EventId} from {Topic}", eventId, result.Topic);
                    var attempts = await GetAttemptsAsync(eventId, stoppingToken);
                    if (attempts >= Math.Max(1, settings.ConsumerMaxAttempts) && !string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
                    {
                        await producer.ProduceAsync(settings.DeadLetterTopic, KafkaDeadLetter.Create(result, "order-saga", exception.Message), stoppingToken);
                        using var deadLetterScope = scopeFactory.CreateScope();
                        var deadLetterDb = deadLetterScope.ServiceProvider.GetRequiredService<OrderDbContext>();
                        await InboxProcessor.MarkDeadLetteredAsync(deadLetterDb, eventId, ConsumerName, exception.Message, stoppingToken);
                        consumer.Commit(result);
                    }
                    else
                    {
                        await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, settings.ConsumerRetrySeconds)), stoppingToken);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally { consumer.Close(); }
    }

    private async Task ProcessAsync(ConsumeResult<string, string> result, Guid eventId, string eventType, CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        await InboxProcessor.RegisterAttemptAsync(db, eventId, ConsumerName, cancellationToken);
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        if (!await InboxProcessor.TryStartAsync(db, eventId, ConsumerName, cancellationToken))
        {
            await transaction.CommitAsync(cancellationToken);
            return;
        }

        var correlationId = Header(result.Message.Headers, "correlation-id") ?? eventId.ToString("N");
        IntegrationPayloadValidator.Validate(eventType, result.Message.Value);
        switch (eventType)
        {
            case "OrderConfirmed.v1":
                await StartSagaAsync(db, result.Message.Value, correlationId, cancellationToken);
                break;
            case "PaymentAuthorized.v1":
                await ApplyPaymentAuthorizedAsync(db, result.Message.Value, correlationId, cancellationToken);
                break;
            case "InventoryReserved.v1":
                await ApplyInventoryReservedAsync(db, result.Message.Value, cancellationToken);
                break;
            case "InventoryReservationFailed.v1":
                await ApplyInventoryFailureAsync(db, result.Message.Value, correlationId, cancellationToken);
                break;
            case "PaymentRefunded.v1":
                await ApplyPaymentRefundedAsync(db, result.Message.Value, cancellationToken);
                break;
        }

        await InboxProcessor.MarkProcessedAsync(db, eventId, ConsumerName, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    private async Task StartSagaAsync(OrderDbContext db, string payload, string correlationId, CancellationToken cancellationToken)
    {
        var confirmed = JsonSerializer.Deserialize<OrderConfirmedEvent>(payload) ?? throw new InvalidOperationException("OrderConfirmed payload is invalid.");
        var saga = await db.OrderFulfillmentSagas.SingleOrDefaultAsync(x => x.OrderId == confirmed.OrderId, cancellationToken);
        if (saga is not null) return;

        var now = DateTime.UtcNow;
        saga = new OrderFulfillmentSaga
        {
            SagaId = Guid.NewGuid(), OrderId = confirmed.OrderId, StoreId = confirmed.StoreId,
            Total = confirmed.Total, PaymentMethod = options.Value.DefaultPaymentMethod,
            Status = "Started", CreatedAtUtc = now, UpdatedAtUtc = now
        };
        db.OrderFulfillmentSagas.Add(saga);
        AddCommand(db, saga.OrderId, OrderFulfillmentSagaEventTypes.PaymentAuthorizationRequested,
            new PaymentAuthorizationRequested(saga.SagaId, saga.OrderId, saga.Total, saga.PaymentMethod, DateTimeOffset.UtcNow), correlationId);
        AddCommand(db, saga.OrderId, OrderFulfillmentSagaEventTypes.InventoryReservationRequested,
            new InventoryReservationRequested(saga.SagaId, saga.OrderId, saga.StoreId, saga.Total,
                confirmed.Lines.Select(x => new SagaOrderLine(x.ProductId, x.Quantity)).ToArray(), DateTimeOffset.UtcNow), correlationId);
    }

    private static async Task ApplyPaymentAuthorizedAsync(OrderDbContext db, string payload, string correlationId, CancellationToken cancellationToken)
    {
        var payment = JsonSerializer.Deserialize<PaymentAuthorizedEvent>(payload) ?? throw new InvalidOperationException("PaymentAuthorized payload is invalid.");
        var saga = await db.OrderFulfillmentSagas.SingleOrDefaultAsync(x => x.OrderId == payment.OrderId, cancellationToken);
        if (saga is null) return;
        saga.PaymentAuthorized = true;
        saga.PaymentId = payment.PaymentId;
        saga.UpdatedAtUtc = DateTime.UtcNow;
        if (saga.InventoryReservationFailed && !saga.PaymentRefundRequested)
        {
            saga.PaymentRefundRequested = true;
            saga.Status = "Compensating";
            AddCommand(db, saga.OrderId, OrderFulfillmentSagaEventTypes.PaymentRefundRequested,
                new PaymentRefundRequested(saga.SagaId, payment.PaymentId, saga.OrderId, payment.Amount, DateTimeOffset.UtcNow), correlationId);
        }
        else saga.Status = saga.InventoryReserved ? "Completed" : "PaymentAuthorized";
    }

    private static async Task ApplyInventoryReservedAsync(OrderDbContext db, string payload, CancellationToken cancellationToken)
    {
        var inventory = JsonSerializer.Deserialize<InventoryReserved>(payload) ?? throw new InvalidOperationException("InventoryReserved payload is invalid.");
        var saga = await db.OrderFulfillmentSagas.SingleOrDefaultAsync(x => x.OrderId == inventory.OrderId, cancellationToken);
        if (saga is null) return;
        saga.InventoryReserved = true;
        saga.Status = saga.PaymentAuthorized ? "Completed" : "InventoryReserved";
        saga.UpdatedAtUtc = DateTime.UtcNow;
    }

    private static async Task ApplyInventoryFailureAsync(OrderDbContext db, string payload, string correlationId, CancellationToken cancellationToken)
    {
        var failure = JsonSerializer.Deserialize<InventoryReservationFailed>(payload) ?? throw new InvalidOperationException("InventoryReservationFailed payload is invalid.");
        var saga = await db.OrderFulfillmentSagas.SingleOrDefaultAsync(x => x.OrderId == failure.OrderId, cancellationToken);
        if (saga is null) return;
        saga.InventoryReservationFailed = true;
        saga.LastError = failure.Reason;
        saga.UpdatedAtUtc = DateTime.UtcNow;
        if (saga.PaymentAuthorized && saga.PaymentId.HasValue && !saga.PaymentRefundRequested)
        {
            saga.PaymentRefundRequested = true;
            saga.Status = "Compensating";
            AddCommand(db, saga.OrderId, OrderFulfillmentSagaEventTypes.PaymentRefundRequested,
                new PaymentRefundRequested(saga.SagaId, saga.PaymentId.Value, saga.OrderId, saga.Total, DateTimeOffset.UtcNow), correlationId);
        }
        else saga.Status = "Failed";
    }

    private static async Task ApplyPaymentRefundedAsync(OrderDbContext db, string payload, CancellationToken cancellationToken)
    {
        var refund = JsonSerializer.Deserialize<PaymentRefundedEvent>(payload) ?? throw new InvalidOperationException("PaymentRefunded payload is invalid.");
        var saga = await db.OrderFulfillmentSagas.SingleOrDefaultAsync(x => x.OrderId == refund.OrderId, cancellationToken);
        if (saga is null) return;
        saga.Status = "Failed";
        saga.LastError ??= "Inventory reservation failed; payment was refunded.";
        saga.UpdatedAtUtc = DateTime.UtcNow;
    }

    private static void AddCommand(OrderDbContext db, Guid aggregateId, string eventType, object payload, string correlationId) =>
        db.OutboxMessages.Add(new OutboxMessage
        {
            Id = Guid.NewGuid(), AggregateId = aggregateId.ToString(), EventType = eventType,
            Payload = JsonSerializer.Serialize(payload), OccurredOnUtc = DateTime.UtcNow, CorrelationId = correlationId
        });

    private async Task<int> GetAttemptsAsync(Guid eventId, CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        return await db.InboxMessages.Where(x => x.EventId == eventId && x.Consumer == ConsumerName).Select(x => x.Attempts).SingleOrDefaultAsync(cancellationToken);
    }

    private static bool IsSagaEvent(string? eventType) => eventType is "OrderConfirmed.v1" or "PaymentAuthorized.v1" or "InventoryReserved.v1" or "InventoryReservationFailed.v1" or "PaymentRefunded.v1";

    private static string? Header(Headers headers, string name) => headers.FirstOrDefault(x => x.Key == name) is { } header ? Encoding.UTF8.GetString(header.GetValueBytes()) : null;
}
