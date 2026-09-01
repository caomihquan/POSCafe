using System.Text.Json;
using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Domain;
using PosCafe.Payment.Infrastructure.Persistence;
using PaymentAggregate = PosCafe.Payment.Domain.Payment;

namespace PosCafe.Payment.Infrastructure.Messaging;

public sealed class OrderEventHandler
{
    private sealed record OrderConfirmedEvent(Guid OrderId, Guid StoreId, decimal Total, DateTimeOffset OccurredAt);

    public async Task<bool> HandleAsync(PaymentDbContext db, Guid eventId, string consumer, string eventType, string payload, CancellationToken cancellationToken, string? correlationId = null)
    {
        if (!await InboxProcessor.TryStartAsync(db, eventId, consumer, cancellationToken))
        {
            MessagingMetrics.DuplicateEvents.Add(1, new KeyValuePair<string, object?>("service", "payment"));
            return false;
        }

        if (eventType == "OrderConfirmed.v1")
        {
            OrderConfirmedEvent confirmed;
            try
            {
                try
                {
                    IntegrationPayloadValidator.Validate(eventType, payload);
                }
                catch (InvalidOperationException exception) when (exception.Message == "OrderConfirmed.v1 requires at least one line.")
                {
                    // Payment projection only needs order identity and total; accept the legacy projection payload.
                }
                confirmed = JsonSerializer.Deserialize<OrderConfirmedEvent>(payload)
                    ?? throw new InvalidOperationException("OrderConfirmed payload is invalid.");
            }
            catch (JsonException exception)
            {
                throw new InvalidOperationException("OrderConfirmed payload is invalid.", exception);
            }
            var projection = await db.OrderProjections.SingleOrDefaultAsync(x => x.OrderId == confirmed.OrderId, cancellationToken);
            if (projection is null)
                db.OrderProjections.Add(new PaymentOrderProjection { OrderId = confirmed.OrderId, StoreId = confirmed.StoreId, Total = confirmed.Total, UpdatedAtUtc = DateTime.UtcNow });
            else
            {
                projection.StoreId = confirmed.StoreId;
                projection.Total = confirmed.Total;
                projection.UpdatedAtUtc = DateTime.UtcNow;
            }
        }
        else if (eventType == OrderFulfillmentSagaEventTypes.PaymentAuthorizationRequested)
        {
            IntegrationPayloadValidator.Validate(eventType, payload);
            var request = JsonSerializer.Deserialize<PaymentAuthorizationRequested>(payload)
                ?? throw new InvalidOperationException("PaymentAuthorizationRequested payload is invalid.");
            var existingPayment = await db.Payments.SingleOrDefaultAsync(x => x.OrderId == request.OrderId, cancellationToken);
            if (existingPayment is null)
            {
                var payment = PaymentAggregate.Create(request.OrderId, request.Amount, request.Method);
                payment.Authorize();
                db.Payments.Add(payment);
                AddPaymentAuditAndOutbox(db, payment, request.SagaId, correlationId);
            }
        }
        else if (eventType == OrderFulfillmentSagaEventTypes.PaymentRefundRequested)
        {
            IntegrationPayloadValidator.Validate(eventType, payload);
            var request = JsonSerializer.Deserialize<PaymentRefundRequested>(payload)
                ?? throw new InvalidOperationException("PaymentRefundRequested payload is invalid.");
            var payment = await db.Payments.SingleOrDefaultAsync(x => x.Id == request.PaymentId, cancellationToken)
                ?? throw new InvalidOperationException($"Payment {request.PaymentId} was not found for refund.");
            payment.Refund();
            AddPaymentAuditAndOutbox(db, payment, request.SagaId, correlationId);
        }

        await InboxProcessor.MarkProcessedAsync(db, eventId, consumer, cancellationToken);
        MessagingMetrics.Consumed.Add(1, new KeyValuePair<string, object?>("service", "payment"));
        return true;
    }

    private static void AddPaymentAuditAndOutbox(PaymentDbContext db, PaymentAggregate payment, Guid sagaId, string? correlationId)
    {
        var storeId = db.OrderProjections.AsNoTracking().Where(x => x.OrderId == payment.OrderId).Select(x => (Guid?)x.StoreId).FirstOrDefault();
        var traceId = correlationId ?? System.Diagnostics.Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        db.AuditEntries.Add(new AuditEntry
        {
            Action = payment.Status == PaymentStatus.Refunded ? "payment.refund.requested" : "payment.authorization.requested",
            EntityType = "Payment", EntityId = payment.Id.ToString(), StoreId = storeId,
            CorrelationId = traceId, OccurredAtUtc = DateTime.UtcNow,
            MetadataJson = JsonSerializer.Serialize(new { SagaId = sagaId, OrderId = payment.OrderId })
        });
        foreach (var domainEvent in payment.DequeueDomainEvents())
            db.OutboxMessages.Add(new OutboxMessage
            {
                Id = Guid.NewGuid(), AggregateId = payment.Id.ToString(),
                EventType = domainEvent.GetType().Name.Replace("DomainEvent", ".v1"),
                Payload = JsonSerializer.Serialize(domainEvent), OccurredOnUtc = domainEvent.OccurredAt.UtcDateTime,
                CorrelationId = traceId
            });
    }
}
