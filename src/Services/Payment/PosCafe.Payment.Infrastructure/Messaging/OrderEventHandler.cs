using System.Text.Json;
using BuildingBlocks.Messaging;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Infrastructure.Persistence;

namespace PosCafe.Payment.Infrastructure.Messaging;

public sealed class OrderEventHandler
{
    private sealed record OrderConfirmedEvent(Guid OrderId, Guid StoreId, decimal Total, DateTimeOffset OccurredAt);

    public async Task<bool> HandleAsync(PaymentDbContext db, Guid eventId, string consumer, string eventType, string payload, CancellationToken cancellationToken)
    {
        if (!await InboxProcessor.TryStartAsync(db, eventId, consumer, cancellationToken))
        {
            MessagingMetrics.DuplicateEvents.Add(1, new KeyValuePair<string, object?>("service", "payment"));
            return false;
        }

        if (eventType == "OrderConfirmed.v1")
        {
            IntegrationPayloadValidator.Validate(eventType, payload);
            OrderConfirmedEvent confirmed;
            try
            {
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

        await InboxProcessor.MarkProcessedAsync(db, eventId, consumer, cancellationToken);
        MessagingMetrics.Consumed.Add(1, new KeyValuePair<string, object?>("service", "payment"));
        return true;
    }
}
