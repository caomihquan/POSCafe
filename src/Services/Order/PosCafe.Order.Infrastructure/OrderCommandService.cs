using System.Text.Json;
using System.Diagnostics;
using BuildingBlocks.Exceptions;
using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Microsoft.EntityFrameworkCore;
using PosCafe.Order.Application;
using PosCafe.Order.Domain;
using PosCafe.Order.Infrastructure.Persistence;
using OrderAggregate = PosCafe.Order.Domain.Order;

namespace PosCafe.Order.Infrastructure;

public sealed class OrderCommandService(OrderDbContext db) : IOrderCommandService
{
    public Task<OrderCommandResult> CreateAsync(CreateOrderCommand command, CancellationToken cancellationToken) =>
        CreateAsync(command, $"legacy-{Guid.NewGuid():N}", Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(command)))), cancellationToken);

    public async Task<OrderCommandResult> CreateAsync(CreateOrderCommand command, string idempotencyKey, string requestHash, CancellationToken cancellationToken)
    {
        var existing = await db.OrderIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, cancellationToken);
        if (existing is not null)
        {
            if (!Idempotency.Matches(existing.RequestHash, requestHash)) throw new ConflictException("Idempotency-Key is already bound to a different order request.");
            MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "order"));
            return new OrderCommandResult(existing.OrderId, existing.Status, existing.Subtotal, true);
        }

        try
        {
            return await ExecuteAsync(() =>
            {
                var order = OrderAggregate.Create(command.StoreId, command.Channel);
                foreach (var line in command.Lines) order.AddLine(OrderLine.Create(line.ProductId, line.ProductName, line.UnitPrice, line.Quantity));
                db.Orders.Add(order);
                db.OrderIdempotencyRecords.Add(new OrderIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = idempotencyKey, RequestHash = requestHash, OrderId = order.Id, Status = order.Status.ToString(), Subtotal = order.Subtotal, CreatedAtUtc = DateTime.UtcNow });
                return order;
            }, command.ActorId, "order.created", cancellationToken);
        }
        catch (DbUpdateException)
        {
            db.ChangeTracker.Clear();
            var winner = await db.OrderIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, cancellationToken);
            if (winner is null) throw;
            if (!Idempotency.Matches(winner.RequestHash, requestHash)) throw new ConflictException("Idempotency-Key is already bound to a different order request.");
            MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "order"));
            return new OrderCommandResult(winner.OrderId, winner.Status, winner.Subtotal, true);
        }
    }

    public Task<OrderCommandResult> ConfirmAsync(ConfirmOrderCommand command, CancellationToken cancellationToken) => ExecuteAsync(async () =>
    {
        var order = await db.Orders.Include(x => x.Lines).SingleOrDefaultAsync(x => x.Id == command.OrderId, cancellationToken) ?? throw new NotFoundException("Order", command.OrderId);
        order.Confirm();
        return order;
    }, command.ActorId, "order.confirmed", cancellationToken);

    public Task<OrderCommandResult> CancelAsync(CancelOrderCommand command, CancellationToken cancellationToken) => ExecuteAsync(async () =>
    {
        var order = await db.Orders.Include(x => x.Lines).SingleOrDefaultAsync(x => x.Id == command.OrderId, cancellationToken) ?? throw new NotFoundException("Order", command.OrderId);
        order.Cancel(command.Reason);
        return order;
    }, command.ActorId, "order.cancelled", cancellationToken);

    private async Task<OrderCommandResult> ExecuteAsync(Func<OrderAggregate> action, Guid? actorId, string auditAction, CancellationToken token) => await ExecuteAsync(() => Task.FromResult(action()), actorId, auditAction, token);
    private async Task<OrderCommandResult> ExecuteAsync(Func<Task<OrderAggregate>> action, Guid? actorId, string auditAction, CancellationToken token)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(token);
        var order = await action();
        var correlationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        db.AuditEntries.Add(new AuditEntry { Action = auditAction, EntityType = "Order", EntityId = order.Id.ToString(), ActorId = actorId, StoreId = order.StoreId, CorrelationId = correlationId, OccurredAtUtc = DateTime.UtcNow });
        foreach (var domainEvent in order.DequeueDomainEvents()) db.OutboxMessages.Add(ToOutbox(order, domainEvent, correlationId));
        await db.SaveChangesAsync(token);
        await transaction.CommitAsync(token);
        return new OrderCommandResult(order.Id, order.Status.ToString(), order.Subtotal);
    }

    private static OutboxMessage ToOutbox(OrderAggregate order, BuildingBlocks.Domain.IDomainEvent domainEvent, string correlationId) => new()
    {
        Id = Guid.NewGuid(), AggregateId = order.Id.ToString(), EventType = domainEvent.GetType().Name.Replace("DomainEvent", ".v1"), Payload = JsonSerializer.Serialize(domainEvent), OccurredOnUtc = domainEvent.OccurredAt.UtcDateTime, CorrelationId = correlationId
    };
}
