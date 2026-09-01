using System.Text.Json;
using System.Diagnostics;
using BuildingBlocks.Exceptions;
using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Application;
using PosCafe.Payment.Domain;
using PosCafe.Payment.Infrastructure.Persistence;
using PaymentAggregate = PosCafe.Payment.Domain.Payment;

namespace PosCafe.Payment.Infrastructure;

public sealed class PaymentCommandService(PaymentDbContext db) : IPaymentCommandService
{
    public Task<PaymentCommandResult> CreateAsync(CreatePaymentCommand command, CancellationToken ct) =>
        CreateAsync(command, $"legacy-{Guid.NewGuid():N}", Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(JsonSerializer.Serialize(command)))), ct);

    public async Task<PaymentCommandResult> CreateAsync(CreatePaymentCommand command, string idempotencyKey, string requestHash, CancellationToken ct)
    {
        var existing = await db.PaymentIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, ct);
        if (existing is not null)
        {
            if (!Idempotency.Matches(existing.RequestHash, requestHash)) throw new ConflictException("Idempotency-Key is already bound to a different payment request.");
            MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "payment"));
            return new PaymentCommandResult(existing.PaymentId, existing.Status, existing.Amount, true);
        }
        try
        {
            return await ExecuteAsync(() =>
            {
                var payment = PaymentAggregate.Create(command.OrderId, command.Amount, command.Method);
                db.PaymentIdempotencyRecords.Add(new PaymentIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = idempotencyKey, RequestHash = requestHash, PaymentId = payment.Id, Status = payment.Status.ToString(), Amount = payment.Amount, CreatedAtUtc = DateTime.UtcNow });
                return Task.FromResult(payment);
            }, command.ActorId, "payment.created", ct);
        }
        catch (DbUpdateException)
        {
            db.ChangeTracker.Clear();
            var winner = await db.PaymentIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, ct);
            if (winner is null) throw;
            if (!Idempotency.Matches(winner.RequestHash, requestHash)) throw new ConflictException("Idempotency-Key is already bound to a different payment request.");
            MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "payment"));
            return new PaymentCommandResult(winner.PaymentId, winner.Status, winner.Amount, true);
        }
    }
    public Task<PaymentCommandResult> AuthorizeAsync(PaymentActionCommand command, CancellationToken ct) => ExecuteAsync(async () => { var payment = await FindAsync(command.PaymentId, ct); payment.Authorize(); return payment; }, command.ActorId, "payment.authorized", ct);
    public Task<PaymentCommandResult> RefundAsync(PaymentActionCommand command, CancellationToken ct) => ExecuteAsync(async () => { var payment = await FindAsync(command.PaymentId, ct); payment.Refund(); return payment; }, command.ActorId, "payment.refunded", ct);

    private async Task<PaymentAggregate> FindAsync(Guid id, CancellationToken ct) => await db.Payments.SingleOrDefaultAsync(x => x.Id == id, ct) ?? throw new NotFoundException("Payment", id);
    private async Task<PaymentCommandResult> ExecuteAsync(Func<Task<PaymentAggregate>> action, Guid? actorId, string auditAction, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var payment = await action();
        if (db.Entry(payment).State == EntityState.Detached) db.Payments.Add(payment);
        var correlationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N");
        var storeId = await db.OrderProjections.Where(x => x.OrderId == payment.OrderId).Select(x => (Guid?)x.StoreId).SingleOrDefaultAsync(ct);
        db.AuditEntries.Add(new AuditEntry { Action = auditAction, EntityType = "Payment", EntityId = payment.Id.ToString(), ActorId = actorId, StoreId = storeId, CorrelationId = correlationId, OccurredAtUtc = DateTime.UtcNow });
        foreach (var domainEvent in payment.DequeueDomainEvents()) db.OutboxMessages.Add(new OutboxMessage { Id = Guid.NewGuid(), AggregateId = payment.Id.ToString(), EventType = domainEvent.GetType().Name.Replace("DomainEvent", ".v1"), Payload = JsonSerializer.Serialize(domainEvent), OccurredOnUtc = domainEvent.OccurredAt.UtcDateTime, CorrelationId = correlationId });
        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
        return new PaymentCommandResult(payment.Id, payment.Status.ToString(), payment.Amount);
    }
}
