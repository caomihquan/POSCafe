using BuildingBlocks.Domain;
using BuildingBlocks.Exceptions;

namespace PosCafe.Payment.Domain;

public enum PaymentStatus { Pending, Authorized, Failed, Refunded }

public sealed class Payment : AggregateRoot<Guid>
{
    private Payment(Guid id, Guid orderId, decimal amount, string method) : base(id) { OrderId = orderId; Amount = amount; Method = method; Status = PaymentStatus.Pending; CreatedAtUtc = DateTimeOffset.UtcNow; }
    private Payment() : base(Guid.Empty) { }
    public Guid OrderId { get; private set; }
    public decimal Amount { get; private set; }
    public string Method { get; private set; } = string.Empty;
    public PaymentStatus Status { get; private set; }
    public DateTimeOffset CreatedAtUtc { get; private set; }
    public static Payment Create(Guid orderId, decimal amount, string method)
    {
        if (orderId == Guid.Empty) throw new ValidationException("Order is required.");
        if (amount <= 0) throw new ValidationException("Payment amount must be greater than zero.");
        if (string.IsNullOrWhiteSpace(method)) throw new ValidationException("Payment method is required.");
        var payment = new Payment(Guid.NewGuid(), orderId, amount, method.Trim());
        payment.Raise(new PaymentCreatedDomainEvent(payment.Id, payment.OrderId, payment.Amount, payment.CreatedAtUtc));
        return payment;
    }
    public void Authorize()
    {
        if (Status != PaymentStatus.Pending) throw new ConflictException("Only pending payments can be authorized.");
        Status = PaymentStatus.Authorized; Raise(new PaymentAuthorizedDomainEvent(Id, OrderId, Amount, DateTimeOffset.UtcNow));
    }
    public void Refund()
    {
        if (Status != PaymentStatus.Authorized) throw new ConflictException("Only authorized payments can be refunded.");
        Status = PaymentStatus.Refunded; Raise(new PaymentRefundedDomainEvent(Id, OrderId, Amount, DateTimeOffset.UtcNow));
    }
}
public sealed record PaymentCreatedDomainEvent(Guid PaymentId, Guid OrderId, decimal Amount, DateTimeOffset OccurredAt) : IDomainEvent;
public sealed record PaymentAuthorizedDomainEvent(Guid PaymentId, Guid OrderId, decimal Amount, DateTimeOffset OccurredAt) : IDomainEvent;
public sealed record PaymentRefundedDomainEvent(Guid PaymentId, Guid OrderId, decimal Amount, DateTimeOffset OccurredAt) : IDomainEvent;
