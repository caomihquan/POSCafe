using BuildingBlocks.Domain;
using BuildingBlocks.Exceptions;

namespace PosCafe.Order.Domain;

public sealed class Order : AggregateRoot<Guid>
{
    private readonly List<OrderLine> _lines = [];
    private Order(Guid id, Guid storeId, string channel) : base(id) { StoreId = storeId; Channel = channel; Status = OrderStatus.Draft; CreatedAtUtc = DateTimeOffset.UtcNow; }
    private Order() : base(Guid.Empty) { }
    public Guid StoreId { get; private set; }
    public string Channel { get; private set; } = string.Empty;
    public OrderStatus Status { get; private set; }
    public DateTimeOffset CreatedAtUtc { get; private set; }
    public DateTimeOffset? ConfirmedAtUtc { get; private set; }
    public IReadOnlyCollection<OrderLine> Lines => _lines.AsReadOnly();
    public decimal Subtotal => _lines.Sum(x => x.Total);
    public static Order Create(Guid storeId, string channel)
    {
        if (storeId == Guid.Empty) throw new ValidationException("Store is required.");
        if (string.IsNullOrWhiteSpace(channel)) throw new ValidationException("Channel is required.");
        var order = new Order(Guid.NewGuid(), storeId, channel.Trim());
        order.Raise(new OrderCreatedDomainEvent(order.Id, order.StoreId, order.CreatedAtUtc));
        return order;
    }
    public void AddLine(OrderLine line) { EnsureDraft(); ArgumentNullException.ThrowIfNull(line); _lines.Add(line); }
    public void Confirm()
    {
        EnsureDraft();
        if (_lines.Count == 0) throw new ValidationException("An order must contain at least one line.");
        Status = OrderStatus.Confirmed; ConfirmedAtUtc = DateTimeOffset.UtcNow;
        Raise(new OrderConfirmedDomainEvent(Id, StoreId, Subtotal, ConfirmedAtUtc.Value, _lines.Select(x => new OrderLineSnapshot(x.ProductId, x.Quantity)).ToArray()));
    }
    public void Cancel(string reason)
    {
        if (Status is OrderStatus.Completed or OrderStatus.Cancelled) throw new ConflictException("Order cannot be cancelled in its current state.");
        if (string.IsNullOrWhiteSpace(reason)) throw new ValidationException("Cancellation reason is required.");
        Status = OrderStatus.Cancelled; Raise(new OrderCancelledDomainEvent(Id, reason.Trim(), DateTimeOffset.UtcNow));
    }
    private void EnsureDraft() { if (Status != OrderStatus.Draft) throw new ConflictException("Only draft orders can be modified."); }
}
public sealed record OrderCreatedDomainEvent(Guid OrderId, Guid StoreId, DateTimeOffset OccurredAt) : IDomainEvent;
public sealed record OrderLineSnapshot(Guid ProductId, decimal Quantity);
public sealed record OrderConfirmedDomainEvent(Guid OrderId, Guid StoreId, decimal Total, DateTimeOffset OccurredAt, IReadOnlyCollection<OrderLineSnapshot> Lines) : IDomainEvent;
public sealed record OrderCancelledDomainEvent(Guid OrderId, string Reason, DateTimeOffset OccurredAt) : IDomainEvent;
