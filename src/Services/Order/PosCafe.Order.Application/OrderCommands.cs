namespace PosCafe.Order.Application;

public sealed record CreateOrderCommand(Guid StoreId, string Channel, IReadOnlyCollection<CreateOrderLine> Lines, Guid? ActorId = null);
public sealed record CreateOrderLine(Guid ProductId, string ProductName, decimal UnitPrice, int Quantity);
public sealed record ConfirmOrderCommand(Guid OrderId, Guid? ActorId = null);
public sealed record CancelOrderCommand(Guid OrderId, string Reason, Guid? ActorId = null);
public sealed record OrderCommandResult(Guid OrderId, string Status, decimal Subtotal, bool IdempotencyReplayed = false);

public interface IOrderCommandService
{
    Task<OrderCommandResult> CreateAsync(CreateOrderCommand command, string idempotencyKey, string requestHash, CancellationToken cancellationToken);
    Task<OrderCommandResult> ConfirmAsync(ConfirmOrderCommand command, CancellationToken cancellationToken);
    Task<OrderCommandResult> CancelAsync(CancelOrderCommand command, CancellationToken cancellationToken);
}
