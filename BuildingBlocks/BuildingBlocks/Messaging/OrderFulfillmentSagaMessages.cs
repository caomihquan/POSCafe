namespace BuildingBlocks.Messaging;

public static class OrderFulfillmentSagaEventTypes
{
    public const string PaymentAuthorizationRequested = "PaymentAuthorizationRequested.v1";
    public const string PaymentRefundRequested = "PaymentRefundRequested.v1";
    public const string InventoryReservationRequested = "InventoryReservationRequested.v1";
    public const string InventoryReserved = "InventoryReserved.v1";
    public const string InventoryReservationFailed = "InventoryReservationFailed.v1";
}

public sealed record PaymentAuthorizationRequested(
    Guid SagaId,
    Guid OrderId,
    decimal Amount,
    string Method,
    DateTimeOffset OccurredAt);

public sealed record PaymentRefundRequested(
    Guid SagaId,
    Guid PaymentId,
    Guid OrderId,
    decimal Amount,
    DateTimeOffset OccurredAt);

public sealed record InventoryReservationRequested(
    Guid SagaId,
    Guid OrderId,
    Guid StoreId,
    decimal Total,
    IReadOnlyCollection<SagaOrderLine> Lines,
    DateTimeOffset OccurredAt);

public sealed record SagaOrderLine(Guid ProductId, decimal Quantity);

public sealed record InventoryReserved(
    Guid SagaId,
    Guid OrderId,
    Guid StoreId,
    DateTimeOffset OccurredAt);

public sealed record InventoryReservationFailed(
    Guid SagaId,
    Guid OrderId,
    Guid StoreId,
    string Reason,
    DateTimeOffset OccurredAt);
