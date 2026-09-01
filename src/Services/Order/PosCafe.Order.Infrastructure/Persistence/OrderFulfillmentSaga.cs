namespace PosCafe.Order.Infrastructure.Persistence;

public sealed class OrderFulfillmentSaga
{
    public Guid SagaId { get; set; }
    public Guid OrderId { get; set; }
    public Guid StoreId { get; set; }
    public decimal Total { get; set; }
    public string PaymentMethod { get; set; } = string.Empty;
    public string Status { get; set; } = "Started";
    public Guid? PaymentId { get; set; }
    public bool PaymentAuthorized { get; set; }
    public bool InventoryReserved { get; set; }
    public bool InventoryReservationFailed { get; set; }
    public bool PaymentRefundRequested { get; set; }
    public string? LastError { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}
