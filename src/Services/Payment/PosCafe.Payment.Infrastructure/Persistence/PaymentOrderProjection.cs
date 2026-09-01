namespace PosCafe.Payment.Infrastructure.Persistence;

public sealed class PaymentOrderProjection
{
    public Guid OrderId { get; set; }
    public Guid StoreId { get; set; }
    public decimal Total { get; set; }
    public string Status { get; set; } = "Confirmed";
    public DateTime UpdatedAtUtc { get; set; }
}
