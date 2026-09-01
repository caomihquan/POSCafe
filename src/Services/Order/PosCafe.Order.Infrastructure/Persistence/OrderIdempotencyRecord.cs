namespace PosCafe.Order.Infrastructure.Persistence;

public sealed class OrderIdempotencyRecord
{
    public Guid Id { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public Guid OrderId { get; set; }
    public string Status { get; set; } = string.Empty;
    public decimal Subtotal { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
