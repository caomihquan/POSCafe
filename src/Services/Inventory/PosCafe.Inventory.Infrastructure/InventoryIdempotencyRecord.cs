namespace PosCafe.Inventory.Infrastructure;

public sealed class InventoryIdempotencyRecord
{
    public Guid Id { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public string ResponseJson { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
}
