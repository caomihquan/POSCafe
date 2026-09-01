namespace PosCafe.Catalog.Infrastructure.Persistence;

public sealed class CatalogIdempotencyRecord
{
    public Guid Id { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public Guid ResourceId { get; set; }
    public string ResourceType { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
}
