namespace PosCafe.Store.Infrastructure;

public sealed class StoreIdempotencyRecord
{
    public Guid Id { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public string Operation { get; set; } = string.Empty;
    public Guid ResourceId { get; set; }
    public string ResponseJson { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
