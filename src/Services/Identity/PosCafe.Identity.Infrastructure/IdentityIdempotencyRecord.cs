namespace PosCafe.Identity.Infrastructure;

public sealed class IdentityIdempotencyRecord
{
    public Guid Id { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public string RequestHash { get; set; } = string.Empty;
    public string Operation { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
}
