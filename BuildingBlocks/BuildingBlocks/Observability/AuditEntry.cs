namespace BuildingBlocks.Observability;

public sealed class AuditEntry
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Action { get; set; } = string.Empty;
    public string EntityType { get; set; } = string.Empty;
    public string EntityId { get; set; } = string.Empty;
    public Guid? ActorId { get; set; }
    public Guid? StoreId { get; set; }
    public string CorrelationId { get; set; } = string.Empty;
    public DateTime OccurredAtUtc { get; set; }
    public string? MetadataJson { get; set; }
}
