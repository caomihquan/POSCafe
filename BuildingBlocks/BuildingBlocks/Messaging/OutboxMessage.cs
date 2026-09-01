namespace BuildingBlocks.Messaging;

public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string AggregateId { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTime OccurredOnUtc { get; set; }
    public string? CorrelationId { get; set; }
    public int Attempts { get; set; }
    public DateTime? ProcessedOnUtc { get; set; }
    public DateTime? LockedUntilUtc { get; set; }
    public DateTime? DeadLetteredOnUtc { get; set; }
    public string? Error { get; set; }
}
