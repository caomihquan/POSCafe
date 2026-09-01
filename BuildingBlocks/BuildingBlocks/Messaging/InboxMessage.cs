namespace BuildingBlocks.Messaging;

public sealed class InboxMessage
{
    public Guid EventId { get; set; }
    public string Consumer { get; set; } = string.Empty;
    public DateTime ReceivedOnUtc { get; set; }
    public int Attempts { get; set; }
    public DateTime? LastAttemptOnUtc { get; set; }
    public DateTime? ProcessedOnUtc { get; set; }
    public string? Error { get; set; }
}
