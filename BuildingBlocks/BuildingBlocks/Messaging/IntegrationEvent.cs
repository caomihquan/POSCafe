namespace BuildingBlocks.Messaging;

public abstract record IntegrationEvent(
    Guid EventId,
    DateTimeOffset OccurredAt,
    string AggregateId,
    string CorrelationId,
    string? CausationId = null,
    int SchemaVersion = 1);
