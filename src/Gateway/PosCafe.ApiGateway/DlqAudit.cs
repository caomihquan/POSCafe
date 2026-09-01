using System.Text.Json;
using BuildingBlocks.Observability;

namespace PosCafe.ApiGateway;

public static class DlqAudit
{
    public static void Add(OpsDbContext db, string action, Guid auditEntityId, Guid eventId, Guid? replayId, string sourceTopic, string targetTopic, string actorId, string? correlationId, object? metadata = null)
    {
        db.Set<AuditEntry>().Add(new AuditEntry { Action = action, EntityType = "DlqReplay", EntityId = auditEntityId.ToString(), ActorId = Guid.TryParse(actorId, out var actor) ? actor : null, CorrelationId = correlationId ?? string.Empty, OccurredAtUtc = DateTime.UtcNow, MetadataJson = JsonSerializer.Serialize(new { replayId, eventId, sourceTopic, targetTopic, metadata }) });
    }
}
