using Microsoft.EntityFrameworkCore;

namespace BuildingBlocks.Messaging;

public static class InboxProcessor
{
    public static async Task<bool> TryStartAsync(DbContext db, Guid eventId, string consumer, CancellationToken cancellationToken)
    {
        var existing = await db.Set<InboxMessage>().SingleOrDefaultAsync(x => x.EventId == eventId && x.Consumer == consumer, cancellationToken);
        if (existing is not null) return existing.ProcessedOnUtc is null;
        db.Set<InboxMessage>().Add(new InboxMessage { EventId = eventId, Consumer = consumer, ReceivedOnUtc = DateTime.UtcNow, Attempts = 1, LastAttemptOnUtc = DateTime.UtcNow });
        try { await db.SaveChangesAsync(cancellationToken); return true; }
        catch (DbUpdateException) { db.ChangeTracker.Clear(); return false; }
    }

    public static async Task<int> RegisterAttemptAsync(DbContext db, Guid eventId, string consumer, CancellationToken cancellationToken)
    {
        var message = await db.Set<InboxMessage>().SingleOrDefaultAsync(x => x.EventId == eventId && x.Consumer == consumer, cancellationToken);
        if (message is null)
        {
            message = new InboxMessage { EventId = eventId, Consumer = consumer, ReceivedOnUtc = DateTime.UtcNow };
            db.Set<InboxMessage>().Add(message);
        }

        if (message.ProcessedOnUtc is not null) return message.Attempts;

        message.Attempts++;
        message.LastAttemptOnUtc = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
        return message.Attempts;
    }

    public static async Task MarkProcessedAsync(DbContext db, Guid eventId, string consumer, CancellationToken cancellationToken)
    {
        var message = await db.Set<InboxMessage>().SingleAsync(x => x.EventId == eventId && x.Consumer == consumer, cancellationToken);
        message.ProcessedOnUtc = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
    }

    public static async Task MarkDeadLetteredAsync(DbContext db, Guid eventId, string consumer, string error, CancellationToken cancellationToken)
    {
        var message = await db.Set<InboxMessage>().SingleAsync(x => x.EventId == eventId && x.Consumer == consumer, cancellationToken);
        message.Error = error;
        message.ProcessedOnUtc = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
    }
}
