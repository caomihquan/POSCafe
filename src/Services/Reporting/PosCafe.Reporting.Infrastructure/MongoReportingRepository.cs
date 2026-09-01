using MongoDB.Driver;

namespace PosCafe.Reporting.Infrastructure;

public sealed record DailySalesReadModel(Guid StoreId, DateOnly BusinessDate, decimal GrossSales, int OrderCount, DateTime UpdatedAtUtc);
public sealed record ProcessedReportingEvent(Guid EventId, DateTime? ProcessedAtUtc, int Attempts = 0, DateTime? LastAttemptAtUtc = null, string? Error = null);

public sealed class MongoReportingRepository(IMongoClient client, IMongoDatabase database)
{
    private readonly IMongoCollection<DailySalesReadModel> collection = database.GetCollection<DailySalesReadModel>("daily_sales");
    private readonly IMongoCollection<ProcessedReportingEvent> events = database.GetCollection<ProcessedReportingEvent>("processed_reporting_events");

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken)
    {
        var keys = Builders<DailySalesReadModel>.IndexKeys.Ascending(x => x.StoreId).Ascending(x => x.BusinessDate);
        await collection.Indexes.CreateOneAsync(new CreateIndexModel<DailySalesReadModel>(keys, new CreateIndexOptions { Unique = true, Name = "ux_daily_sales_store_date" }), cancellationToken: cancellationToken);
        await events.Indexes.CreateOneAsync(new CreateIndexModel<ProcessedReportingEvent>(Builders<ProcessedReportingEvent>.IndexKeys.Ascending(x => x.EventId), new CreateIndexOptions { Unique = true, Name = "ux_processed_reporting_event" }), cancellationToken: cancellationToken);
    }

    public async Task<DailySalesReadModel?> GetAsync(Guid storeId, DateOnly businessDate, CancellationToken cancellationToken) =>
        await collection.Find(x => x.StoreId == storeId && x.BusinessDate == businessDate).FirstOrDefaultAsync(cancellationToken);

    public Task UpsertAsync(DailySalesReadModel model, CancellationToken cancellationToken) =>
        collection.ReplaceOneAsync(x => x.StoreId == model.StoreId && x.BusinessDate == model.BusinessDate, model, new ReplaceOptions { IsUpsert = true }, cancellationToken);

    public async Task<int> RegisterAttemptAsync(Guid eventId, CancellationToken cancellationToken)
    {
        var current = await events.Find(x => x.EventId == eventId).FirstOrDefaultAsync(cancellationToken);
        if (current?.ProcessedAtUtc is not null) return current.Attempts;
        var updated = await events.FindOneAndUpdateAsync(
            x => x.EventId == eventId,
            Builders<ProcessedReportingEvent>.Update.SetOnInsert(x => x.EventId, eventId).SetOnInsert(x => x.ProcessedAtUtc, null).Inc(x => x.Attempts, 1).Set(x => x.LastAttemptAtUtc, DateTime.UtcNow),
            new FindOneAndUpdateOptions<ProcessedReportingEvent> { IsUpsert = true, ReturnDocument = ReturnDocument.After }, cancellationToken);
        return updated.Attempts;
    }

    public Task MarkDeadLetteredAsync(Guid eventId, string error, CancellationToken cancellationToken) =>
        events.UpdateOneAsync(x => x.EventId == eventId, Builders<ProcessedReportingEvent>.Update.Set(x => x.ProcessedAtUtc, DateTime.UtcNow).Set(x => x.Error, error), cancellationToken: cancellationToken);

    public async Task<bool> ApplyOrderConfirmedAsync(Guid eventId, Guid storeId, DateOnly businessDate, decimal total, CancellationToken cancellationToken)
    {
        using var session = await client.StartSessionAsync(cancellationToken: cancellationToken);
        session.StartTransaction();
        try
        {
            await events.InsertOneAsync(session, new ProcessedReportingEvent(eventId, DateTime.UtcNow), cancellationToken: cancellationToken);
            var filter = Builders<DailySalesReadModel>.Filter.Eq(x => x.StoreId, storeId) & Builders<DailySalesReadModel>.Filter.Eq(x => x.BusinessDate, businessDate);
            var update = Builders<DailySalesReadModel>.Update.Inc(x => x.GrossSales, total).Inc(x => x.OrderCount, 1).Set(x => x.UpdatedAtUtc, DateTime.UtcNow).SetOnInsert(x => x.StoreId, storeId).SetOnInsert(x => x.BusinessDate, businessDate);
            await collection.UpdateOneAsync(session, filter, update, new UpdateOptions { IsUpsert = true }, cancellationToken);
            await session.CommitTransactionAsync(cancellationToken);
            return true;
        }
        catch (MongoWriteException exception) when (exception.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            await session.AbortTransactionAsync(cancellationToken);
            return false;
        }
    }
}
