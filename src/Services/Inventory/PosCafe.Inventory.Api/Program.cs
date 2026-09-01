using BuildingBlocks.Exceptions;
using BuildingBlocks.Messaging;
using Microsoft.EntityFrameworkCore;
using PosCafe.Inventory.Domain;
using PosCafe.Inventory.Infrastructure;
using PosCafe.Inventory.Infrastructure.Messaging;
using PosCafe.ServiceDefaults;
using BuildingBlocks.Observability;
using System.Diagnostics;
using System.Text.Json;
using System.Security.Cryptography;
using System.Text;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddPosCafeAuthentication();
builder.AddNpgsqlDbContext<InventoryDbContext>("inventorydb");
builder.Services.Configure<AuditRetentionOptions>(builder.Configuration.GetSection("Audit"));
builder.Services.AddSingleton(sp => new AuditArchiveClient(new AuditArchiveOptions { Enabled = sp.GetRequiredService<IConfiguration>().GetValue("Audit:Archive:Enabled", false), ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("auditarchive") ?? sp.GetRequiredService<IConfiguration>()["Audit:Archive:ConnectionString"] ?? string.Empty, ContainerName = sp.GetRequiredService<IConfiguration>()["Audit:Archive:ContainerName"] ?? "audit-archive", ServiceName = "inventory" }));
builder.Services.AddHostedService<AuditRetentionService>();
builder.Services.Configure<InventoryMessagingOptions>(options =>
{
    builder.Configuration.GetSection("Inventory:Messaging").Bind(options);
    options.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? options.BootstrapServers;
    options.DeadLetterTopic = string.IsNullOrWhiteSpace(options.DeadLetterTopic) ? "pos.inventory.order-events.dlq" : options.DeadLetterTopic;
});
builder.Services.Configure<OutboxOptions>(options =>
{
    builder.Configuration.GetSection("Outbox:Inventory").Bind(options);
    options.Topic = string.IsNullOrWhiteSpace(options.Topic) ? "pos.inventory.events" : options.Topic;
    options.DeadLetterTopic = string.IsNullOrWhiteSpace(options.DeadLetterTopic) ? "pos.inventory.events.dlq" : options.DeadLetterTopic;
    options.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? options.BootstrapServers;
});
builder.Services.AddSingleton<IProducer<string, string>>(sp => new ProducerBuilder<string, string>(KafkaProducerConfiguration.Create(sp.GetRequiredService<IConfiguration>(), sp.GetRequiredService<IConfiguration>().GetConnectionString("kafka") ?? "localhost:9092")).Build());
builder.Services.AddHostedService<KafkaProducerShutdownService>();
builder.Services.AddSingleton<IAdminClient>(sp => { var configuration = sp.GetRequiredService<IConfiguration>(); var config = new AdminClientConfig { BootstrapServers = configuration.GetConnectionString("kafka") ?? "localhost:9092" }; KafkaProducerConfiguration.ApplySecurity(config, configuration.GetSection("Kafka:Security")); return new AdminClientBuilder(config).Build(); });
builder.Services.AddHostedService<InventoryOrderEventsConsumer>();
builder.Services.AddHostedService<InventoryOutboxPublisher>();
builder.Services.AddHealthChecks().AddDbContextCheck<InventoryDbContext>("inventory-db", tags: ["ready"]).AddCheck<KafkaTopicHealthCheck>("kafka-topics", tags: ["ready"]).AddCheck<KafkaConsumerLagHealthCheck>("consumer-lag", tags: ["ready"]);

var app = builder.Build();
app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

app.MapGet("/api/v1/inventory", async (Guid storeId, Guid productId, InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    if (!principal.CanAccessStore(storeId)) return Results.Forbid();
    var stock = await db.StockItems.AsNoTracking().SingleOrDefaultAsync(x => x.StoreId == storeId && x.ProductId == productId, ct);
    return stock is null ? Results.NotFound() : Results.Ok(StockResponse.From(stock));
});

app.MapPost("/api/v1/inventory/receive", async (StockRequest request, InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
    !principal.CanAccessStore(request.StoreId) ? Results.Forbid() : await ExecuteIdempotent(request, "inventory.received", http, db, principal, async () => { var stock = await db.StockItems.SingleOrDefaultAsync(x => x.StoreId == request.StoreId && x.ProductId == request.ProductId, ct); if (stock is null) { stock = new StockItem(request.StoreId, request.ProductId, request.Quantity); db.StockItems.Add(stock); } else stock.Receive(request.Quantity); return StockResponse.From(stock); }, ct));

app.MapPut("/api/v1/inventory/adjust", async (StockRequest request, InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
    !principal.CanAccessStore(request.StoreId) ? Results.Forbid() : await ExecuteIdempotent(request, "inventory.adjusted", http, db, principal, async () => { var stock = await db.StockItems.SingleOrDefaultAsync(x => x.StoreId == request.StoreId && x.ProductId == request.ProductId, ct) ?? throw new NotFoundException("StockItem", request.ProductId); stock.Adjust(request.Quantity); return StockResponse.From(stock); }, ct));

app.MapPost("/api/v1/inventory/reserve", async (StockRequest request, InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
    !principal.CanAccessStore(request.StoreId) ? Results.Forbid() : await ExecuteIdempotent(request, "inventory.reserved", http, db, principal, async () => { var stock = await db.StockItems.SingleOrDefaultAsync(x => x.StoreId == request.StoreId && x.ProductId == request.ProductId, ct) ?? throw new NotFoundException("StockItem", request.ProductId); stock.Reserve(request.Quantity); return StockResponse.From(stock); }, ct));

app.MapPost("/api/v1/inventory/release", async (StockRequest request, InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
    !principal.CanAccessStore(request.StoreId) ? Results.Forbid() : await ExecuteIdempotent(request, "inventory.released", http, db, principal, async () => { var stock = await db.StockItems.SingleOrDefaultAsync(x => x.StoreId == request.StoreId && x.ProductId == request.ProductId, ct) ?? throw new NotFoundException("StockItem", request.ProductId); stock.Release(request.Quantity); return StockResponse.From(stock); }, ct));

if (app.Environment.IsDevelopment())
{
    await using var scope = app.Services.CreateAsyncScope();
    await scope.ServiceProvider.GetRequiredService<InventoryDbContext>().Database.MigrateAsync();
}

static void AddAudit(InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, string action, string entityId, Guid storeId)
{
    db.AuditEntries.Add(new AuditEntry { Action = action, EntityType = "StockItem", EntityId = entityId, StoreId = storeId, ActorId = Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null, CorrelationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N"), OccurredAtUtc = DateTime.UtcNow });
}

static async Task<IResult> ExecuteIdempotent(StockRequest request, string action, HttpRequest http, InventoryDbContext db, System.Security.Claims.ClaimsPrincipal principal, Func<Task<StockResponse>> mutation, CancellationToken ct)
{
    string key;
    try { key = Idempotency.ValidateKey(http.Headers["Idempotency-Key"].ToString()); }
    catch (ArgumentException exception) { return Results.BadRequest(new { message = exception.Message }); }
    var hash = Idempotency.Hash(request);
    var existing = await db.InventoryIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
    if (existing is not null)
    {
        if (!Idempotency.Matches(existing.RequestHash, hash)) throw new ConflictException("Idempotency-Key is already bound to a different inventory request.");
        MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "inventory"));
        http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true";
        return Results.Ok(JsonSerializer.Deserialize<StockResponse>(existing.ResponseJson)!);
    }
    try
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);
        var response = await mutation();
        AddAudit(db, principal, action, $"{request.StoreId}:{request.ProductId}", request.StoreId);
        db.InventoryIdempotencyRecords.Add(new InventoryIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, ResponseJson = JsonSerializer.Serialize(response), CreatedAtUtc = DateTime.UtcNow });
        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
        http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false";
        return Results.Ok(response);
    }
    catch (DbUpdateException)
    {
        db.ChangeTracker.Clear();
        var winner = await db.InventoryIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
        if (winner is null) throw;
        if (!Idempotency.Matches(winner.RequestHash, hash)) throw new ConflictException("Idempotency-Key is already bound to a different inventory request.");
        MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "inventory"));
        http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true";
        return Results.Ok(JsonSerializer.Deserialize<StockResponse>(winner.ResponseJson)!);
    }
}

app.Run();

record StockRequest(Guid StoreId, Guid ProductId, decimal Quantity);
record StockResponse(Guid StoreId, Guid ProductId, decimal Quantity, decimal ReservedQuantity, decimal AvailableQuantity, int Version)
{
    public static StockResponse From(StockItem stock) => new(stock.StoreId, stock.ProductId, stock.Quantity, stock.ReservedQuantity, stock.AvailableQuantity, stock.Version);
}
