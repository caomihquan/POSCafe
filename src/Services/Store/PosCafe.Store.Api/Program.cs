using BuildingBlocks.Exceptions;
using Microsoft.EntityFrameworkCore;
using PosCafe.Store.Domain;
using PosCafe.Store.Infrastructure;
using BuildingBlocks.Observability;
using BuildingBlocks.Messaging;
using System.Text.Json;
using System.Diagnostics;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddPosCafeAuthentication();
builder.AddNpgsqlDbContext<StoreDbContext>("storedb");
builder.Services.Configure<AuditRetentionOptions>(builder.Configuration.GetSection("Audit"));
builder.Services.Configure<IdempotencyRetentionOptions>(builder.Configuration.GetSection("Idempotency"));
builder.Services.AddSingleton(sp => new AuditArchiveClient(new AuditArchiveOptions { Enabled = sp.GetRequiredService<IConfiguration>().GetValue("Audit:Archive:Enabled", false), ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("auditarchive") ?? sp.GetRequiredService<IConfiguration>()["Audit:Archive:ConnectionString"] ?? string.Empty, ContainerName = sp.GetRequiredService<IConfiguration>()["Audit:Archive:ContainerName"] ?? "audit-archive", ServiceName = "store" }));
builder.Services.AddHostedService<AuditRetentionService>();
builder.Services.AddHostedService<StoreIdempotencyRetentionService>();
builder.Services.AddHealthChecks().AddDbContextCheck<StoreDbContext>("store-db", tags: ["ready"]);

var app = builder.Build();
app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

app.MapGet("/api/v1/stores", async (StoreDbContext db, CancellationToken ct) =>
    Results.Ok(await db.Stores.AsNoTracking().Where(x => x.IsActive).OrderBy(x => x.Name).ToListAsync(ct)));

app.MapPost("/api/v1/stores", async (StoreRequest request, StoreDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
{
    var (key, hash, existing) = await GetIdempotencyAsync(request, "store.create", http, db, ct);
    if (existing is not null) { MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "store")); http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.Created($"/api/v1/stores/{existing.ResourceId}", JsonSerializer.Deserialize<StoreResponse>(existing.ResponseJson)!); }
    await using var transaction = await db.Database.BeginTransactionAsync(ct);
    var store = new Store(request.Code, request.Name, request.TimeZone);
    db.Stores.Add(store); AddAudit(db, principal, "store.created", store.Id);
    var response = new StoreResponse(store.Id, store.Code, store.Name, store.TimeZone, store.IsActive);
    db.StoreIdempotencyRecords.Add(new StoreIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, Operation = "store.create", ResourceId = store.Id, ResponseJson = JsonSerializer.Serialize(response), StatusCode = 201, CreatedAtUtc = DateTime.UtcNow });
    try { await db.SaveChangesAsync(ct); await transaction.CommitAsync(ct); }
    catch (DbUpdateException) { await transaction.RollbackAsync(CancellationToken.None); db.ChangeTracker.Clear(); var winner = await db.StoreIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct); if (winner is not null && Idempotency.Matches(winner.RequestHash, hash) && winner.Operation == "store.create") { http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.Created($"/api/v1/stores/{winner.ResourceId}", JsonSerializer.Deserialize<StoreResponse>(winner.ResponseJson)!); } throw; }
    http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false";
    return Results.Created($"/api/v1/stores/{store.Id}", store);
}).RequireAuthorization("store-manager");

app.MapPut("/api/v1/stores/{id:guid}", async (Guid id, StoreRequest request, StoreDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
{
    var (key, hash, existing) = await GetIdempotencyAsync(new StoreMutationRequest(id, request), "store.update", http, db, ct);
    if (existing is not null) { http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.Ok(JsonSerializer.Deserialize<StoreResponse>(existing.ResponseJson)!); }
    var store = await db.Stores.SingleOrDefaultAsync(x => x.Id == id && x.IsActive, ct) ?? throw new NotFoundException("Store", id);
    store.Update(request.Name, request.TimeZone); AddAudit(db, principal, "store.updated", store.Id);
    var response = new StoreResponse(store.Id, store.Code, store.Name, store.TimeZone, store.IsActive);
    db.StoreIdempotencyRecords.Add(new StoreIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, Operation = "store.update", ResourceId = store.Id, ResponseJson = JsonSerializer.Serialize(response), StatusCode = 200, CreatedAtUtc = DateTime.UtcNow });
    try { await db.SaveChangesAsync(ct); }
    catch (DbUpdateException) { db.ChangeTracker.Clear(); var winner = await db.StoreIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct); if (winner is not null && Idempotency.Matches(winner.RequestHash, hash) && winner.Operation == "store.update") { http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.Ok(JsonSerializer.Deserialize<StoreResponse>(winner.ResponseJson)!); } throw; }
    http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false"; return Results.Ok(store);
}).RequireAuthorization("store-manager");

app.MapDelete("/api/v1/stores/{id:guid}", async (Guid id, StoreDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
{
    var (key, hash, existing) = await GetIdempotencyAsync(new { id }, "store.delete", http, db, ct);
    if (existing is not null) { http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.NoContent(); }
    var store = await db.Stores.SingleOrDefaultAsync(x => x.Id == id && x.IsActive, ct) ?? throw new NotFoundException("Store", id);
    store.Deactivate(); AddAudit(db, principal, "store.deactivated", store.Id);
    db.StoreIdempotencyRecords.Add(new StoreIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, Operation = "store.delete", ResourceId = store.Id, ResponseJson = "", StatusCode = 204, CreatedAtUtc = DateTime.UtcNow });
    try { await db.SaveChangesAsync(ct); }
    catch (DbUpdateException) { db.ChangeTracker.Clear(); var winner = await db.StoreIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct); if (winner is not null && Idempotency.Matches(winner.RequestHash, hash) && winner.Operation == "store.delete") { http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.NoContent(); } throw; }
    http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false"; return Results.NoContent();
}).RequireAuthorization("store-manager");

if (app.Environment.IsDevelopment())
{
    await using var scope = app.Services.CreateAsyncScope();
    await scope.ServiceProvider.GetRequiredService<StoreDbContext>().Database.MigrateAsync();
}

static void AddAudit(StoreDbContext db, System.Security.Claims.ClaimsPrincipal principal, string action, Guid storeId) => db.AuditEntries.Add(new AuditEntry { Action = action, EntityType = "Store", EntityId = storeId.ToString(), StoreId = storeId, ActorId = Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null, CorrelationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N"), OccurredAtUtc = DateTime.UtcNow });

static async Task<(string Key, string Hash, StoreIdempotencyRecord? Existing)> GetIdempotencyAsync<T>(T request, string operation, HttpRequest http, StoreDbContext db, CancellationToken ct)
{
    var key = Idempotency.ValidateKey(http.Headers["Idempotency-Key"].ToString());
    var hash = Idempotency.Hash(request);
    var existing = await db.StoreIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
    if (existing is not null && (!Idempotency.Matches(existing.RequestHash, hash) || existing.Operation != operation)) throw new ConflictException("Idempotency-Key is already bound to a different store request.");
    return (key, hash, existing);
}

app.Run();

record StoreRequest(string Code, string Name, string TimeZone);
record StoreMutationRequest(Guid Id, StoreRequest Request);
record StoreResponse(Guid Id, string Code, string Name, string TimeZone, bool IsActive);
