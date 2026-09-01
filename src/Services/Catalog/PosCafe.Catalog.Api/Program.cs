using Microsoft.EntityFrameworkCore;
using BuildingBlocks.Exceptions;
using PosCafe.Catalog.Domain.Entities;
using PosCafe.Catalog.Infrastructure.Persistence;
using PosCafe.Catalog.Infrastructure;
using BuildingBlocks.Observability;
using BuildingBlocks.Messaging;
using System.Diagnostics;
using System.Text.Json;
using System.Security.Cryptography;
using System.Text;


var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddPosCafeAuthentication();
builder.AddNpgsqlDbContext<CatalogDbContext>("catalogdb");
builder.Services.Configure<AuditRetentionOptions>(builder.Configuration.GetSection("Audit"));
builder.Services.AddSingleton(sp => new AuditArchiveClient(new AuditArchiveOptions { Enabled = sp.GetRequiredService<IConfiguration>().GetValue("Audit:Archive:Enabled", false), ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("auditarchive") ?? sp.GetRequiredService<IConfiguration>()["Audit:Archive:ConnectionString"] ?? string.Empty, ContainerName = sp.GetRequiredService<IConfiguration>()["Audit:Archive:ContainerName"] ?? "audit-archive", ServiceName = "catalog" }));
builder.Services.AddHostedService<PosCafe.Catalog.Infrastructure.AuditRetentionService>();

var app = builder.Build();

app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

app.MapGet("/api/v1/catalog/categories", async (int? page, int? pageSize, CatalogDbContext db, CancellationToken ct) =>
{
    var number = Math.Max(1, page ?? 1);
    var size = Math.Clamp(pageSize ?? 50, 1, 200);
    var query = db.Categories.AsNoTracking().Where(x => x.IsActive).OrderBy(x => x.Name);
    var total = await query.CountAsync(ct);
    var items = await query.Skip((number - 1) * size).Take(size).ToListAsync(ct);
    return Results.Ok(new { page = number, pageSize = size, total, items });
});

app.MapPost("/api/v1/catalog/categories", async (CategoryRequest request, CatalogDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.Name)) throw new ValidationException("Category name is required.");
    var (key, hash, existing) = await GetIdempotencyAsync(request, "Category", http, db, ct);
    if (existing is not null) { MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "catalog")); http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.Created($"/api/v1/catalog/categories/{existing.ResourceId}", await db.Categories.AsNoTracking().SingleAsync(x => x.Id == existing.ResourceId, ct)); }
    await using var transaction = await db.Database.BeginTransactionAsync(ct);
    var category = new Category(request.Name);
    db.Categories.Add(category);
    AddAudit(db, principal, "catalog.category-created", category.Id.ToString());
    db.CatalogIdempotencyRecords.Add(new CatalogIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, ResourceId = category.Id, ResourceType = "Category", CreatedAtUtc = DateTime.UtcNow });
    try
    {
        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
    catch (DbUpdateException)
    {
        await transaction.RollbackAsync(CancellationToken.None);
        db.ChangeTracker.Clear();
        var winner = await db.CatalogIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
        if (winner is not null && Idempotency.Matches(winner.RequestHash, hash) && winner.ResourceType == "Category")
        {
            http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true";
            return Results.Created($"/api/v1/catalog/categories/{winner.ResourceId}", await db.Categories.AsNoTracking().SingleAsync(x => x.Id == winner.ResourceId, ct));
        }
        throw;
    }
    http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false";
    return Results.Created($"/api/v1/catalog/categories/{category.Id}", category);
}).RequireAuthorization("catalog-manager");

app.MapGet("/api/v1/catalog/products", async (int? page, int? pageSize, Guid? categoryId, CatalogDbContext db, CancellationToken ct) =>
{
    var number = Math.Max(1, page ?? 1);
    var size = Math.Clamp(pageSize ?? 50, 1, 200);
    var query = db.Products.AsNoTracking().Where(x => x.IsActive);
    if (categoryId is { } id && id != Guid.Empty) query = query.Where(x => x.CategoryId == id);
    var ordered = query.OrderBy(x => x.Name);
    var total = await ordered.CountAsync(ct);
    var items = await ordered.Skip((number - 1) * size).Take(size).ToListAsync(ct);
    return Results.Ok(new { page = number, pageSize = size, total, items });
});

app.MapPost("/api/v1/catalog/products", async (ProductRequest request, CatalogDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest http, CancellationToken ct) =>
{
    if (request.CategoryId == Guid.Empty) throw new ValidationException("Category is required.");
    if (string.IsNullOrWhiteSpace(request.Name)) throw new ValidationException("Product name is required.");
    if (request.Price < 0) throw new ValidationException("Product price cannot be negative.");
    if (!await db.Categories.AnyAsync(x => x.Id == request.CategoryId && x.IsActive, ct)) throw new NotFoundException("Category", request.CategoryId);
    var (key, hash, existing) = await GetIdempotencyAsync(request, "Product", http, db, ct);
    if (existing is not null) { MessagingMetrics.IdempotencyReplays.Add(1, new KeyValuePair<string, object?>("service", "catalog")); http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.Created($"/api/v1/catalog/products/{existing.ResourceId}", await db.Products.AsNoTracking().SingleAsync(x => x.Id == existing.ResourceId, ct)); }
    await using var transaction = await db.Database.BeginTransactionAsync(ct);
    var product = new Product(request.CategoryId, request.Name, request.Price);
    db.Products.Add(product);
    AddAudit(db, principal, "catalog.product-created", product.Id.ToString());
    db.CatalogIdempotencyRecords.Add(new CatalogIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, ResourceId = product.Id, ResourceType = "Product", CreatedAtUtc = DateTime.UtcNow });
    try
    {
        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
    catch (DbUpdateException)
    {
        await transaction.RollbackAsync(CancellationToken.None);
        db.ChangeTracker.Clear();
        var winner = await db.CatalogIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
        if (winner is not null && Idempotency.Matches(winner.RequestHash, hash) && winner.ResourceType == "Product")
        {
            http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true";
            return Results.Created($"/api/v1/catalog/products/{winner.ResourceId}", await db.Products.AsNoTracking().SingleAsync(x => x.Id == winner.ResourceId, ct));
        }
        throw;
    }
    http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false";
    return Results.Created($"/api/v1/catalog/products/{product.Id}", product);
}).RequireAuthorization("catalog-manager");

app.MapPut("/api/v1/catalog/products/{id:guid}/price", async (Guid id, PriceRequest request, CatalogDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    if (request.Price < 0) throw new ValidationException("Product price cannot be negative.");
    var product = await db.Products.SingleOrDefaultAsync(x => x.Id == id && x.IsActive, ct) ?? throw new NotFoundException("Product", id);
    product.UpdatePrice(request.Price);
    AddAudit(db, principal, "catalog.product-price-updated", product.Id.ToString());
    await db.SaveChangesAsync(ct);
    return Results.Ok(product);
}).RequireAuthorization("catalog-manager");

app.MapDelete("/api/v1/catalog/products/{id:guid}", async (Guid id, CatalogDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var product = await db.Products.SingleOrDefaultAsync(x => x.Id == id && x.IsActive, ct) ?? throw new NotFoundException("Product", id);
    product.Deactivate();
    AddAudit(db, principal, "catalog.product-deactivated", product.Id.ToString());
    await db.SaveChangesAsync(ct);
    return Results.NoContent();
}).RequireAuthorization("catalog-manager");

if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    var db = scope.ServiceProvider.GetRequiredService<CatalogDbContext>();
    await db.Database.MigrateAsync();
}

static void AddAudit(CatalogDbContext db, System.Security.Claims.ClaimsPrincipal principal, string action, string entityId) => db.AuditEntries.Add(new AuditEntry { Action = action, EntityType = "Catalog", EntityId = entityId, ActorId = Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null, CorrelationId = Activity.Current?.TraceId.ToString() ?? Guid.NewGuid().ToString("N"), OccurredAtUtc = DateTime.UtcNow });

static async Task<(string Key, string Hash, CatalogIdempotencyRecord? Existing)> GetIdempotencyAsync<T>(T request, string resourceType, HttpRequest http, CatalogDbContext db, CancellationToken ct)
{
    var key = Idempotency.ValidateKey(http.Headers["Idempotency-Key"].ToString());
    var hash = Idempotency.Hash(request);
    var existing = await db.CatalogIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
    if (existing is not null && (!Idempotency.Matches(existing.RequestHash, hash) || existing.ResourceType != resourceType)) throw new ConflictException("Idempotency-Key is already bound to a different catalog request.");
    return (key, hash, existing);
}

app.Run();

record CategoryRequest(string Name);
record ProductRequest(Guid CategoryId, string Name, decimal Price);
record PriceRequest(decimal Price);


