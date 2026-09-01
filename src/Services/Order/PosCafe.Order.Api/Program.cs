using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using PosCafe.Order.Infrastructure.Messaging;
using PosCafe.Order.Infrastructure.Persistence;
using PosCafe.Order.Application;
using PosCafe.Order.Domain;
using PosCafe.Order.Infrastructure;
using PosCafe.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddPosCafeAuthentication();
builder.AddNpgsqlDbContext<OrderDbContext>("orderdb");
builder.Services.Configure<OutboxOptions>(options =>
{
    builder.Configuration.GetSection("Outbox:Order").Bind(options);
    options.Topic = string.IsNullOrWhiteSpace(options.Topic) ? "pos.order.events" : options.Topic;
    options.DeadLetterTopic = string.IsNullOrWhiteSpace(options.DeadLetterTopic) ? "pos.order.events.dlq" : options.DeadLetterTopic;
    options.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? options.BootstrapServers;
});
builder.Services.AddSingleton<IProducer<string, string>>(sp => new ProducerBuilder<string, string>(KafkaProducerConfiguration.Create(sp.GetRequiredService<IConfiguration>(), sp.GetRequiredService<IConfiguration>().GetConnectionString("kafka") ?? "localhost:9092")).Build());
builder.Services.AddHostedService<KafkaProducerShutdownService>();
builder.Services.AddSingleton<IAdminClient>(sp => { var configuration = sp.GetRequiredService<IConfiguration>(); var config = new AdminClientConfig { BootstrapServers = configuration.GetConnectionString("kafka") ?? "localhost:9092" }; KafkaProducerConfiguration.ApplySecurity(config, configuration.GetSection("Kafka:Security")); return new AdminClientBuilder(config).Build(); });
builder.Services.AddHostedService<OrderOutboxPublisher>();
builder.Services.Configure<SagaMessagingOptions>(options =>
{
    builder.Configuration.GetSection("Saga:OrderFulfillment").Bind(options);
    options.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? options.BootstrapServers;
    options.InputTopics = options.InputTopics.Length == 0 ? ["pos.order.events", "pos.payment.events", "pos.inventory.events"] : options.InputTopics;
    options.DeadLetterTopic = string.IsNullOrWhiteSpace(options.DeadLetterTopic) ? "pos.order.fulfillment-saga.dlq" : options.DeadLetterTopic;
});
builder.Services.AddHostedService<OrderFulfillmentSagaOrchestrator>();
builder.Services.Configure<AuditRetentionOptions>(builder.Configuration.GetSection("Audit"));
builder.Services.AddSingleton(sp => new AuditArchiveClient(new AuditArchiveOptions { Enabled = sp.GetRequiredService<IConfiguration>().GetValue("Audit:Archive:Enabled", false), ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("auditarchive") ?? sp.GetRequiredService<IConfiguration>()["Audit:Archive:ConnectionString"] ?? string.Empty, ContainerName = sp.GetRequiredService<IConfiguration>()["Audit:Archive:ContainerName"] ?? "audit-archive", ServiceName = "order" }));
builder.Services.AddHostedService<PosCafe.Order.Infrastructure.AuditRetentionService>();
builder.Services.AddScoped<IOrderCommandService, OrderCommandService>();
builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"]).AddCheck<KafkaTopicHealthCheck>("kafka-topics", tags: ["ready"]);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();
app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

app.MapPost("/api/v1/orders", async (CreateOrderCommand command, IOrderCommandService service, System.Security.Claims.ClaimsPrincipal principal, HttpRequest request, CancellationToken ct) =>
{
    if (!principal.CanAccessStore(command.StoreId)) return Results.Forbid();
    var idempotencyKey = request.Headers["Idempotency-Key"].ToString();
    if (string.IsNullOrWhiteSpace(idempotencyKey) || idempotencyKey.Length > 200) return Results.BadRequest(new { message = "Idempotency-Key header is required and must be at most 200 characters." });
    var requestHash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(command))));
    var result = await service.CreateAsync(command with { ActorId = Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null }, idempotencyKey, requestHash, ct);
    request.HttpContext.Response.Headers["Idempotency-Replayed"] = result.IdempotencyReplayed ? "true" : "false";
    return Results.Created($"/api/v1/orders/{result.OrderId}", result);
}).RequireAuthorization("order-operator");
app.MapPost("/api/v1/orders/{id:guid}/confirm", async (Guid id, IOrderCommandService service, OrderDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var order = await db.Orders.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    if (order is null) return Results.NotFound();
    if (!principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    return Results.Ok(await service.ConfirmAsync(new ConfirmOrderCommand(id, Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null), ct));
}).RequireAuthorization("order-operator");
app.MapPost("/api/v1/orders/{id:guid}/cancel", async (Guid id, CancelRequest request, IOrderCommandService service, OrderDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var order = await db.Orders.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    if (order is null) return Results.NotFound();
    if (!principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    return Results.Ok(await service.CancelAsync(new CancelOrderCommand(id, request.Reason, Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null), ct));
}).RequireAuthorization("order-operator");

app.MapGet("/api/v1/orders/{id:guid}", async (Guid id, OrderDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var order = await db.Orders.AsNoTracking().Include(x => x.Lines).SingleOrDefaultAsync(x => x.Id == id, ct);
    if (order is not null && !principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    return order is null ? Results.NotFound() : Results.Ok(OrderResponse.From(order));
});

app.MapGet("/api/v1/orders/{id:guid}/fulfillment", async (Guid id, OrderDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var order = await db.Orders.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    if (order is null) return Results.NotFound();
    if (!principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    var saga = await db.OrderFulfillmentSagas.AsNoTracking().SingleOrDefaultAsync(x => x.OrderId == id, ct);
    return saga is null ? Results.NotFound("Order fulfillment saga has not started yet.") : Results.Ok(new
    {
        saga.SagaId, saga.OrderId, saga.Status, saga.PaymentAuthorized, saga.InventoryReserved,
        saga.InventoryReservationFailed, saga.PaymentRefundRequested, saga.PaymentId, saga.LastError,
        saga.CreatedAtUtc, saga.UpdatedAtUtc
    });
}).RequireAuthorization("order-operator");

app.MapGet("/api/v1/orders", async (Guid storeId, string? status, int? limit, OrderDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    if (!principal.CanAccessStore(storeId)) return Results.Forbid();
    var query = db.Orders.AsNoTracking().Include(x => x.Lines).Where(x => x.StoreId == storeId);
    if (!string.IsNullOrWhiteSpace(status) && Enum.TryParse<OrderStatus>(status, true, out var parsedStatus)) query = query.Where(x => x.Status == parsedStatus);
    var orders = await query.OrderByDescending(x => x.CreatedAtUtc).Take(Math.Clamp(limit ?? 50, 1, 200)).ToListAsync(ct);
    return Results.Ok(orders.Select(OrderResponse.From));
});

if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    await scope.ServiceProvider.GetRequiredService<OrderDbContext>().Database.MigrateAsync();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast =  Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast");

app.Run();

record CancelRequest(string Reason);

record OrderResponse(Guid Id, Guid StoreId, string Channel, string Status, DateTimeOffset CreatedAtUtc, DateTimeOffset? ConfirmedAtUtc, decimal Subtotal, IReadOnlyCollection<OrderLineResponse> Lines)
{
    public static OrderResponse From(PosCafe.Order.Domain.Order order) => new(order.Id, order.StoreId, order.Channel, order.Status.ToString(), order.CreatedAtUtc, order.ConfirmedAtUtc, order.Subtotal, order.Lines.Select(x => new OrderLineResponse(x.ProductId, x.ProductName, x.UnitPrice, x.Quantity, x.Total)).ToArray());
}

record OrderLineResponse(Guid ProductId, string ProductName, decimal UnitPrice, int Quantity, decimal Total);

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
