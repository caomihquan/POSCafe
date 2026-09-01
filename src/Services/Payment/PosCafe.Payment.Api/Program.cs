using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Infrastructure.Messaging;
using PosCafe.Payment.Infrastructure.Persistence;
using PosCafe.Payment.Application;
using PosCafe.Payment.Infrastructure;
using PosCafe.ServiceDefaults;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddPosCafeAuthentication();
builder.AddNpgsqlDbContext<PaymentDbContext>("paymentdb");
builder.Services.Configure<OutboxOptions>(options =>
{
    builder.Configuration.GetSection("Outbox:Payment").Bind(options);
    options.Topic = string.IsNullOrWhiteSpace(options.Topic) ? "pos.payment.events" : options.Topic;
    options.InputTopic = string.IsNullOrWhiteSpace(options.InputTopic) ? "pos.order.events" : options.InputTopic;
    options.ConsumerGroup = string.IsNullOrWhiteSpace(options.ConsumerGroup) ? "pos-payment-order-events-v1" : options.ConsumerGroup;
    options.DeadLetterTopic = string.IsNullOrWhiteSpace(options.DeadLetterTopic) ? "pos.payment.order-events.dlq" : options.DeadLetterTopic;
    options.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? options.BootstrapServers;
});
builder.Services.AddSingleton<IProducer<string, string>>(sp => new ProducerBuilder<string, string>(KafkaProducerConfiguration.Create(sp.GetRequiredService<IConfiguration>(), sp.GetRequiredService<IConfiguration>().GetConnectionString("kafka") ?? "localhost:9092")).Build());
builder.Services.AddHostedService<KafkaProducerShutdownService>();
builder.Services.AddSingleton<IAdminClient>(sp => { var configuration = sp.GetRequiredService<IConfiguration>(); var config = new AdminClientConfig { BootstrapServers = configuration.GetConnectionString("kafka") ?? "localhost:9092" }; KafkaProducerConfiguration.ApplySecurity(config, configuration.GetSection("Kafka:Security")); return new AdminClientBuilder(config).Build(); });
builder.Services.AddHostedService<PaymentOutboxPublisher>();
builder.Services.Configure<AuditRetentionOptions>(builder.Configuration.GetSection("Audit"));
builder.Services.AddSingleton(sp => new AuditArchiveClient(new AuditArchiveOptions { Enabled = sp.GetRequiredService<IConfiguration>().GetValue("Audit:Archive:Enabled", false), ConnectionString = sp.GetRequiredService<IConfiguration>().GetConnectionString("auditarchive") ?? sp.GetRequiredService<IConfiguration>()["Audit:Archive:ConnectionString"] ?? string.Empty, ContainerName = sp.GetRequiredService<IConfiguration>()["Audit:Archive:ContainerName"] ?? "audit-archive", ServiceName = "payment" }));
builder.Services.AddHostedService<PosCafe.Payment.Infrastructure.AuditRetentionService>();
builder.Services.AddHostedService<OrderEventsConsumer>();
builder.Services.AddHealthChecks().AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"]).AddCheck<KafkaTopicHealthCheck>("kafka-topics", tags: ["ready"]).AddCheck<KafkaConsumerLagHealthCheck>("consumer-lag", tags: ["ready"]);
builder.Services.AddScoped<IPaymentCommandService, PaymentCommandService>();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();
app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

app.MapPost("/api/v1/payments", async (CreatePaymentCommand command, IPaymentCommandService service, PaymentDbContext db, System.Security.Claims.ClaimsPrincipal principal, HttpRequest request, CancellationToken ct) =>
{
    var order = await db.OrderProjections.AsNoTracking().SingleOrDefaultAsync(x => x.OrderId == command.OrderId, ct);
    if (order is null) return Results.NotFound("Order projection is not available yet.");
    if (!principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    var idempotencyKey = request.Headers["Idempotency-Key"].ToString();
    if (string.IsNullOrWhiteSpace(idempotencyKey) || idempotencyKey.Length > 200) return Results.BadRequest(new { message = "Idempotency-Key header is required and must be at most 200 characters." });
    var requestHash = Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(command))));
    var result = await service.CreateAsync(command with { ActorId = Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null }, idempotencyKey, requestHash, ct);
    request.HttpContext.Response.Headers["Idempotency-Replayed"] = result.IdempotencyReplayed ? "true" : "false";
    return Results.Created($"/api/v1/payments/{result.PaymentId}", result);
}).RequireAuthorization("payment-operator");
app.MapPost("/api/v1/payments/{id:guid}/authorize", async (Guid id, IPaymentCommandService service, PaymentDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var payment = await db.Payments.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    if (payment is null) return Results.NotFound();
    var order = await db.OrderProjections.AsNoTracking().SingleOrDefaultAsync(x => x.OrderId == payment.OrderId, ct);
    if (order is null) return Results.NotFound("Order projection is not available yet.");
    if (!principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    return Results.Ok(await service.AuthorizeAsync(new PaymentActionCommand(id, Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null), ct));
}).RequireAuthorization("payment-operator");
app.MapPost("/api/v1/payments/{id:guid}/refund", async (Guid id, IPaymentCommandService service, PaymentDbContext db, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
{
    var payment = await db.Payments.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    if (payment is null) return Results.NotFound();
    var order = await db.OrderProjections.AsNoTracking().SingleOrDefaultAsync(x => x.OrderId == payment.OrderId, ct);
    if (order is null) return Results.NotFound("Order projection is not available yet.");
    if (!principal.CanAccessStore(order.StoreId)) return Results.Forbid();
    return Results.Ok(await service.RefundAsync(new PaymentActionCommand(id, Guid.TryParse(principal.FindFirst("sub")?.Value, out var actorId) ? actorId : null), ct));
}).RequireAuthorization("payment-operator");

if (app.Environment.IsDevelopment())
{
    using var scope = app.Services.CreateScope();
    await scope.ServiceProvider.GetRequiredService<PaymentDbContext>().Database.MigrateAsync();
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

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
