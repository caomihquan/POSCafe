using MongoDB.Driver;
using PosCafe.Reporting.Infrastructure;
using System.Security.Cryptography;
using System.Text;
using PosCafe.ServiceDefaults;
using BuildingBlocks.Messaging;
using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddPosCafeAuthentication();

var mongoConnection = builder.Configuration.GetConnectionString("catalogread");
if (string.IsNullOrWhiteSpace(mongoConnection) && !builder.Environment.IsDevelopment())
    throw new InvalidOperationException("The Reporting Mongo connection is required outside development.");
mongoConnection ??= "mongodb://localhost:27017";
var mongoClient = new MongoClient(mongoConnection);
var mongoDatabase = mongoClient.GetDatabase(builder.Configuration["Mongo:Database"] ?? "poscafe_reporting");
var internalApiKeys = builder.Configuration.GetSection("Reporting:InternalApiKeys").Get<string[]>()?.Where(x => !string.IsNullOrWhiteSpace(x)).ToArray() ?? [];
if (internalApiKeys.Length == 0 && !string.IsNullOrWhiteSpace(builder.Configuration["Reporting:InternalApiKey"])) internalApiKeys = [builder.Configuration["Reporting:InternalApiKey"]!];
if (internalApiKeys.Length == 0 && builder.Environment.IsDevelopment()) internalApiKeys = ["development-reporting-key"];
if (internalApiKeys.Length == 0) throw new InvalidOperationException("Reporting:InternalApiKeys must be configured outside development.");
builder.Services.AddSingleton<IMongoDatabase>(mongoDatabase);
builder.Services.AddSingleton<IMongoClient>(mongoClient);
builder.Services.AddSingleton<MongoReportingRepository>();
builder.Services.AddHostedService<MongoReportingHostedService>();
builder.Services.Configure<ReportingMessagingOptions>(options =>
{
    builder.Configuration.GetSection("Reporting:Messaging").Bind(options);
    var kafkaConnection = builder.Configuration.GetConnectionString("kafka");
    if (string.IsNullOrWhiteSpace(kafkaConnection) && !builder.Environment.IsDevelopment())
        throw new InvalidOperationException("The Reporting Kafka connection is required outside development.");
    options.BootstrapServers = kafkaConnection ?? options.BootstrapServers;
    options.DeadLetterTopic = string.IsNullOrWhiteSpace(options.DeadLetterTopic) ? "pos.reporting.order-events.dlq" : options.DeadLetterTopic;
});
builder.Services.AddHostedService<ReportingOrderEventsConsumer>();
builder.Services.AddSingleton<IAdminClient>(sp => { var configuration = sp.GetRequiredService<IConfiguration>(); var config = new AdminClientConfig { BootstrapServers = configuration.GetConnectionString("kafka") ?? "localhost:9092" }; KafkaProducerConfiguration.ApplySecurity(config, configuration.GetSection("Kafka:Security")); return new AdminClientBuilder(config).Build(); });
builder.Services.AddHealthChecks().AddCheck<MongoHealthCheck>("mongodb", tags: ["ready"]).AddCheck<KafkaTopicHealthCheck>("kafka-topics", tags: ["ready"]).AddCheck<KafkaConsumerLagHealthCheck>("consumer-lag", tags: ["ready"]);

var app = builder.Build();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

app.MapGet("/api/v1/reports/daily-sales/{storeId:guid}/{businessDate}", async (Guid storeId, DateOnly businessDate, MongoReportingRepository repository, System.Security.Claims.ClaimsPrincipal principal, CancellationToken ct) =>
    !principal.CanAccessStore(storeId) ? Results.Forbid() : await repository.GetAsync(storeId, businessDate, ct) is { } report ? Results.Ok(report) : Results.NotFound());

app.MapPost("/internal/v1/reporting/daily-sales", async (HttpRequest request, DailySalesReadModel model, MongoReportingRepository repository, CancellationToken ct) =>
{
    var providedKey = request.Headers["X-Internal-Api-Key"].ToString();
    var providedBytes = Encoding.UTF8.GetBytes(providedKey);
    var authorized = internalApiKeys.Any(key =>
    {
        var expectedBytes = Encoding.UTF8.GetBytes(key);
        return providedBytes.Length == expectedBytes.Length && CryptographicOperations.FixedTimeEquals(providedBytes, expectedBytes);
    });
    if (!authorized)
        return Results.Unauthorized();
    await repository.UpsertAsync(model with { UpdatedAtUtc = DateTime.UtcNow }, ct);
    return Results.Accepted($"/api/v1/reports/daily-sales/{model.StoreId}/{model.BusinessDate:yyyy-MM-dd}");
});

app.Run();
