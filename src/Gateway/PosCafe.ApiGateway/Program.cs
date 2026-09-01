using Confluent.Kafka;
using BuildingBlocks.Messaging;
using PosCafe.ServiceDefaults;
using PosCafe.ApiGateway;
using Microsoft.EntityFrameworkCore;
using BuildingBlocks.Observability;
using Microsoft.Extensions.Diagnostics.HealthChecks;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
var kafkaBootstrapServers = builder.Configuration.GetConnectionString("kafka");
if (string.IsNullOrWhiteSpace(kafkaBootstrapServers) && !builder.Environment.IsDevelopment())
    throw new InvalidOperationException("The Gateway Kafka connection is required outside development.");
kafkaBootstrapServers ??= "localhost:9092";
var opsConnectionString = builder.Configuration.GetConnectionString("opsdb");
if (string.IsNullOrWhiteSpace(opsConnectionString) && !builder.Environment.IsDevelopment())
    throw new InvalidOperationException("The Gateway opsdb connection is required outside development.");
builder.Services.AddDbContext<OpsDbContext>(options => options.UseNpgsql(opsConnectionString ?? "Host=localhost;Database=poscafe_ops;Username=postgres;Password=postgres"));
builder.Services.AddHostedService<DlqReplayRetentionService>();
var auditArchiveOptions = new AuditArchiveOptions
{
    Enabled = builder.Configuration.GetValue("Audit:Archive:Enabled", false),
    ConnectionString = builder.Configuration.GetConnectionString("auditarchive") ?? builder.Configuration["Audit:Archive:ConnectionString"] ?? string.Empty,
    ContainerName = builder.Configuration["Audit:Archive:ContainerName"] ?? "audit-archive",
    ServiceName = "gateway"
};
builder.Services.AddSingleton(auditArchiveOptions);
builder.Services.AddSingleton(new AuditArchiveClient(auditArchiveOptions));
builder.Services.AddHostedService<OpsAuditRetentionService>();
builder.Services.AddSingleton<IProducer<string, string>>(sp => new ProducerBuilder<string, string>(KafkaProducerConfiguration.Create(sp.GetRequiredService<IConfiguration>(), kafkaBootstrapServers)).Build());
builder.Services.AddHostedService<KafkaProducerShutdownService>();
builder.Services.AddSingleton(sp => new DlqManagementService(
    sp.GetRequiredService<IProducer<string, string>>(), kafkaBootstrapServers));
builder.Services.AddSingleton<IAdminClient>(sp => { var configuration = sp.GetRequiredService<IConfiguration>(); var config = new AdminClientConfig { BootstrapServers = kafkaBootstrapServers }; KafkaProducerConfiguration.ApplySecurity(config, configuration.GetSection("Kafka:Security")); return new AdminClientBuilder(config).Build(); });
builder.Services.AddHealthChecks()
    .AddDbContextCheck<OpsDbContext>("opsdb", tags: ["ready"])
    .AddCheck<OpsKafkaHealthCheck>("kafka", tags: ["ready"])
    .AddCheck<AuditArchiveConfigurationHealthCheck>("audit-archive", tags: ["ready"])
    .AddCheck<PendingReplayHealthCheck>("pending-replays", tags: ["ready"])
    .AddCheck<AuditRetentionFreshnessHealthCheck>("audit-retention", tags: ["ready"]);
var jwtKey = builder.Configuration["Jwt:Key"];
if (string.IsNullOrWhiteSpace(jwtKey) && builder.Environment.IsDevelopment()) jwtKey = "development-only-key-must-be-at-least-32-characters";
if (string.IsNullOrWhiteSpace(jwtKey) || System.Text.Encoding.UTF8.GetByteCount(jwtKey) < 32)
    throw new InvalidOperationException("Jwt:Key must be configured with at least 32 bytes.");
builder.Services.AddAuthentication("Bearer").AddJwtBearer("Bearer", options =>
{
    options.RequireHttpsMetadata = !builder.Environment.IsDevelopment();
    options.TokenValidationParameters = new Microsoft.IdentityModel.Tokens.TokenValidationParameters
    {
        ValidateIssuerSigningKey = true,
        IssuerSigningKey = new Microsoft.IdentityModel.Tokens.SymmetricSecurityKey(System.Text.Encoding.UTF8.GetBytes(jwtKey)),
        ValidateIssuer = false,
        ValidateAudience = false,
        ValidateLifetime = true,
        ClockSkew = TimeSpan.FromSeconds(30),
        NameClaimType = System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub
    };
});
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("authenticated", policy => policy.RequireAuthenticatedUser());
    options.AddPolicy("operations", policy => policy.RequireRole("admin"));
});
builder.Services.AddReverseProxy().LoadFromConfig(builder.Configuration.GetSection("ReverseProxy"));

var app = builder.Build();
if (app.Environment.IsDevelopment())
{
    await using var scope = app.Services.CreateAsyncScope();
    await scope.ServiceProvider.GetRequiredService<OpsDbContext>().Database.MigrateAsync();
}

app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();
app.MapGet("/", () => Results.Ok(new { service = "poscafe-api-gateway", status = "ready" }));
app.MapGet("/ops/health", async (HealthCheckService healthChecks, CancellationToken ct) =>
{
    var report = await healthChecks.CheckHealthAsync(registration => registration.Tags.Contains("ready"), ct);
    return Results.Json(new { status = report.Status.ToString(), checks = report.Entries.ToDictionary(x => x.Key, x => new { status = x.Value.Status.ToString(), description = x.Value.Description }) }, statusCode: report.Status == HealthStatus.Healthy ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
}).RequireAuthorization("operations").RequireRateLimiting("dlq-operations");
app.MapGet("/ops/dlq/routes", (HttpContext context) => Results.Ok(DlqManagementService.Routes.Where(route => DlqManagementService.CanAccess(route, context.User))))
    .RequireAuthorization("dlq-operations").RequireRateLimiting("dlq-operations");
app.MapGet("/ops/dlq/history", async (int? page, int? pageSize, OpsDbContext db, HttpContext context, CancellationToken ct) =>
{
    var size = Math.Clamp(pageSize ?? 50, 1, 200);
    var number = Math.Max(1, page ?? 1);
    var allowedTopics = DlqManagementService.Routes.Where(route => DlqManagementService.CanAccess(route, context.User)).Select(route => route.SourceTopic).ToArray();
    var records = await db.DlqReplays.AsNoTracking().Where(x => allowedTopics.Contains(x.SourceTopic)).OrderByDescending(x => x.CreatedAtUtc).Skip((number - 1) * size).Take(size).ToListAsync(ct);
    return Results.Ok(new { page = number, pageSize = size, items = records });
}).RequireAuthorization("dlq-operations").RequireRateLimiting("dlq-operations");
app.MapGet("/ops/dlq/summary", async (DateTime? from, DateTime? to, OpsDbContext db, HttpContext context, CancellationToken ct) =>
{
    var end = (to ?? DateTime.UtcNow).ToUniversalTime();
    var start = (from ?? end.AddHours(-24)).ToUniversalTime();
    if (start >= end || end - start > TimeSpan.FromDays(31))
        return Results.BadRequest(new { message = "The time range must be positive and no longer than 31 days." });
    var allowedTopics = DlqManagementService.Routes.Where(route => DlqManagementService.CanAccess(route, context.User)).Select(route => route.SourceTopic).ToArray();
    var rows = await db.DlqReplays.AsNoTracking()
        .Where(x => allowedTopics.Contains(x.SourceTopic) && x.CreatedAtUtc >= start && x.CreatedAtUtc < end)
        .GroupBy(x => new { x.SourceTopic, x.TargetTopic, x.Status })
        .Select(group => new { group.Key.SourceTopic, group.Key.TargetTopic, group.Key.Status, Count = group.Count(), LastCreatedAtUtc = group.Max(x => x.CreatedAtUtc) })
        .OrderByDescending(x => x.LastCreatedAtUtc)
        .ToListAsync(ct);
    return Results.Ok(new { from = start, to = end, total = rows.Sum(x => x.Count), items = rows });
}).RequireAuthorization("dlq-operations").RequireRateLimiting("dlq-operations");
app.MapPost("/ops/dlq/replay", async (DlqReplayRequest request, DlqManagementService service, OpsDbContext db, HttpContext context, CancellationToken ct) =>
{
    var idempotencyKey = context.Request.Headers["Idempotency-Key"].ToString();
    if (string.IsNullOrWhiteSpace(idempotencyKey) || idempotencyKey.Length > 200)
        return Results.BadRequest(new { message = "Idempotency-Key header is required and must be at most 200 characters." });
    var actor = context.User.FindFirst(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub)?.Value ?? "unknown";
    var correlationId = context.Request.Headers["X-Correlation-Id"].ToString();
    var route = DlqManagementService.Routes.SingleOrDefault(x => x.SourceTopic == request.SourceTopic && x.TargetTopic == request.TargetTopic);
    if (route is null || !DlqManagementService.CanAccess(route, context.User))
    {
        DlqAudit.Add(db, "DlqReplayDenied", request.EventId, request.EventId, null, request.SourceTopic, request.TargetTopic, actor, correlationId, new { reason = "topic_scope" });
        await db.SaveChangesAsync(ct);
        return Results.Forbid();
    }
    var existing = await db.DlqReplays.SingleOrDefaultAsync(x => x.IdempotencyKey == idempotencyKey, ct);
    if (existing is not null)
    {
        if (existing.EventId != request.EventId || existing.SourceTopic != request.SourceTopic || existing.TargetTopic != request.TargetTopic)
            return Results.Conflict(new { message = "Idempotency-Key is already bound to a different replay request." });
        if (existing.Status == "Pending")
            return Results.Conflict(new { replay = existing, message = "A replay with this idempotency key is still in progress or its lease has not expired." });
        return Results.Ok(new { replay = existing, duplicate = true });
    }
    var record = new DlqReplayRecord { Id = Guid.NewGuid(), IdempotencyKey = idempotencyKey, EventId = request.EventId, SourceTopic = request.SourceTopic, TargetTopic = request.TargetTopic, ActorId = actor, CorrelationId = correlationId, CreatedAtUtc = DateTime.UtcNow, Status = "Pending", LeaseUntilUtc = DateTime.UtcNow.AddMinutes(5), AttemptCount = 1, LeaseToken = Guid.NewGuid() };
    db.DlqReplays.Add(record);
    try { await db.SaveChangesAsync(ct); }
    catch (DbUpdateException)
    {
        var winner = await db.DlqReplays.AsNoTracking().SingleAsync(x => x.IdempotencyKey == idempotencyKey, ct);
        return winner.EventId == request.EventId && winner.SourceTopic == request.SourceTopic && winner.TargetTopic == request.TargetTopic
            ? Results.Ok(new { replay = winner, duplicate = true })
            : Results.Conflict(new { message = "Idempotency-Key is already bound to a different replay request." });
    }
    (bool Found, string? Topic, long? Offset) result;
    try
    {
        result = await service.ReplayAsync(request, ct, async token =>
        {
            var renewed = await db.DlqReplays.Where(x => x.Id == record.Id && x.Status == "Pending" && x.LeaseToken == record.LeaseToken).ExecuteUpdateAsync(setters => setters.SetProperty(x => x.LeaseUntilUtc, DateTime.UtcNow.AddMinutes(5)), token);
            if (renewed != 1) throw new InvalidOperationException("Replay lease was lost before Kafka publish.");
        });
    }
    catch (Exception exception) when (!ct.IsCancellationRequested)
    {
        BuildingBlocks.Messaging.MessagingMetrics.DlqReplays.Add(1, new KeyValuePair<string, object?>("outcome", "failed"));
        record.Status = "Failed";
        record.Error = exception.Message;
        DlqAudit.Add(db, "DlqReplayFailed", record.Id, record.EventId, record.Id, request.SourceTopic, request.TargetTopic, actor, correlationId, new { error = exception.Message });
        await db.SaveChangesAsync(ct);
        throw;
    }
    record.SourceOffset = result.Offset;
    record.Status = result.Found ? "Completed" : "NotFound";
    record.CompletedAtUtc = result.Found ? DateTime.UtcNow : null;
    record.LeaseUntilUtc = null;
    await db.SaveChangesAsync(ct);
    if (!result.Found)
    {
        BuildingBlocks.Messaging.MessagingMetrics.DlqReplays.Add(1, new KeyValuePair<string, object?>("outcome", "not_found"));
        DlqAudit.Add(db, "DlqReplayNotFound", record.Id, record.EventId, record.Id, request.SourceTopic, request.TargetTopic, actor, correlationId);
        await db.SaveChangesAsync(ct);
        return Results.NotFound(new { request.EventId, request.SourceTopic, message = "Event was not found in the configured DLQ." });
    }
    BuildingBlocks.Messaging.MessagingMetrics.DlqReplays.Add(1, new KeyValuePair<string, object?>("outcome", "completed"));
    DlqAudit.Add(db, "DlqReplayCompleted", record.Id, record.EventId, record.Id, request.SourceTopic, request.TargetTopic, actor, correlationId, new { result.Offset });
    await db.SaveChangesAsync(ct);
    app.Logger.LogWarning("DLQ event {EventId} replayed by {ActorId} from {SourceTopic} to {TargetTopic} at offset {Offset}; correlation {CorrelationId}", request.EventId, actor, request.SourceTopic, request.TargetTopic, result.Offset, correlationId);
    return Results.Ok(new { request.EventId, request.SourceTopic, request.TargetTopic, result.Offset, replayedBy = actor, correlationId, idempotencyKey });
}).RequireAuthorization("dlq-operations").RequireRateLimiting("dlq-operations");
app.MapPost("/ops/dlq/replay/{id:guid}/retry", async (Guid id, OpsDbContext db, DlqManagementService service, HttpContext context, CancellationToken ct) =>
{
    var source = await db.DlqReplays.SingleOrDefaultAsync(x => x.Id == id, ct);
    if (source is null) return Results.NotFound(new { id });
    if (source.Status != "Failed") return Results.Conflict(new { message = "Only Failed replay records can be retried.", replay = source });
    var actor = context.User.FindFirst(System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub)?.Value ?? "unknown";
    var correlationId = context.Request.Headers["X-Correlation-Id"].ToString();
    var route = DlqManagementService.Routes.SingleOrDefault(x => x.SourceTopic == source.SourceTopic && x.TargetTopic == source.TargetTopic);
    if (route is null || !DlqManagementService.CanAccess(route, context.User))
    {
        DlqAudit.Add(db, "DlqReplayRetryDenied", source.Id, source.EventId, source.Id, source.SourceTopic, source.TargetTopic, actor, correlationId, new { reason = "topic_scope", sourceReplayId = source.Id });
        await db.SaveChangesAsync(ct);
        return Results.Forbid();
    }
    var key = context.Request.Headers["Idempotency-Key"].ToString();
    if (string.IsNullOrWhiteSpace(key) || key.Length > 200) return Results.BadRequest(new { message = "A new Idempotency-Key is required for retry." });
    var retry = new DlqReplayRecord { Id = Guid.NewGuid(), IdempotencyKey = key, EventId = source.EventId, SourceTopic = source.SourceTopic, TargetTopic = source.TargetTopic, ActorId = actor, CorrelationId = correlationId, CreatedAtUtc = DateTime.UtcNow, Status = "Pending", LeaseUntilUtc = DateTime.UtcNow.AddMinutes(5), AttemptCount = source.AttemptCount + 1, LeaseToken = Guid.NewGuid() };
    db.DlqReplays.Add(retry);
    try { await db.SaveChangesAsync(ct); }
    catch (DbUpdateException) { return Results.Conflict(new { message = "Idempotency-Key has already been used." }); }
    try
    {
        var result = await service.ReplayAsync(new DlqReplayRequest(retry.SourceTopic, retry.TargetTopic, retry.EventId), ct, async token =>
        {
            var renewed = await db.DlqReplays.Where(x => x.Id == retry.Id && x.Status == "Pending" && x.LeaseToken == retry.LeaseToken).ExecuteUpdateAsync(setters => setters.SetProperty(x => x.LeaseUntilUtc, DateTime.UtcNow.AddMinutes(5)), token);
            if (renewed != 1) throw new InvalidOperationException("Replay lease was lost before Kafka publish.");
        });
        retry.SourceOffset = result.Offset;
        retry.Status = result.Found ? "Completed" : "NotFound";
        retry.CompletedAtUtc = result.Found ? DateTime.UtcNow : null;
        retry.LeaseUntilUtc = null;
        DlqAudit.Add(db, result.Found ? "DlqReplayRetryCompleted" : "DlqReplayRetryNotFound", retry.Id, retry.EventId, retry.Id, retry.SourceTopic, retry.TargetTopic, actor, correlationId, new { result.Offset, sourceReplayId = source.Id });
        await db.SaveChangesAsync(ct);
        BuildingBlocks.Messaging.MessagingMetrics.DlqReplays.Add(1, new KeyValuePair<string, object?>("outcome", result.Found ? "completed" : "not_found"));
        return result.Found ? Results.Ok(new { replay = retry, retriedFrom = source.Id }) : Results.NotFound(new { replay = retry, retriedFrom = source.Id });
    }
    catch (Exception exception) when (!ct.IsCancellationRequested)
    {
        retry.Status = "Failed";
        retry.Error = exception.Message;
        retry.LeaseUntilUtc = null;
        DlqAudit.Add(db, "DlqReplayRetryFailed", retry.Id, retry.EventId, retry.Id, retry.SourceTopic, retry.TargetTopic, actor, correlationId, new { error = exception.Message, sourceReplayId = source.Id });
        await db.SaveChangesAsync(ct);
        BuildingBlocks.Messaging.MessagingMetrics.DlqReplays.Add(1, new KeyValuePair<string, object?>("outcome", "failed"));
        throw;
    }
}).RequireAuthorization("dlq-operations");
app.MapReverseProxy();

app.Run();
