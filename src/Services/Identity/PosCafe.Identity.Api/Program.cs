using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using PosCafe.Identity.Infrastructure;
using BuildingBlocks.Messaging;
using BuildingBlocks.Exceptions;
using IdentityUserEntity = PosCafe.Identity.Infrastructure.IdentityUser;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.AddNpgsqlDbContext<IdentityDbContext>("identitydb");
builder.Services.AddIdentityCore<IdentityUserEntity>(options =>
{
    options.User.RequireUniqueEmail = true;
    options.Password.RequiredLength = 8;
})
.AddRoles<IdentityRole<Guid>>()
.AddEntityFrameworkStores<IdentityDbContext>()
.AddSignInManager();

var jwtKey = builder.Configuration["Jwt:Key"];
if (string.IsNullOrWhiteSpace(jwtKey) && builder.Environment.IsDevelopment()) jwtKey = "development-only-key-must-be-at-least-32-characters";
if (string.IsNullOrWhiteSpace(jwtKey) || Encoding.UTF8.GetByteCount(jwtKey) < 32) throw new InvalidOperationException("Jwt:Key must be configured with at least 32 bytes.");
builder.Services.AddAuthentication(options =>
{
    options.DefaultAuthenticateScheme = "Bearer";
    options.DefaultChallengeScheme = "Bearer";
}).AddJwtBearer("Bearer", options => options.TokenValidationParameters = new TokenValidationParameters
{
    ValidateIssuerSigningKey = true,
    IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(jwtKey)),
    ValidateIssuer = false,
    ValidateAudience = false,
    ValidateLifetime = true,
    NameClaimType = JwtRegisteredClaimNames.Sub
});
builder.Services.AddAuthorization();
builder.Services.AddScoped<JwtTokenService>();
builder.Services.Configure<RefreshTokenCleanupOptions>(builder.Configuration.GetSection("Security:RefreshTokenCleanup"));
builder.Services.AddHostedService<RefreshTokenCleanupService>();
var app = builder.Build();
app.UsePosCafeExceptionHandler();
app.UseAuthentication();
app.UseAuthorization();
app.MapDefaultEndpoints();

using (var scope = app.Services.CreateScope())
{
    if (app.Environment.IsDevelopment())
        await scope.ServiceProvider.GetRequiredService<IdentityDbContext>().Database.MigrateAsync();
    var roles = new[] { "admin", "manager", "cashier", "catalog-manager", "store-manager", "inventory-manager" };
    var roleManager = scope.ServiceProvider.GetRequiredService<RoleManager<IdentityRole<Guid>>>();
    foreach (var role in roles)
        if (!await roleManager.RoleExistsAsync(role)) await roleManager.CreateAsync(new IdentityRole<Guid>(role));

    var bootstrapEmail = builder.Configuration["Identity:Bootstrap:AdminEmail"];
    var bootstrapPassword = builder.Configuration["Identity:Bootstrap:AdminPassword"];
    if (!string.IsNullOrWhiteSpace(bootstrapEmail) && !string.IsNullOrWhiteSpace(bootstrapPassword))
    {
        var userManager = scope.ServiceProvider.GetRequiredService<UserManager<IdentityUserEntity>>();
        var admin = await userManager.FindByEmailAsync(bootstrapEmail);
        if (admin is null)
        {
            admin = new IdentityUserEntity { Id = Guid.NewGuid(), UserName = bootstrapEmail, Email = bootstrapEmail, DisplayName = "Bootstrap Administrator" };
            var created = await userManager.CreateAsync(admin, bootstrapPassword);
            if (!created.Succeeded) throw new InvalidOperationException($"Bootstrap admin creation failed: {string.Join(';', created.Errors.Select(x => x.Description))}");
        }
        if (!await userManager.IsInRoleAsync(admin, "admin")) await userManager.AddToRoleAsync(admin, "admin");
    }
}

app.MapPost("/identity/register", async (RegisterRequest request, UserManager<IdentityUserEntity> users, JwtTokenService tokens, IdentityDbContext db) =>
{
    var user = new IdentityUserEntity { Id = Guid.NewGuid(), UserName = request.Email, Email = request.Email, DisplayName = request.DisplayName };
    var result = await users.CreateAsync(user, request.Password);
    if (!result.Succeeded) return Results.ValidationProblem(result.Errors.GroupBy(x => x.Code).ToDictionary(x => x.Key, x => x.Select(e => e.Description).ToArray()));
    var pair = await tokens.CreateAsync(user, db);
    db.RefreshTokens.Add(new RefreshToken { UserId = user.Id, TokenHash = JwtTokenService.Hash(pair.RefreshToken), ExpiresAtUtc = DateTime.UtcNow.AddDays(30) });
    await db.SaveChangesAsync();
    return Results.Ok(new TokenResponse(pair.AccessToken, pair.RefreshToken));
});

app.MapPost("/identity/login", async (LoginRequest request, UserManager<IdentityUserEntity> users, SignInManager<IdentityUserEntity> signIn, JwtTokenService tokens, IdentityDbContext db) =>
{
    var user = await users.FindByEmailAsync(request.Email);
    if (user is null || !await signIn.CheckPasswordSignInAsync(user, request.Password, true).ContinueWith(x => x.Result.Succeeded)) return Results.Unauthorized();
    var pair = await tokens.CreateAsync(user, db);
    db.RefreshTokens.Add(new RefreshToken { UserId = user.Id, TokenHash = JwtTokenService.Hash(pair.RefreshToken), ExpiresAtUtc = DateTime.UtcNow.AddDays(30) });
    await db.SaveChangesAsync();
    return Results.Ok(new TokenResponse(pair.AccessToken, pair.RefreshToken));
});

app.MapPost("/identity/refresh", async (RefreshRequest request, UserManager<IdentityUserEntity> users, JwtTokenService tokens, IdentityDbContext db) =>
{
    var hash = JwtTokenService.Hash(request.RefreshToken);
    var stored = await db.RefreshTokens.AsNoTracking().SingleOrDefaultAsync(x => x.TokenHash == hash);
    if (stored is null) return Results.Unauthorized();
    var revokedAt = DateTime.UtcNow;
    var claimed = await db.RefreshTokens.Where(x => x.Id == stored.Id && x.RevokedAtUtc == null && x.ExpiresAtUtc > revokedAt).ExecuteUpdateAsync(setters => setters.SetProperty(x => x.RevokedAtUtc, revokedAt));
    if (claimed != 1) return Results.Unauthorized();
    var user = await users.FindByIdAsync(stored.UserId.ToString());
    if (user is null) return Results.Unauthorized();
    var pair = await tokens.CreateAsync(user, db);
    var replacementHash = JwtTokenService.Hash(pair.RefreshToken);
    await db.RefreshTokens.Where(x => x.Id == stored.Id).ExecuteUpdateAsync(setters => setters.SetProperty(x => x.ReplacedByTokenHash, replacementHash));
    db.RefreshTokens.Add(new RefreshToken { UserId = user.Id, TokenHash = replacementHash, ExpiresAtUtc = DateTime.UtcNow.AddDays(30) });
    await db.SaveChangesAsync();
    return Results.Ok(new TokenResponse(pair.AccessToken, pair.RefreshToken));
});

app.MapPost("/identity/revoke-all", async (ClaimsPrincipal principal, IdentityDbContext db) =>
{
    var userIdValue = principal.FindFirstValue(JwtRegisteredClaimNames.Sub) ?? principal.FindFirstValue("sub");
    if (!Guid.TryParse(userIdValue, out var userId)) return Results.Unauthorized();
    await db.RefreshTokens.Where(x => x.UserId == userId && x.RevokedAtUtc == null).ExecuteUpdateAsync(setters => setters.SetProperty(x => x.RevokedAtUtc, DateTime.UtcNow));
    return Results.NoContent();
}).RequireAuthorization();

app.MapPost("/identity/logout", async (RefreshRequest request, IdentityDbContext db) =>
{
    var token = await db.RefreshTokens.SingleOrDefaultAsync(x => x.TokenHash == JwtTokenService.Hash(request.RefreshToken));
    if (token is not null) { token.RevokedAtUtc = DateTime.UtcNow; await db.SaveChangesAsync(); }
    return Results.NoContent();
});

app.MapGet("/identity/me", (System.Security.Claims.ClaimsPrincipal principal) => Results.Ok(new { UserId = principal.FindFirst(JwtRegisteredClaimNames.Sub)?.Value ?? principal.FindFirst("sub")?.Value, principal.Identity?.Name })).RequireAuthorization();

app.MapPost("/identity/admin/users/{userId:guid}/stores", async (Guid userId, StoreAssignmentRequest request, UserManager<IdentityUserEntity> users, IdentityDbContext db, ClaimsPrincipal principal, ILoggerFactory loggerFactory, HttpRequest http, CancellationToken ct) =>
{
    if (!principal.IsInRole("admin")) return Results.Forbid();
    var key = Idempotency.ValidateKey(http.Headers["Idempotency-Key"].ToString());
    var hash = Idempotency.Hash(new { userId, request.StoreId, request.IsActive });
    var existing = await db.IdentityIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct);
    if (existing is not null) { if (!Idempotency.Matches(existing.RequestHash, hash) || existing.Operation != "identity.store-assignment") throw new ConflictException("Idempotency-Key is already bound to a different identity request."); http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.NoContent(); }
    if (await users.FindByIdAsync(userId.ToString()) is null) return Results.NotFound();
    var assignment = await db.UserStoreAssignments.SingleOrDefaultAsync(x => x.UserId == userId && x.StoreId == request.StoreId, ct);
    if (assignment is null) db.UserStoreAssignments.Add(new UserStoreAssignment { UserId = userId, StoreId = request.StoreId, AssignedAtUtc = DateTime.UtcNow, IsActive = true });
    else assignment.IsActive = request.IsActive;
    db.IdentityIdempotencyRecords.Add(new IdentityIdempotencyRecord { Id = Guid.NewGuid(), IdempotencyKey = key, RequestHash = hash, Operation = "identity.store-assignment", CreatedAtUtc = DateTime.UtcNow });
    try { await db.SaveChangesAsync(ct); }
    catch (DbUpdateException) { db.ChangeTracker.Clear(); var winner = await db.IdentityIdempotencyRecords.AsNoTracking().SingleOrDefaultAsync(x => x.IdempotencyKey == key, ct); if (winner is not null && Idempotency.Matches(winner.RequestHash, hash) && winner.Operation == "identity.store-assignment") { http.HttpContext.Response.Headers["Idempotency-Replayed"] = "true"; return Results.NoContent(); } throw; }
    loggerFactory.CreateLogger("IdentityAudit").LogInformation("Admin {AdminId} changed store assignment for user {UserId}, store {StoreId}, active {Active}", principal.FindFirstValue(JwtRegisteredClaimNames.Sub), userId, request.StoreId, request.IsActive);
    http.HttpContext.Response.Headers["Idempotency-Replayed"] = "false";
    return Results.NoContent();
}).RequireAuthorization();

app.MapGet("/", () => "Hello World!");

app.Run();

record RegisterRequest(string Email, string Password, string DisplayName);
record LoginRequest(string Email, string Password);
record RefreshRequest(string RefreshToken);
record TokenResponse(string AccessToken, string RefreshToken);
record StoreAssignmentRequest(Guid StoreId, bool IsActive = true);
