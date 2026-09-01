using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Extensions.Hosting;

public static class AuthenticationExtensions
{
    public static IHostApplicationBuilder AddPosCafeAuthentication(this IHostApplicationBuilder builder)
    {
        var key = builder.Configuration["Jwt:Key"];
        if (string.IsNullOrWhiteSpace(key) && builder.Environment.IsDevelopment()) key = "development-only-key-must-be-at-least-32-characters";
        if (string.IsNullOrWhiteSpace(key) || Encoding.UTF8.GetByteCount(key) < 32)
            throw new InvalidOperationException("Jwt:Key must be configured with at least 32 bytes.");

        builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options => options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(key)),
                ValidateIssuer = false,
                ValidateAudience = false,
                ValidateLifetime = true,
                ClockSkew = TimeSpan.FromSeconds(30),
                NameClaimType = System.IdentityModel.Tokens.Jwt.JwtRegisteredClaimNames.Sub
            });
        builder.Services.AddAuthorization(options =>
        {
            options.AddPolicy("catalog-manager", policy => policy.RequireRole("admin", "catalog-manager"));
            options.AddPolicy("store-manager", policy => policy.RequireRole("admin", "store-manager"));
            options.AddPolicy("order-operator", policy => policy.RequireRole("admin", "manager", "cashier"));
            options.AddPolicy("payment-operator", policy => policy.RequireRole("admin", "manager", "cashier"));
        options.AddPolicy("inventory-manager", policy => policy.RequireRole("admin", "store-manager", "inventory-manager"));
        options.AddPolicy("operations", policy => policy.RequireRole("admin"));
        options.AddPolicy("dlq-operations", policy => policy.RequireRole("admin", "manager", "order-operator", "payment-operator", "store-manager", "inventory-manager"));
        });
        return builder;
    }
}
