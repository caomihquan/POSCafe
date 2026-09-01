using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;

namespace PosCafe.Identity.Infrastructure;

public sealed class JwtTokenService(IConfiguration configuration, UserManager<IdentityUser> userManager)
{
    public async Task<(string AccessToken, string RefreshToken)> CreateAsync(IdentityUser user, IdentityDbContext db)
    {
        var key = configuration["Jwt:Key"] ?? throw new InvalidOperationException("Jwt:Key is not configured.");
        var roles = await userManager.GetRolesAsync(user);
        var claims = new List<Claim> { new(JwtRegisteredClaimNames.Sub, user.Id.ToString()), new(ClaimTypes.Name, user.UserName!) };
        claims.AddRange(roles.Select(role => new Claim(ClaimTypes.Role, role)));
        var storeIds = await db.UserStoreAssignments.Where(x => x.UserId == user.Id && x.IsActive).Select(x => x.StoreId).ToListAsync();
        claims.AddRange(storeIds.Select(storeId => new Claim("store_id", storeId.ToString())));
        var credentials = new SigningCredentials(new SymmetricSecurityKey(Encoding.UTF8.GetBytes(key)), SecurityAlgorithms.HmacSha256);
        var token = new JwtSecurityToken(claims: claims, expires: DateTime.UtcNow.AddMinutes(15), signingCredentials: credentials);
        return (new JwtSecurityTokenHandler().WriteToken(token), CreateOpaqueToken());
    }

    public static string Hash(string token) => Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(token)));

    private static string CreateOpaqueToken() => Convert.ToBase64String(RandomNumberGenerator.GetBytes(64));
}
