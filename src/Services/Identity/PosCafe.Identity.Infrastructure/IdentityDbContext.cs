using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;

namespace PosCafe.Identity.Infrastructure;

public sealed class IdentityDbContext(DbContextOptions<IdentityDbContext> options)
    : IdentityDbContext<IdentityUser, IdentityRole<Guid>, Guid>(options)
{
    public DbSet<RefreshToken> RefreshTokens => Set<RefreshToken>();
    public DbSet<UserStoreAssignment> UserStoreAssignments => Set<UserStoreAssignment>();
    public DbSet<IdentityIdempotencyRecord> IdentityIdempotencyRecords => Set<IdentityIdempotencyRecord>();

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);
        builder.Entity<IdentityUser>().Property(x => x.DisplayName).HasMaxLength(200);
        builder.Entity<RefreshToken>().HasIndex(x => x.TokenHash).IsUnique();
        builder.Entity<RefreshToken>().HasIndex(x => new { x.UserId, x.ExpiresAtUtc });
        builder.Entity<UserStoreAssignment>().HasKey(x => new { x.UserId, x.StoreId });
        builder.Entity<UserStoreAssignment>().HasIndex(x => new { x.StoreId, x.IsActive });
        builder.Entity<IdentityIdempotencyRecord>(entity =>
        {
            entity.ToTable("identity_idempotency_records"); entity.HasKey(x => x.Id);
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(64).IsRequired();
            entity.Property(x => x.Operation).HasMaxLength(100).IsRequired();
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
        });
    }
}
