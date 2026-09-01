using Microsoft.EntityFrameworkCore;
using PosCafe.Store.Domain;
using StoreEntity = PosCafe.Store.Domain.Store;
using BuildingBlocks.Observability;

namespace PosCafe.Store.Infrastructure;

public sealed class StoreDbContext(DbContextOptions<StoreDbContext> options) : DbContext(options)
{
    public DbSet<StoreEntity> Stores => Set<StoreEntity>();
    public DbSet<AuditEntry> AuditEntries => Set<AuditEntry>();
    public DbSet<StoreIdempotencyRecord> StoreIdempotencyRecords => Set<StoreIdempotencyRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StoreEntity>(entity =>
        {
            entity.ToTable("stores"); entity.HasKey(x => x.Id);
            entity.Property(x => x.Code).HasMaxLength(50).IsRequired();
            entity.Property(x => x.Name).HasMaxLength(200).IsRequired();
            entity.Property(x => x.TimeZone).HasMaxLength(100).IsRequired();
            entity.HasIndex(x => x.Code).IsUnique();
            entity.HasIndex(x => new { x.IsActive, x.Name });
        });
        modelBuilder.Entity<AuditEntry>(entity =>
        {
            entity.ToTable("audit_entries"); entity.HasKey(x => x.Id);
            entity.Property(x => x.Action).HasMaxLength(100).IsRequired(); entity.Property(x => x.EntityType).HasMaxLength(100).IsRequired(); entity.Property(x => x.EntityId).HasMaxLength(100).IsRequired(); entity.Property(x => x.CorrelationId).HasMaxLength(128).IsRequired();
            entity.HasIndex(x => new { x.StoreId, x.OccurredAtUtc });
        });
        modelBuilder.Entity<StoreIdempotencyRecord>(entity =>
        {
            entity.ToTable("store_idempotency_records"); entity.HasKey(x => x.Id);
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(64).IsRequired();
            entity.Property(x => x.Operation).HasMaxLength(100).IsRequired();
            entity.Property(x => x.ResponseJson).IsRequired();
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
        });
    }
}
