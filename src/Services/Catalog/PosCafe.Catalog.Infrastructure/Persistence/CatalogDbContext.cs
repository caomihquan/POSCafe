using Microsoft.EntityFrameworkCore;
using PosCafe.Catalog.Domain.Entities;
using BuildingBlocks.Observability;

namespace PosCafe.Catalog.Infrastructure.Persistence;

public class CatalogDbContext : DbContext
{
    public CatalogDbContext(
        DbContextOptions<CatalogDbContext> options)
        : base(options)
    {
    }

    public DbSet<Product> Products => Set<Product>();

    public DbSet<Category> Categories => Set<Category>();
    public DbSet<AuditEntry> AuditEntries => Set<AuditEntry>();
    public DbSet<CatalogIdempotencyRecord> CatalogIdempotencyRecords => Set<CatalogIdempotencyRecord>();

    protected override void OnModelCreating(
        ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Category>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.Name)
                .HasMaxLength(200)
                .IsRequired();
        });

        modelBuilder.Entity<Product>(entity =>
        {
            entity.HasKey(x => x.Id);

            entity.Property(x => x.Name)
                .HasMaxLength(200)
                .IsRequired();

            entity.Property(x => x.Price)
                .HasPrecision(18, 2);

            entity.Property(x => x.CategoryId)
                .IsRequired();
        });
        modelBuilder.Entity<AuditEntry>(entity =>
        {
            entity.ToTable("audit_entries"); entity.HasKey(x => x.Id);
            entity.Property(x => x.Action).HasMaxLength(100).IsRequired(); entity.Property(x => x.EntityType).HasMaxLength(100).IsRequired(); entity.Property(x => x.EntityId).HasMaxLength(100).IsRequired(); entity.Property(x => x.CorrelationId).HasMaxLength(128).IsRequired();
            entity.HasIndex(x => x.OccurredAtUtc);
        });
        modelBuilder.Entity<CatalogIdempotencyRecord>(entity =>
        {
            entity.ToTable("catalog_idempotency_records"); entity.HasKey(x => x.Id);
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(128).IsRequired();
            entity.Property(x => x.ResourceType).HasMaxLength(40).IsRequired();
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
        });
    }
}
