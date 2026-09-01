using Microsoft.EntityFrameworkCore;
using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using PosCafe.Inventory.Domain;

namespace PosCafe.Inventory.Infrastructure;

public sealed class InventoryDbContext(DbContextOptions<InventoryDbContext> options) : DbContext(options)
{
    public DbSet<StockItem> StockItems => Set<StockItem>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    public DbSet<AuditEntry> AuditEntries => Set<AuditEntry>();
    public DbSet<InventoryIdempotencyRecord> InventoryIdempotencyRecords => Set<InventoryIdempotencyRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<StockItem>(entity =>
        {
            entity.ToTable("stock_items"); entity.HasKey(x => x.Id);
            entity.Property(x => x.Quantity).HasPrecision(18, 3);
            entity.Property(x => x.ReservedQuantity).HasPrecision(18, 3);
            entity.Property(x => x.Version).IsConcurrencyToken();
            entity.HasIndex(x => new { x.StoreId, x.ProductId }).IsUnique();
        });
        modelBuilder.Entity<InboxMessage>(entity =>
        {
            entity.ToTable("inbox_messages");
            entity.HasKey(x => new { x.EventId, x.Consumer });
            entity.Property(x => x.Consumer).HasMaxLength(200).IsRequired();
            entity.HasIndex(x => new { x.ProcessedOnUtc, x.LastAttemptOnUtc });
        });
        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("outbox_messages");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.EventType).HasMaxLength(250).IsRequired();
            entity.Property(x => x.AggregateId).HasMaxLength(100).IsRequired();
            entity.Property(x => x.Payload).IsRequired();
            entity.HasIndex(x => new { x.ProcessedOnUtc, x.DeadLetteredOnUtc, x.LockedUntilUtc, x.OccurredOnUtc });
        });
        modelBuilder.Entity<AuditEntry>(entity =>
        {
            entity.ToTable("audit_entries"); entity.HasKey(x => x.Id);
            entity.Property(x => x.Action).HasMaxLength(100).IsRequired();
            entity.Property(x => x.EntityType).HasMaxLength(100).IsRequired();
            entity.Property(x => x.EntityId).HasMaxLength(100).IsRequired();
            entity.Property(x => x.CorrelationId).HasMaxLength(128).IsRequired();
            entity.HasIndex(x => new { x.StoreId, x.OccurredAtUtc });
        });
        modelBuilder.Entity<InventoryIdempotencyRecord>(entity =>
        {
            entity.ToTable("inventory_idempotency_records");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(128).IsRequired();
            entity.Property(x => x.ResponseJson).IsRequired();
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
        });
    }
}
