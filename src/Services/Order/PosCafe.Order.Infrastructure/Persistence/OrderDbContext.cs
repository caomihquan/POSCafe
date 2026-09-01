using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Microsoft.EntityFrameworkCore;
using PosCafe.Order.Domain;
using OrderAggregate = PosCafe.Order.Domain.Order;

namespace PosCafe.Order.Infrastructure.Persistence;

public sealed class OrderDbContext(DbContextOptions<OrderDbContext> options) : DbContext(options)
{
    public DbSet<OrderAggregate> Orders => Set<OrderAggregate>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    public DbSet<AuditEntry> AuditEntries => Set<AuditEntry>();
    public DbSet<OrderIdempotencyRecord> OrderIdempotencyRecords => Set<OrderIdempotencyRecord>();
    public DbSet<OrderFulfillmentSaga> OrderFulfillmentSagas => Set<OrderFulfillmentSaga>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OrderAggregate>(entity =>
        {
            entity.ToTable("orders");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Channel).HasMaxLength(30).IsRequired();
            entity.Property(x => x.Status).HasConversion<string>().HasMaxLength(40).IsRequired();
            entity.Property(x => x.Version).IsConcurrencyToken();
            entity.HasMany(x => x.Lines).WithOne().HasForeignKey("OrderId").IsRequired();
        });
        modelBuilder.Entity<OrderLine>(entity =>
        {
            entity.ToTable("order_lines");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.ProductName).HasMaxLength(200).IsRequired();
            entity.Property(x => x.UnitPrice).HasPrecision(18, 2);
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
        modelBuilder.Entity<InboxMessage>(entity =>
        {
            entity.ToTable("inbox_messages");
            entity.HasKey(x => new { x.EventId, x.Consumer });
            entity.Property(x => x.Consumer).HasMaxLength(200);
            entity.HasIndex(x => new { x.ProcessedOnUtc, x.LastAttemptOnUtc });
        });
        modelBuilder.Entity<AuditEntry>(entity =>
        {
            entity.ToTable("audit_entries"); entity.HasKey(x => x.Id);
            entity.Property(x => x.Action).HasMaxLength(100).IsRequired();
            entity.Property(x => x.EntityType).HasMaxLength(100).IsRequired();
            entity.Property(x => x.EntityId).HasMaxLength(100).IsRequired();
            entity.Property(x => x.CorrelationId).HasMaxLength(128).IsRequired();
            entity.HasIndex(x => new { x.StoreId, x.OccurredAtUtc });
            entity.HasIndex(x => new { x.EntityType, x.EntityId, x.OccurredAtUtc });
        });
        modelBuilder.Entity<OrderIdempotencyRecord>(entity =>
        {
            entity.ToTable("order_idempotency_records");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(128).IsRequired();
            entity.Property(x => x.Status).HasMaxLength(40).IsRequired();
            entity.Property(x => x.Subtotal).HasPrecision(18, 2);
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
        });
        modelBuilder.Entity<OrderFulfillmentSaga>(entity =>
        {
            entity.ToTable("order_fulfillment_sagas");
            entity.HasKey(x => x.SagaId);
            entity.Property(x => x.PaymentMethod).HasMaxLength(40).IsRequired();
            entity.Property(x => x.Status).HasMaxLength(40).IsRequired();
            entity.Property(x => x.Total).HasPrecision(18, 2);
            entity.Property(x => x.LastError).HasMaxLength(1000);
            entity.HasIndex(x => x.OrderId).IsUnique();
            entity.HasIndex(x => new { x.Status, x.UpdatedAtUtc });
        });
    }
}
