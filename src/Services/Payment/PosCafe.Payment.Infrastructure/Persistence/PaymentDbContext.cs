using BuildingBlocks.Messaging;
using BuildingBlocks.Observability;
using Microsoft.EntityFrameworkCore;
using PosCafe.Payment.Domain;
using PaymentAggregate = PosCafe.Payment.Domain.Payment;

namespace PosCafe.Payment.Infrastructure.Persistence;

public sealed class PaymentDbContext(DbContextOptions<PaymentDbContext> options) : DbContext(options)
{
    public DbSet<PaymentAggregate> Payments => Set<PaymentAggregate>();
    public DbSet<PaymentOrderProjection> OrderProjections => Set<PaymentOrderProjection>();
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();
    public DbSet<InboxMessage> InboxMessages => Set<InboxMessage>();
    public DbSet<AuditEntry> AuditEntries => Set<AuditEntry>();
    public DbSet<PaymentIdempotencyRecord> PaymentIdempotencyRecords => Set<PaymentIdempotencyRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PaymentAggregate>(entity =>
        {
            entity.ToTable("payments");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Method).HasMaxLength(40).IsRequired();
            entity.Property(x => x.Amount).HasPrecision(18, 2);
            entity.Property(x => x.Status).HasConversion<string>().HasMaxLength(30).IsRequired();
            entity.Property(x => x.Version).IsConcurrencyToken();
        });
        modelBuilder.Entity<PaymentOrderProjection>(entity =>
        {
            entity.ToTable("payment_order_projections");
            entity.HasKey(x => x.OrderId);
            entity.Property(x => x.Total).HasPrecision(18, 2);
            entity.Property(x => x.Status).HasMaxLength(30).IsRequired();
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
        modelBuilder.Entity<PaymentIdempotencyRecord>(entity =>
        {
            entity.ToTable("payment_idempotency_records");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.RequestHash).HasMaxLength(128).IsRequired();
            entity.Property(x => x.Status).HasMaxLength(30).IsRequired();
            entity.Property(x => x.Amount).HasPrecision(18, 2);
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
        });
    }
}
