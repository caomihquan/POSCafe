using Microsoft.EntityFrameworkCore;
using BuildingBlocks.Observability;

namespace PosCafe.ApiGateway;

public sealed class DlqReplayRecord
{
    public Guid Id { get; set; }
    public string IdempotencyKey { get; set; } = string.Empty;
    public Guid EventId { get; set; }
    public string SourceTopic { get; set; } = string.Empty;
    public string TargetTopic { get; set; } = string.Empty;
    public string Status { get; set; } = "Pending";
    public string ActorId { get; set; } = "unknown";
    public string? CorrelationId { get; set; }
    public long? SourceOffset { get; set; }
    public string? Error { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? CompletedAtUtc { get; set; }
    public DateTime? LeaseUntilUtc { get; set; }
    public int AttemptCount { get; set; }
    public Guid LeaseToken { get; set; }
}

public sealed class OpsDbContext(DbContextOptions<OpsDbContext> options) : DbContext(options)
{
    public DbSet<DlqReplayRecord> DlqReplays => Set<DlqReplayRecord>();
    public DbSet<AuditEntry> AuditEntries => Set<AuditEntry>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DlqReplayRecord>(entity =>
        {
            entity.ToTable("dlq_replay_records");
            entity.HasKey(x => x.Id);
            entity.HasIndex(x => x.IdempotencyKey).IsUnique();
            entity.HasIndex(x => new { x.CreatedAtUtc, x.Status });
            entity.Property(x => x.IdempotencyKey).HasMaxLength(200).IsRequired();
            entity.Property(x => x.SourceTopic).HasMaxLength(250).IsRequired();
            entity.Property(x => x.TargetTopic).HasMaxLength(250).IsRequired();
            entity.Property(x => x.Status).HasMaxLength(30).IsRequired();
            entity.Property(x => x.ActorId).HasMaxLength(200).IsRequired();
            entity.Property(x => x.CorrelationId).HasMaxLength(128);
            entity.Property(x => x.Error).HasMaxLength(2000);
            entity.HasIndex(x => new { x.Status, x.LeaseUntilUtc });
        });
        modelBuilder.Entity<AuditEntry>(entity =>
        {
            entity.ToTable("audit_entries");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Action).HasMaxLength(100).IsRequired();
            entity.Property(x => x.EntityType).HasMaxLength(100).IsRequired();
            entity.Property(x => x.EntityId).HasMaxLength(100).IsRequired();
            entity.Property(x => x.CorrelationId).HasMaxLength(128).IsRequired();
            entity.HasIndex(x => new { x.EntityType, x.EntityId, x.OccurredAtUtc });
        });
    }
}
