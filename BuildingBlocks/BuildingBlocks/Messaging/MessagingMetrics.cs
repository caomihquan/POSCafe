using System.Diagnostics.Metrics;

namespace BuildingBlocks.Messaging;

public static class MessagingMetrics
{
    private static long pendingDlqReplays;
    private static long failedDlqReplays;
    private static long notFoundDlqReplays;
    private static long completedDlqReplays;
    public static readonly Meter Meter = new("PosCafe.Messaging", "1.0.0");
    public static readonly Counter<long> Published = Meter.CreateCounter<long>("poscafe.outbox.published", "messages", "Outbox messages published to Kafka.");
    public static readonly Counter<long> PublishFailures = Meter.CreateCounter<long>("poscafe.outbox.publish_failures", "messages", "Outbox publish failures.");
    public static readonly Counter<long> DeadLettered = Meter.CreateCounter<long>("poscafe.outbox.dead_lettered", "messages", "Outbox messages moved to dead-letter state.");
    public static readonly Counter<long> DuplicateEvents = Meter.CreateCounter<long>("poscafe.inbox.duplicates", "messages", "Duplicate events skipped by Inbox idempotency.");
    public static readonly Counter<long> Consumed = Meter.CreateCounter<long>("poscafe.inbox.consumed", "messages", "Kafka events consumed successfully.");
    public static readonly Counter<long> ArchiveSucceeded = Meter.CreateCounter<long>("poscafe.audit.archive.succeeded", "batches", "Audit archive batches uploaded successfully.");
    public static readonly Counter<long> ArchiveFailures = Meter.CreateCounter<long>("poscafe.audit.archive.failures", "batches", "Audit archive batches that failed to upload.");
    public static readonly Counter<long> ArchivedRecords = Meter.CreateCounter<long>("poscafe.audit.archive.records", "records", "Audit records uploaded to archive storage.");
    public static readonly Counter<long> RetentionFailures = Meter.CreateCounter<long>("poscafe.audit.retention.failures", "failures", "Audit retention cycles that failed.");
    public static readonly Histogram<long> ConsumerLag = Meter.CreateHistogram<long>("poscafe.kafka.consumer.lag", "messages", "Kafka records behind the high watermark for a consumed partition.");
    public static readonly Counter<long> DlqReplays = Meter.CreateCounter<long>("poscafe.dlq.replays", "replays", "DLQ replay requests by outcome.");
    public static readonly Counter<long> DlqReplayRetentionFailures = Meter.CreateCounter<long>("poscafe.dlq.retention.failures", "failures", "DLQ replay history retention failures.");
    public static readonly Counter<long> AuditPurgedRecords = Meter.CreateCounter<long>("poscafe.audit.retention.purged", "records", "Audit records purged after successful retention processing.");
    public static readonly Counter<long> IdempotencyReplays = Meter.CreateCounter<long>("poscafe.idempotency.replays", "requests", "Requests served from an idempotency record.");
    public static readonly Counter<long> IdempotencyConflicts = Meter.CreateCounter<long>("poscafe.idempotency.conflicts", "requests", "Requests rejected because an idempotency key was reused with a different payload.");
    public static readonly Counter<long> IdempotencyPurgedRecords = Meter.CreateCounter<long>("poscafe.idempotency.retention.purged", "records", "Expired idempotency records deleted.");
    public static readonly ObservableGauge<long> DlqPending = Meter.CreateObservableGauge("poscafe.dlq.pending", () => Interlocked.Read(ref pendingDlqReplays), "replays", "Current pending DLQ replay records.");
    public static readonly ObservableGauge<long> DlqFailed = Meter.CreateObservableGauge("poscafe.dlq.failed", () => Interlocked.Read(ref failedDlqReplays), "replays", "Current failed DLQ replay records.");
    public static readonly ObservableGauge<long> DlqNotFound = Meter.CreateObservableGauge("poscafe.dlq.not_found", () => Interlocked.Read(ref notFoundDlqReplays), "replays", "Current DLQ replay records where the event was not found.");
    public static readonly ObservableGauge<long> DlqCompleted = Meter.CreateObservableGauge("poscafe.dlq.completed", () => Interlocked.Read(ref completedDlqReplays), "replays", "Current completed DLQ replay records.");
    public static void UpdateDlqState(long pending, long failed, long notFound, long completed)
    {
        Interlocked.Exchange(ref pendingDlqReplays, pending);
        Interlocked.Exchange(ref failedDlqReplays, failed);
        Interlocked.Exchange(ref notFoundDlqReplays, notFound);
        Interlocked.Exchange(ref completedDlqReplays, completed);
    }
}
