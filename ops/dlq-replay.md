# DLQ Replay Runbook

DLQ replay is an administrative operation. Obtain an admin JWT, confirm the incident is resolved, and replay one event at a time before starting a larger recovery.

Consumers route malformed or unsupported messages to the configured DLQ instead of silently committing them. These records carry `poison-message=true`, the failure reason, and the original topic/partition/offset. When the source has no valid `event-id`, the consumer creates a deterministic synthetic ID from that location so the record remains addressable for investigation and replay.
Published order events use `schema-version=1`; consumers reject missing or unsupported versions before opening an Inbox transaction. A schema change must publish a new version and retain the old consumer path until all producers and replay traffic have migrated.

List allowed routes:

```http
GET /ops/dlq/routes
Authorization: Bearer <admin-token>
```

Read replay history and aggregates:

```http
GET /ops/dlq/history?page=1&pageSize=50
GET /ops/dlq/summary?from=2026-08-30T00:00:00Z&to=2026-08-31T00:00:00Z
Authorization: Bearer <admin-token>
```

The summary is grouped by source topic, target topic, and status. Queries are limited to a 31-day window. Replay history is retained for 90 days by default and purged in batches by the Gateway worker; configure `Dlq__ReplayHistory__RetentionDays`, `Dlq__ReplayHistory__IntervalHours`, and `Dlq__ReplayHistory__BatchSize`. The Gateway applies the versioned `InitialOps` EF migration at startup. For controlled production rollout, run the migration as a deployment job using the Gateway project before enabling new instances.

Replay an event:

```http
POST /ops/dlq/replay
Authorization: Bearer <admin-token>
Idempotency-Key: incident-2026-08-31-payment-001
Content-Type: application/json

{"sourceTopic":"pos.payment.order-events.dlq","targetTopic":"pos.order.events","eventId":"<event-id>"}
```

The API claims the idempotency key before searching Kafka and assigns a five-minute lease with a fencing token. Long scans renew the lease every minute and immediately before publish; the conditional token check prevents an old Gateway instance from publishing after losing ownership. It searches the configured DLQ, copies the original event headers, adds replay metadata, publishes with `acks=all` and an idempotent producer, and commits the source offset only after publish succeeds. The original `event-id` is preserved so consumer Inbox idempotency prevents duplicate business effects. Every replay is logged with actor, correlation ID, source/target topic, and source offset. If the Gateway crashes, the retention worker marks an expired `Pending` lease as `Failed`; operators must investigate and submit a new idempotency key rather than blindly replaying.

Retry a failed record while preserving the original history:

```http
POST /ops/dlq/replay/<history-id>/retry
Authorization: Bearer <admin-token>
Idempotency-Key: incident-2026-08-31-payment-002
```

Only `Failed` records can be retried, and each retry creates a new immutable history record linked by `retriedFrom` in the response. Every replay and retry outcome is also written to the `audit_entries` table in `opsdb`; the audit migration is `AddDlqAuditEntries`. Gateway audit entries follow the same retention policy as other services: `Audit__RetentionDays` defaults to 365 days, and `Audit__Archive__Enabled=true` archives JSONL to Azure Blob before deletion. Enable immutable retention/WORM and lifecycle policy on the storage account; keep the connection string in a secret manager.

Do not expose these routes to cashier roles. Scope is enforced by service: `order` uses `admin`, `manager`, `order-operator`; `payment` uses `admin`, `manager`, `payment-operator`; `inventory` uses `admin`, `store-manager`, `inventory-manager`; `reporting` uses `admin`, `manager`. History and summary are filtered by the same scope. DLQ endpoints are rate-limited per client IP by `Security__RateLimit__DlqPermitLimit` and `Security__RateLimit__DlqWindowSeconds` (defaults 30 requests/60 seconds). Use the existing `tools/PosCafe.DlqReplay` command only for emergency recovery when the Gateway is unavailable.

Gateway readiness is available at `GET /ops/health` for `admin` and checks `opsdb`, Kafka, audit archive configuration, stale `Pending` replay claims, and audit retention freshness. It returns HTTP 503 when a ready check is unhealthy; configure `Dlq__ReplayHistory__PendingMaxAgeMinutes` and `Audit__RetentionMaxAgeDays`, keep it behind an internal ingress, and do not use it as an unauthenticated public probe.
