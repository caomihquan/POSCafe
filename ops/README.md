# PosCafe Operations

This directory contains production observability configuration.

## Prometheus

1. Set `Observability__Prometheus__Enabled=true` for every scrape target.
2. Create a dedicated admin JWT and write it to `prometheus/secrets/poscafe-admin-token` with filesystem mode `0600`.
3. Replace the example targets and `environment` label in `prometheus/prometheus.yml`.
4. Mount `prometheus/prometheus.yml` and `prometheus/rules/` into Prometheus.

The bearer token must not be committed. In Kubernetes, mount it from a Secret. In a VM deployment, use a secret manager and a tmpfs-mounted file. Terminate TLS at the ingress and use `scheme: https` for production targets.

## Grafana

Mount `grafana/provisioning` and `grafana/dashboards` into Grafana. The datasource and dashboard are provisioned on startup and are intentionally not editable from the UI. Configure notification contact points and routing in the Grafana environment or IaC layer.

## Alerts

The included rules cover outbox publish failures, dead-letter messages, audit archive failures, retention failures, and consumer lag above 1,000 records for 10 minutes. Consumer lag is calculated from each Kafka partition high watermark after consumption, with `service`, `topic`, and `partition` labels. A DLQ alert is critical because replay requires operator investigation; archive failures are critical because PostgreSQL purge is blocked until archival succeeds.

The Grafana dashboard includes Gateway-only audit throughput, retention failure, and purged-record volume panels. Purged volume is emitted only after a successful delete from `opsdb.audit_entries`. A Gateway retention failure is critical because audit data is retained until the archive/purge cycle is repaired.

The dashboard also exposes current DLQ replay gauges: `poscafe.dlq.pending`, `poscafe.dlq.failed`, `poscafe.dlq.not_found`, and `poscafe.dlq.completed`. These snapshots are refreshed from `opsdb` by the Gateway retention worker.

Payment, Inventory, and Reporting readiness checks include consumer lag. A stale snapshot is degraded and a lag above `Messaging__ConsumerLag__MaxLag` is unhealthy; tune the threshold with environment variables rather than changing code.
Consumer retry uses exponential backoff bounded by `RetryMaxSeconds` (default 300 seconds). Services derive `MaxPollIntervalMs` from that bound to avoid consumer-group rebalance during retry. DLQ records retain the original topic, partition, offset, final attempt, and failure reason for safe replay and incident analysis.
All Kafka producers use idempotence, `acks=all`, bounded retries, compression, and a 120-second message timeout. Topic creation is disabled in applications; provision topics, partitions, replication, and ACLs through infrastructure deployment before starting services.
Order, Payment, Inventory, and Reporting readiness also verify `Messaging:RequiredTopics` through Kafka metadata. Missing topics, insufficient partitions, or denied metadata access return unhealthy; configure `Messaging__MinimumTopicPartitions` according to the production partition plan.
Aspire dependency ordering must include `.WaitFor(kafka)` for every Kafka consumer service; the local AppHost applies this to Order, Payment, Inventory, Reporting, and Gateway. Production orchestrators should model the same broker-before-consumer dependency and gate traffic on readiness.
For production Kafka security, set `Kafka__Security__Enabled=true`, `Kafka__Security__Protocol=SaslSsl` (or `Ssl`), `Kafka__Security__SaslMechanism=ScramSha512`, `Kafka__Security__Username`, `Kafka__Security__Password`, and `Kafka__Security__CaLocation` through the secret manager/runtime environment. Client certificate fields are `CertificateLocation`, `KeyLocation`, and `KeyPassword`; keep certificate verification enabled and never commit these values.
PostgreSQL and MongoDB credentials must likewise come from secret-managed connection strings. PostgreSQL deployments should use `SSL Mode=VerifyFull;Root Certificate=/run/secrets/postgres-ca.crt`; MongoDB deployments should use `tls=true&tlsCAFile=/run/secrets/mongo-ca.pem` (and `replicaSet` where applicable). Do not use the development `postgres/postgres` or unauthenticated localhost MongoDB values outside Development.
Reporting internal writes accept `Reporting__InternalApiKeys` as a rotating key set; keep the previous and next key during rollout, then remove the old key after all callers migrate. Legacy `Reporting__InternalApiKey` remains supported for compatibility. Health endpoints can be exposed to the internal platform network with `HealthChecks__ExposeEndpoints=true`; never publish them through the public gateway.
All mutating Order, Payment, Inventory, Catalog, Store, and Identity-admin commands require a client-generated `Idempotency-Key` (maximum 200 characters). Retries with the same request return the stored result and set `Idempotency-Replayed: true`; a first execution sets it to `false`; reusing a key with a different payload returns `409 Conflict`. The key record and business change are committed atomically. Unique-key races re-read the winning record, so concurrent retries do not duplicate work.

Idempotency records are retained for the configured retry window and purged in bounded batches by each service's existing retention worker. Store defaults are `Idempotency:RetentionDays=7`, `IntervalHours=6`, and `BatchSize=1000`; other services use the same retention intent and can override their service configuration. Do not set retention shorter than the maximum client retry window.

The relevant metrics are `poscafe.idempotency.replays`, `poscafe.idempotency.conflicts`, `poscafe.idempotency.retention.purged`, and `poscafe.audit.retention.failures`. Alert on a sustained conflict spike, retention failures, and a growing idempotency table. Keep metric labels low-cardinality; never label by the raw idempotency key, user email, or request body.

## Security

Keep `/metrics` on an internal network, require the `admin` role, rotate the scrape token, and never expose Prometheus or Grafana administration ports publicly.

## Deployment Checklist

Apply EF migrations as a release step with a short-lived deployment identity before starting new application replicas. Do not let every replica race to mutate the schema in production; the application startup migration is convenient for local Aspire only. Verify the following migration families are applied for each database: Order, Payment, Inventory, Catalog, Store, Identity, and Gateway `opsdb`.

Provide connection strings, JWT signing keys, Kafka credentials/certificates, and archive credentials through the platform secret manager. Roll secrets by overlapping old and new values where supported, validate readiness after rotation, and revoke the old secret only after all replicas have reloaded configuration.

During termination, stop ingress traffic first, allow in-flight HTTP requests to finish, then give Kafka publishers time to flush. Keep broker and database readiness checks separate from liveness checks so a dependency outage does not cause a restart storm.
