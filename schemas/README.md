# Event Schema Registry

This directory is the version-controlled source of truth for POS Cafe integration-event contracts. The deployed Kafka registry, when enabled, must use the same `schemaId` and compatibility mode declared here.

## Registration

Register each schema before enabling a producer rollout. CI or the deployment pipeline should reject changes that break the declared compatibility mode. Producers publish `schema-version` and `schema-id` headers; consumers reject missing or unsupported values before opening an Inbox transaction.

## Compatibility

`backward` is the default policy: a new consumer can read events written with the previous schema. Additive optional fields are allowed in the same major version. Removing required fields, changing types, or changing semantics requires a new version such as `OrderConfirmed.v2`; run v1 and v2 consumers in parallel until all producers and replay traffic migrate.

Replay tooling must preserve the schema headers. Poison messages retain the original headers and include `original-event-id` when a synthetic event ID is generated.
