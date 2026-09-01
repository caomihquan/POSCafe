# Production Deployment Baseline

This directory contains a reference Docker Compose deployment for infrastructure and all current APIs. It is a self-hosted baseline; production teams should replace local stateful containers with managed HA services where available.

## Usage

Create a secret-managed environment file outside source control:

```bash
docker compose --env-file /run/secrets/poscafe.env -f deploy/docker-compose.production.yml up -d --build
```

Required values include `POSTGRES_USER`, `POSTGRES_PASSWORD`, `MONGO_ROOT_USERNAME`, `MONGO_ROOT_PASSWORD`, `JWT_KEY`, `OPSDB_CONNECTION_STRING`, and `ORDERDB_CONNECTION_STRING` (plus the remaining service connection strings referenced in the Compose file). Compose initializes the single-node Mongo replica set, provisions the required Kafka topics, and applies PostgreSQL migrations before APIs start. Use TLS-enabled managed PostgreSQL/Mongo/Kafka in real production; this Compose file is a self-hosted baseline, not a substitute for managed HA infrastructure.

Run EF migrations once as a release job before scaling API replicas. Do not enable application startup migrations for multi-replica production. Place TLS termination, rate limiting, and the public firewall in the ingress layer; keep PostgreSQL, MongoDB, Kafka, health, and metrics endpoints on the private network.

The release job is provided as `deploy/migrate.ps1` or `deploy/migrate.sh`. Run it from the repository root with `POSCAFE_MIGRATION_ALLOWED=true` and secret-managed `ConnectionStrings__*` variables. It is fail-fast and intentionally requires an explicit opt-in flag so an accidental shell invocation cannot mutate production schema. The `Migration.Dockerfile` builds the solution before using `--no-build`; for a host-based release job, build the solution first and run with a single deployment identity while API replicas remain stopped or isolated from traffic.

Kubernetes manifests are under `deploy/k8s/`. Build and publish `deploy/Migration.Dockerfile` as a private migration image, replace the example image names and secret resources, then apply the migration Job before the application rollout. `secrets.example.yaml` is intentionally not included by `kustomization.yaml`; use External Secrets, Sealed Secrets, or the cloud secret manager integration.

The Helm chart is under `deploy/helm/poscafe/`. Set image repositories/tags and secret names in an environment values file, then install with `helm upgrade --install poscafe deploy/helm/poscafe -n poscafe --create-namespace -f values.production.yaml`. Its pre-install/pre-upgrade hook runs the separately published migration image before API rollout.

The chart also provisions a PodDisruptionBudget by default. Enable `autoscaling.enabled` only after resource requests and cluster metrics-server are configured; set `minReplicas`, `maxReplicas`, and the CPU target per workload rather than using the defaults blindly.

Set `ingress.enabled=true` only when the cluster ingress controller and TLS certificate strategy are ready. Set `externalSecret.enabled=true` when External Secrets Operator is installed; configure `secretStoreRef` and `remoteKey` for the platform secret manager. The chart intentionally creates no plaintext production Secret by default.

The Kubernetes baseline includes deny-by-default network policy. Update namespace labels and allowed database/broker selectors to match the cluster platform before applying it; verify DNS, ingress-controller, PostgreSQL, MongoDB, and Kafka labels in a staging cluster first.

Kubernetes Service names intentionally match the Gateway YARP cluster IDs (`identity`, `catalog`, `order`, `payment`, `inventory`, `store`, `reporting`, and `gateway`). If a platform naming policy requires prefixes, update the Gateway `ReverseProxy:Clusters:*:Destinations` addresses together with the Service names.

The `poscafe` namespace also enables Kubernetes Pod Security Standards at `restricted` for enforce, audit, and warn. Keep this policy enabled in production; if a platform component requires an exception, isolate that component in a separate namespace rather than weakening the application namespace.
