# Production release and rollback

Production runs only an image addressed by its OCI digest. The image is built from pinned base
images with Cargo's lockfile and records the exact Git revision and Lance version in OCI labels.
The release workflow also publishes an SPDX JSON SBOM, provenance attestation, vulnerability gate,
and immutable release identity artifact.

## Build and inspect locally

Use the repository toolchain and lockfiles. The Git revision build argument is required.

```bash
GIT_REVISION=$(git rev-parse HEAD)
docker build \
  --file containers/search-api.Dockerfile \
  --build-arg "GIT_REVISION=${GIT_REVISION}" \
  --tag "lance-etl-search:git-${GIT_REVISION}" \
  .
docker inspect \
  --format '{{index .Config.Labels "org.opencontainers.image.revision"}} {{index .Config.Labels "io.lance-etl.lance.version"}}' \
  "lance-etl-search:git-${GIT_REVISION}"
```

The output must be the current Git revision followed by `8.0.0`. Release tags execute
[the release workflow](../.github/workflows/release.yml), which refuses images with a fixed HIGH
or CRITICAL vulnerability and records the registry digest. Never deploy a mutable image tag.

## Install production configuration

Create configuration and secrets before rendering the workload. The database role should have
read access to the serving catalog only. Storage credentials should use the cluster workload
identity rather than static environment variables.

```bash
kubectl create configmap lance-etl-search \
  --from-literal=lance-base-uri='s3://production-bucket/lance' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic lance-etl-search \
  --from-literal=database-url="$LANCE_ETL_DATABASE_URL" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k deploy/search-api
```

The zero digest in the checked-in manifests is a fail-closed placeholder. Substitute a verified
release digest before applying either deployment. The workload runs as UID and GID 65532 with a
read-only root filesystem, no service-account token, no Linux capabilities, bounded ephemeral
volumes, and a 660 second graceful-drain window. Its startup, readiness, and liveness probes use the
standard gRPC health service.

## Canary an image digest

Set `IMAGE` to the exact image and registry digest from `release-identity.json`. The following
command renders the canary without modifying the checked-in manifest.

```bash
IMAGE='ghcr.io/gstamatakis95/lance-etl-search@sha256:RELEASE_DIGEST'
test "${IMAGE#*@sha256:}" != "$IMAGE"
sed "s|ghcr.io/gstamatakis95/lance-etl-search@sha256:0\{64\}|${IMAGE}|" \
  deploy/search-api/canary.yaml | kubectl apply -f -
kubectl rollout status deployment/lance-etl-search-canary --timeout=15m
kubectl port-forward service/lance-etl-search-canary 18080:8080
```

From another shell, verify the registered service and run the release-owned authenticated smoke
query against a non-sensitive test target. The search response must report the exact catalog
`served_version`.

```bash
grpcurl -plaintext \
  -d '{"service":"lance_etl.v1.SearchService"}' \
  127.0.0.1:18080 grpc.health.v1.Health/Check
```

Hold the canary for the observation window. Compare request errors, deadlines, saturation, object
store requests, cold latency, and recall against stable replicas. Delete the canary immediately on
any regression.

```bash
kubectl delete -f deploy/search-api/canary.yaml --ignore-not-found
```

Promote by applying the same verified digest to the stable manifest. Wait for every replica to
become ready before ending the release window.

```bash
sed "s|ghcr.io/gstamatakis95/lance-etl-search@sha256:0\{64\}|${IMAGE}|" \
  deploy/search-api/deployment.yaml | kubectl apply -f -
kubectl rollout status deployment/lance-etl-search --timeout=20m
```

## Roll back the service image

Kubernetes retains ten stable ReplicaSets. This command restores the immediately preceding image
digest and waits for its gRPC health checks.

```bash
kubectl rollout undo deployment/lance-etl-search
kubectl rollout status deployment/lance-etl-search --timeout=20m
```

Confirm the resulting pod image is digest-addressed.

```bash
kubectl get pods -l app.kubernetes.io/name=lance-etl-search,app.kubernetes.io/track=stable \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

## Roll back one serving catalog target

Catalog rollback never changes `ingest_lance_uri`. It selects a retained successful SERVE or
REBUILD publication, locks the current target, compares the expected current URI and version, and
updates the exact served tuple in one PostgreSQL transaction. The transaction also inserts a
successful publication audit row. A mismatch or missing publication rolls back and exits nonzero.

First list retained validated publications for the target and record the current catalog tuple.

```bash
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v target_id="$TARGET_ID" -c \
  "SELECT work_id, candidate_lance_uri, indexed_lance_version, updated_at
   FROM target_work
   WHERE target_id = :'target_id'::uuid
     AND state = 'SUCCEEDED'
     AND kind IN ('SERVE', 'REBUILD')
     AND artifact_manifest_uri IS NOT NULL
     AND artifact_digest IS NOT NULL
   ORDER BY updated_at DESC;"
psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1 -v target_id="$TARGET_ID" -c \
  "SELECT served_lance_uri, served_lance_version
   FROM targets WHERE target_id = :'target_id'::uuid;"
```

Run the compare-and-swap with an independently generated audit work ID.

```bash
export ROLLBACK_WORK_ID='UUID_OF_PRIOR_SUCCESS'
export ROLLBACK_AUDIT_WORK_ID="$(uuidgen)"
export EXPECTED_URI='CURRENT_SERVED_LANCE_URI'
export EXPECTED_VERSION='CURRENT_SERVED_LANCE_VERSION'
deploy/rollback-serving.sh
```

After success, resolve the logical target through the search service until every replica reports
the restored exact version. Catalog caches can retain the prior validated tuple for at most their
short propagation TTL. Keep the replaced publication pin and artifact manifest through the full
rollback and audit horizon.
