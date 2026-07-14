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
docker build \
  --file containers/reconciler.Dockerfile \
  --build-arg "GIT_REVISION=${GIT_REVISION}" \
  --tag "lance-etl-reconciler:git-${GIT_REVISION}" \
  .
docker run --rm --entrypoint /opt/lance-etl/.venv/bin/python \
  "lance-etl-reconciler:git-${GIT_REVISION}" \
  -c "import importlib.metadata as m, pyspark, sys; print(sys.version.split()[0], pyspark.__version__, m.version('pylance'))"
```

The label output must be the current Git revision followed by `8.0.0`. The reconciler smoke output
must be Python `3.14.0`, PySpark `4.0.1`, and pylance `8.0.0`. Its Spark jars directory includes
`iceberg-spark-runtime-4.0_2.13:1.10.0` with SHA-256
`0480f1248e0a8b50ae2a730d7ad3e1a727351c362ca63f4a0c35182087a49323`. The cluster must use the
reconciler image by digest for driver and executors. No platform-supplied Spark or Iceberg runtime
may shadow those locked artifacts.

Release tags execute
[the release workflow](../.github/workflows/release.yml), which refuses images with a fixed HIGH
or CRITICAL vulnerability and records each registry digest. Never deploy a mutable image tag.

## Install production configuration

Create configuration and secrets before rendering the workload. The database role should have
read access to the serving catalog only. Storage credentials should use the cluster workload
identity rather than static environment variables.

```bash
kubectl create configmap lance-etl-search \
  --from-literal=lance-base-uri='s3://production-bucket/lance' \
  --from-literal=jwt-issuer='https://identity.example.com/' \
  --from-literal=jwt-audience='lance-etl-search' \
  --from-literal=jwks-uri='https://identity.example.com/.well-known/jwks.json' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic lance-etl-search \
  --from-literal=database-url='postgresql://search@postgres.example.com/lance?sslmode=verify-full' \
  --from-file=database-ca.pem="$DATABASE_CA_PATH" \
  --from-file=tls.crt="$SEARCH_TLS_CERT_PATH" \
  --from-file=tls.key="$SEARCH_TLS_KEY_PATH" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic lance-etl-reconciler-admin \
  --from-file=admin-token="$SEARCH_ADMIN_JWT_PATH" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic lance-etl-search-client-ca \
  --from-file=search-ca.pem="$SEARCH_SERVER_CA_PATH" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap lance-etl-reconciler \
  --from-literal=lance-base-uri='s3://production-bucket/lance' \
  --from-literal=source-table='production.vectors.events' \
  --from-literal=environment='production' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic lance-etl-reconciler-runtime \
  --from-literal=database-url='postgresql+psycopg://reconciler@postgres.example.com/lance?sslmode=verify-full&sslrootcert=/var/run/secrets/lance-etl-reconciler/database-ca.pem' \
  --from-file=database-ca.pem="$DATABASE_CA_PATH" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k deploy/reconciler
kubectl apply \
  -f deploy/search-api/service-account.yaml \
  -f deploy/search-api/headless-service.yaml \
  -f deploy/search-api/service.yaml \
  -f deploy/search-api/pod-disruption-budget.yaml \
  -f deploy/search-api/network-policy.yaml
```

The source catalog name is the first component of `source-table`. Supply its Iceberg catalog
implementation and warehouse or REST settings through the deployment-owned Spark defaults. Do not
accept those settings as a DAG-run parameter. Driver and executors must use the reconciler image's
bundled Iceberg runtime instead of resolving Maven packages at job start.

## Migrate the control plane

Run Alembic once per release before starting search or allowing the reconciler DAG to claim work.
The migration role is separate from the search read role and reconciler data role. Its URL uses
psycopg, `sslmode=verify-full`, and the exact mounted CA path.

```bash
kubectl create secret generic lance-etl-migrator \
  --from-literal=database-url='postgresql+psycopg://migrator@postgres.example.com/lance?sslmode=verify-full&sslrootcert=/var/run/secrets/lance-etl-migrator/database-ca.pem' \
  --from-file=database-ca.pem="$DATABASE_CA_PATH" \
  --dry-run=client -o yaml | kubectl apply -f -
RECONCILER_IMAGE='ghcr.io/gstamatakis95/lance-etl-reconciler@sha256:RELEASE_DIGEST'
MIGRATION_JOB=$(
  sed "s|ghcr.io/gstamatakis95/lance-etl-reconciler@sha256:0\{64\}|${RECONCILER_IMAGE}|" \
    deploy/control-plane/migration-job.yaml | kubectl create -f - -o name
)
kubectl wait --for=condition=complete --timeout=15m "$MIGRATION_JOB"
kubectl logs "$MIGRATION_JOB"
```

Never run migration from each Spark task and never run multiple release migrations concurrently.
Keep a verified PostgreSQL backup before migration. Database changes remain backward compatible
with the previous application image for the rollback window. Image rollback does not downgrade the
schema. A migration that cannot preserve that compatibility requires a separate expand, migrate,
and contract release sequence with restore rehearsal.

The zero digest in the checked-in manifests is a fail-closed placeholder. Substitute a verified
release digest before applying either deployment. The workload runs as UID and GID 65532 with a
read-only root filesystem, no service-account token, no Linux capabilities, bounded ephemeral
volumes, and a 45 second termination window around the service's fixed 30 second drain. The 12 GiB
cache volume is explicitly selected through `SEARCH_API_CACHE_DIR=/var/cache/search-api`. Public
search is TLS-only on port 8080 and requires
a JWT whose issuer, audience, signature, role, and target claims pass validation. PostgreSQL uses
`sslmode=verify-full` with the mounted database CA. Startup, readiness, and liveness use the
plaintext standard gRPC health service on private port 8081, which the public Service never exposes.
Readiness follows catalog connectivity, JWKS health, and graceful drain state.
The server certificate must cover the stable Service, canary Service, headless Service, and all
three ordinal DNS names used by exact prewarm. Keep the signing CA available to reconciler and
smoke-test clients.

Stable serving uses three StatefulSet replicas with the fixed internal endpoints
`lance-etl-search-0.lance-etl-search-internal:8080` through
`lance-etl-search-2.lance-etl-search-internal:8080`. The headless Service publishes no public health
port. The reconciler must prewarm every ordinal through TLS and admin JWT authorization before a
catalog publication succeeds. Provide at least three eligible worker nodes. Hard hostname
anti-affinity prevents two stable replicas from sharing one node, and zone spreading keeps failure
domains balanced.

Configure the Spark driver with
[`deploy/reconciler/driver-pod-template.yaml`](../deploy/reconciler/driver-pod-template.yaml). The
template fixes the ordered `LANCE_ETL_SEARCH_REPLICA_ENDPOINTS` list to those three TLS endpoints
and mounts the admin JWT at the path named by `LANCE_ETL_SEARCH_ADMIN_TOKEN_PATH`. The reconciler
reads that file afresh for every replica attempt, so projected Secret rotation does not require a
process restart. `LANCE_ETL_SEARCH_CA_PATH` names the separately projected PEM trust root. Hostname
verification remains enabled for every ordinal. The deployment-scoped token requires the `admin`
role and `lance-etl:prewarm` scope. It intentionally carries no exact logical-target claims because
one rotating fleet credential prewarms every fenced candidate. It must not be mounted into search
pods or Spark executors. Each ordinal handles
`/lance_etl.internal.v1.AdminService/PrewarmExact` locally and returns its stable replica identity
plus resolved exact version. Publication fails until all three unique identities confirm the
candidate URI and version.

Replace the reconciler image's zero digest in the pod template with the verified digest from the
reconciler release identity. Configure the Spark Kubernetes driver pod template and container
image through the deployment-owned Spark connection. Executors use the same image digest but do
not receive the admin-token volume.

The Airflow deployment fixes cluster deploy mode, the reconciler image digest, driver and executor
pod-template paths, namespace, and service account. The checked-in application reads
`LANCE_ETL_RECONCILER_IMAGE` only from the scheduler environment and rejects a value that is not a
registry digest. It sets these Spark properties without exposing them as DAG or user parameters:

```text
spark.submit.deployMode=cluster
spark.kubernetes.container.image=${LANCE_ETL_RECONCILER_IMAGE}
spark.kubernetes.driver.podTemplateFile=/opt/lance-etl/deploy/reconciler/driver-pod-template.yaml
spark.kubernetes.executor.podTemplateFile=/opt/lance-etl/deploy/reconciler/executor-pod-template.yaml
spark.kubernetes.authenticate.driver.serviceAccountName=lance-etl-reconciler
```

The driver template injects the PostgreSQL URL, Lance base URI, source table, environment,
replica endpoints, search CA, and rotating admin JWT from deployment ConfigMaps and Secrets. The
executor template receives only the immutable image, telemetry environment, writable temporary
storage, and cloud workload identity needed by executor-side Iceberg and Lance work.
Bind object-store workload identity to the `lance-etl-executor` ServiceAccount without granting it
the driver's Kubernetes Role. The `lance-etl-reconciler` ServiceAccount is reserved for the Spark
driver's bounded pod, Service, and ConfigMap lifecycle permissions.

The Airflow Spark connection supplies the Kubernetes API master, namespace, and fixed cluster
deploy mode. Its scheduler image must contain the repository at `/opt/lance-etl`, including both
pod templates and `spark-submit` from the same lock. The production DAG has no params and no
per-run configuration override.

Server certificate and private-key projection also updates files in place. The search process
loads those files at startup, so roll the StatefulSet after certificate rotation. JWKS signing-key
rotation is discovered through the HTTPS JWKS document without mounting private identity-provider
material.

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

Use a short-lived JWT scoped to the non-sensitive smoke target. The request file is a
release-controlled valid search request for that same target. Verify that the response reports the
expected exact `served_version`.

```bash
python -m bench.smoke_client \
  --endpoint 127.0.0.1:18080 \
  --server-name "$SEARCH_TLS_SERVER_NAME" \
  --ca-path "$SEARCH_SERVER_CA_PATH" \
  --token-path "$SEARCH_CANARY_JWT_PATH" \
  --request-path "$SEARCH_CANARY_REQUEST_PATH" \
  --expected-version "$SEARCH_CANARY_EXPECTED_VERSION"
```

The smoke client reads the token file inside the process immediately before the RPC. The secret is
never expanded into a command-line argument or printed in evidence.

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
  deploy/search-api/statefulset.yaml | kubectl apply -f -
kubectl rollout status statefulset/lance-etl-search --timeout=20m
```

## Roll back the service image

Kubernetes retains ten stable StatefulSet revisions. This command restores the immediately
preceding image digest and waits for its private gRPC health checks.

```bash
kubectl rollout undo statefulset/lance-etl-search
kubectl rollout status statefulset/lance-etl-search --timeout=20m
```

Confirm the resulting pod image is digest-addressed.

```bash
kubectl get pods -l app.kubernetes.io/name=lance-etl-search,app.kubernetes.io/track=stable \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[0].imageID}{"\n"}{end}'
```

## Roll back one serving catalog target

Catalog rollback never changes `ingest_lance_uri` and never updates PostgreSQL directly. The
restricted repair command validates a retained successful SERVE or REBUILD publication and
enqueues new PREWARM work containing its exact immutable URI, version, manifest, and digest. The
ordinary reconciler then claims the work with a new lease and target fence, prewarms every required
replica, and performs the normal catalog compare-and-swap. A busy target lane, mismatched target,
or incomplete retained result is rejected before enqueue.

First list retained validated publications for the target. Read-only evidence selection may use a
catalog replica.

```bash
psql "$LANCE_ETL_DATABASE_URL" -X -v ON_ERROR_STOP=1 -v target_id="$TARGET_ID" -c \
  "SELECT work_id, candidate_lance_uri, indexed_lance_version, updated_at
   FROM target_work
   WHERE target_id = :'target_id'::uuid
     AND state = 'SUCCEEDED'
     AND kind IN ('SERVE', 'REBUILD')
     AND artifact_manifest_uri IS NOT NULL
     AND artifact_digest IS NOT NULL
   ORDER BY updated_at DESC;"
```

Validate the fully specified logical target first. The dry run parses and validates the restricted
command without changing control state.

```bash
lance-etl-reconcile repair \
  --action rollback \
  --tenant-id "$TENANT_ID" \
  --namespace "$NAMESPACE" \
  --org-id "$ORG_ID" \
  --retained-work-id "$RETAINED_WORK_ID" \
  --dry-run
lance-etl-reconcile repair \
  --action rollback \
  --tenant-id "$TENANT_ID" \
  --namespace "$NAMESPACE" \
  --org-id "$ORG_ID" \
  --retained-work-id "$RETAINED_WORK_ID"
```

The second command returns the new work identity. Do not bypass the queue if it remains pending or
retrying. Run the normal `run_due_target_work`, `reconcile_results`, and `emit_slo_status` phases or
wait for the next scheduled reconciler cycle. Observe that work identity until it reaches
`SUCCEEDED` and `PUBLISH`. A `BLOCKED` result requires operator diagnosis and a `RETRY_WAIT` result
must retain its identity for ordinary retry.

After success, resolve the logical target through every replica until each reports the restored
exact version. Catalog caches can retain the prior validated tuple for at most their short
propagation TTL. Keep the replaced publication pin and artifact manifest through the full rollback
and audit horizon.
