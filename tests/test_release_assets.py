"""Tests for locked release inputs and production deployment assets."""

from __future__ import annotations

import re
import textwrap
import tomllib
from pathlib import Path

import pytest
from alembic import command as alembic_command
from alembic.config import Config

REPOSITORY_ROOT: Path = Path(__file__).resolve().parent.parent
SHA_PATTERN: re.Pattern[str] = re.compile(r"uses:\s+[^\s@]+@([0-9a-f]{40})")
REMOTE_ACTION_PATTERN: re.Pattern[str] = re.compile(r"uses:\s+([^\s]+@[^\s]+)")


def read_repository_file(relative_path: str) -> str:
    """Read one UTF-8 repository file.

    Args:
        relative_path: Path relative to the repository root.

    Returns:
        File contents.
    """
    return (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")


def test_python_and_rust_release_versions_are_exact() -> None:
    """Production language and Lance inputs are exact and represented in lockfiles."""
    project = tomllib.loads(read_repository_file("pyproject.toml"))
    assert "pylance==8.0.0" in project["project"]["dependencies"]
    assert 'specifier = "==8.0.0"' in read_repository_file("uv.lock")
    toolchain = tomllib.loads(read_repository_file("rust-toolchain.toml"))
    assert toolchain["toolchain"] == {
        "channel": "1.91.0",
        "components": ["clippy", "rustfmt"],
        "profile": "minimal",
    }


def test_ci_uses_immutable_actions_and_locked_commands() -> None:
    """Every remote action is commit-pinned and dependency-resolving command is locked."""
    workflows = "\n".join(
        read_repository_file(path)
        for path in (".github/workflows/ci.yml", ".github/workflows/integration.yml", ".github/workflows/release.yml")
    )
    remote_actions: list[str] = REMOTE_ACTION_PATTERN.findall(workflows)
    assert remote_actions
    pinned_actions: list[str] = SHA_PATTERN.findall(workflows)
    assert len(pinned_actions) == len(remote_actions)
    assert "continue-on-error" not in workflows
    assert "ubuntu-latest" not in workflows
    assert "apt-get" not in workflows
    assert 'python-version: "3.14.0"' in workflows
    assert 'java-version: "17.0.19+10"' in workflows
    assert 'version: "33.2"' in workflows
    assert "uv sync --locked" in workflows
    assert "redis:8.2.1-bookworm@sha256:5fa2edb1e408fa8235e6db8fab01d1afaaae96c9403ba67b70feceb8661e8621" in workflows
    assert "SEARCH_API_TEST_REDIS_URL: redis://127.0.0.1:6379" in workflows
    assert (
        "ghcr.io/yannh/kubeconform:v0.7.0-alpine@sha256:8f0eeaaa96ba27ba1500b0e4b1c215acc358d159c62a7ecae58d7a03403287b0"
        in workflows
    )
    assert "rhysd/actionlint:1.7.7@sha256:887a259a5a534f3c4f36cb02dca341673c6089431057242cdc931e9f133147e9" in workflows
    assert "containers/search-api.Dockerfile" in workflows
    assert "containers/reconciler.Dockerfile" in workflows
    assert "provenance: mode=max" in workflows
    assert "sbom: true" in workflows
    assert "severity: HIGH,CRITICAL" in workflows
    for command in re.findall(r"cargo (?:clippy|test|build)[^\n]*", workflows):
        assert "--locked" in command


def test_container_is_immutable_and_unprivileged() -> None:
    """The production image pins bases, records identity, and drops root."""
    dockerfile: str = read_repository_file("containers/search-api.Dockerfile")
    assert dockerfile.count("@sha256:") == 2
    assert 'org.opencontainers.image.revision="${GIT_REVISION}"' in dockerfile
    assert 'io.lance-etl.lance.version="8.0.0"' in dockerfile
    assert "protoc-33.2-linux-x86_64.zip" in dockerfile
    assert "b24b53f87c151bfd48b112fe4c3a6e6574e5198874f38036aff41df3456b8caf" in dockerfile
    assert "706662a332683aa2fffe1c4ea61588279d31679cd42d91c7d60a69651768edb8" in dockerfile
    assert "protoc-33.2-linux-aarch_64.zip" in dockerfile
    assert "libprotoc 33.2" in dockerfile
    assert "cargo build --release --locked" in dockerfile
    assert "EXPOSE 8080 8081" in dockerfile
    assert "USER 65532:65532" in dockerfile
    reconciler: str = read_repository_file("containers/reconciler.Dockerfile")
    assert reconciler.count("@sha256:") == 2
    assert "uv sync --locked" in reconciler
    assert "uv python install 3.14.0" in reconciler
    assert 'io.lance-etl.pylance.version="8.0.0"' in reconciler
    assert 'io.lance-etl.spark.version="4.0.1"' in reconciler
    assert "iceberg-spark-runtime-4.0_2.13-1.10.0.jar" in reconciler
    assert "0480f1248e0a8b50ae2a730d7ad3e1a727351c362ca63f4a0c35182087a49323" in reconciler
    assert ".venv/bin/spark-submit --version" in reconciler
    assert "USER 185:185" in reconciler
    assert "apt-get" not in reconciler


def test_deployment_fails_closed_and_exposes_runtime_health() -> None:
    """The stable workload uses a digest placeholder and hardened runtime health gates."""
    deployment: str = read_repository_file("deploy/search-api/statefulset.yaml")
    assert "@sha256:" + "0" * 64 in deployment
    assert "kind: StatefulSet" in deployment
    assert "replicas: 3" in deployment
    assert "serviceName: lance-etl-search-internal" in deployment
    assert "readOnlyRootFilesystem: true" in deployment
    assert "allowPrivilegeEscalation: false" in deployment
    assert "automountServiceAccountToken: false" in deployment
    assert "fsGroup: 65532" in deployment
    assert "startupProbe:" in deployment
    assert "readinessProbe:" in deployment
    assert "livenessProbe:" in deployment
    assert "terminationGracePeriodSeconds: 45" in deployment
    assert "requiredDuringSchedulingIgnoredDuringExecution" in deployment
    assert "topologyKey: kubernetes.io/hostname" in deployment
    assert "topologyKey: topology.kubernetes.io/zone" in deployment
    assert "SEARCH_API_CACHE_DIR" in deployment
    assert "value: /var/cache/search-api" in deployment
    assert deployment.count("port: 8081") >= 3
    assert "service: lance_etl.v1.SearchService" not in deployment
    assert "SEARCH_API_DATABASE_CA_PATH" in deployment
    assert "SEARCH_API_TLS_CERT_PATH" in deployment
    assert "SEARCH_API_TLS_KEY_PATH" in deployment
    assert "SEARCH_API_JWT_ISSUER" in deployment
    assert "SEARCH_API_JWT_AUDIENCE" in deployment
    assert "SEARCH_API_JWKS_URI" in deployment
    assert "SEARCH_API_REPLICA_ID" in deployment
    assert "fieldPath: metadata.name" in deployment
    assert "admin-token" not in deployment
    service: str = read_repository_file("deploy/search-api/service.yaml")
    assert "port: 8081" not in service
    headless: str = read_repository_file("deploy/search-api/headless-service.yaml")
    assert "clusterIP: None" in headless
    assert "port: 8081" not in headless
    policy: str = read_repository_file("deploy/search-api/network-policy.yaml")
    assert "kind: NetworkPolicy" in policy
    assert "port: 8080" in policy
    assert "port: 8081" in policy
    assert "port: 6379" not in policy
    canary_runbook: str = read_repository_file("docs/production-release.md")
    canary: str = read_repository_file("deploy/search-api/canary.yaml")
    assert "SEARCH_API_REPLICA_ID" in canary
    assert "fieldPath: metadata.name" in canary
    assert "grpcurl -plaintext" not in canary_runbook
    assert '-H "authorization: Bearer' in canary_runbook
    assert "-cacert" in canary_runbook
    assert "sslmode=verify-full" in canary_runbook


def test_catalog_rollback_uses_fenced_reconciler_work() -> None:
    """Rollback documentation enqueues retained evidence through the restricted repair surface."""
    runbook: str = read_repository_file("docs/production-release.md")
    assert "lance-etl-reconcile repair" in runbook
    assert "--action rollback" in runbook
    assert "--retained-work-id" in runbook
    assert "--dry-run" in runbook
    assert "PREWARM" in runbook
    assert "target fence" in runbook
    assert "deploy/rollback-serving.sh" not in runbook


def test_reconciler_prewarm_contract_targets_every_stable_ordinal() -> None:
    """The Spark driver receives all replica endpoints and a rotating admin JWT file."""
    template: str = read_repository_file("deploy/reconciler/driver-pod-template.yaml")
    assert "LANCE_ETL_SEARCH_REPLICA_ENDPOINTS" in template
    for ordinal in range(3):
        assert f"https://lance-etl-search-{ordinal}.lance-etl-search-internal:8080" in template
    assert "LANCE_ETL_SEARCH_ADMIN_TOKEN_PATH" in template
    assert "/var/run/secrets/lance-etl-reconciler/admin-token" in template
    assert "LANCE_ETL_SEARCH_CA_PATH" in template
    assert "/var/run/secrets/lance-etl-reconciler/search-ca.pem" in template
    assert "name: lance-etl-reconciler-admin" in template
    assert "name: lance-etl-search-client-ca" in template
    assert "app.kubernetes.io/component: reconciler" in template
    assert "LANCE_ETL_DATABASE_URL" in template
    assert "LANCE_ETL_LANCE_BASE_URI" in template
    assert "LANCE_ETL_SOURCE_TABLE" in template
    assert "DD_SERVICE" in template
    executor: str = read_repository_file("deploy/reconciler/executor-pod-template.yaml")
    assert "spark-kubernetes-executor" in executor
    assert "lance-etl-reconciler@sha256:" + "0" * 64 in executor
    assert "LANCE_ETL_SEARCH_ADMIN_TOKEN_PATH" not in executor
    assert "LANCE_ETL_DATABASE_URL" not in executor
    assert "serviceAccountName: lance-etl-executor" in executor
    rbac: str = read_repository_file("deploy/reconciler/rbac.yaml")
    assert "kind: Role" in rbac
    assert "kind: RoleBinding" in rbac
    assert "name: lance-etl-reconciler" in rbac
    assert "name: lance-etl-executor" in rbac
    runbook: str = read_repository_file("docs/production-release.md")
    assert "/lance_etl.internal.v1.AdminService/PrewarmExact" in runbook
    assert "reads that file afresh" in runbook
    assert "spark.kubernetes.driver.podTemplateFile" in runbook
    assert "spark.kubernetes.executor.podTemplateFile" in runbook


def test_control_plane_migration_is_one_shot_and_tls_verified(monkeypatch: pytest.MonkeyPatch) -> None:
    """The non-root Job gives Alembic the exact TLS-verified production URL.

    Args:
        monkeypatch: Scoped Alembic and environment replacement fixture.
    """
    migration: str = read_repository_file("deploy/control-plane/migration-job.yaml")
    assert "kind: Job" in migration
    assert "generateName: lance-etl-control-plane-migrate-" in migration
    assert "lance-etl-reconciler@sha256:" + "0" * 64 in migration
    assert 'command.upgrade(config, "head")' in migration
    assert "name: lance-etl-migrator" in migration
    assert "readOnlyRootFilesystem: true" in migration
    assert "runAsNonRoot: true" in migration
    script_match = re.search(r"          args:\n            - \|\n(?P<script>(?:              .*\n)+)", migration)
    assert script_match is not None
    script = textwrap.dedent(script_match.group("script"))
    production_url = (
        "postgresql+psycopg://migrator:p%25word@postgres.example.com/lance?"
        "sslmode=verify-full&sslrootcert=/var/run/secrets/lance-etl-migrator/database-ca.pem"
    )
    captured: dict[str, str] = {}

    def make_config(path: str) -> Config:
        """Replace the image path with the checked-in Alembic configuration.

        Args:
            path: Absolute path requested by the migration Job.

        Returns:
            Local Alembic configuration using the same checked-in file.
        """
        captured["config_path"] = path
        return Config(str(REPOSITORY_ROOT / "alembic.ini"))

    def capture_upgrade(config: Config, revision: str) -> None:
        """Capture the URL that the Job passes to Alembic.

        Args:
            config: Prepared Alembic configuration.
            revision: Requested migration revision.
        """
        captured["url"] = config.get_main_option("sqlalchemy.url")
        captured["revision"] = revision

    monkeypatch.setenv("LANCE_ETL_DATABASE_URL", production_url)
    monkeypatch.setattr("alembic.config.Config", make_config)
    monkeypatch.setattr(alembic_command, "upgrade", capture_upgrade)
    exec(script, {})
    assert captured == {
        "config_path": "/opt/lance-etl/alembic.ini",
        "url": production_url,
        "revision": "head",
    }
    runbook: str = read_repository_file("docs/production-release.md")
    assert "sslmode=verify-full" in runbook
    assert "kubectl wait --for=condition=complete" in runbook
    assert "never run multiple release migrations concurrently" in runbook.lower()
