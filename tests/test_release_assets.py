"""Tests for locked release inputs and production deployment assets."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

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
    assert "uv sync --locked" in workflows
    for command in re.findall(r"cargo (?:clippy|test|build)[^\n]*", workflows):
        assert "--locked" in command


def test_container_is_immutable_and_unprivileged() -> None:
    """The production image pins bases, records identity, and drops root."""
    dockerfile: str = read_repository_file("containers/search-api.Dockerfile")
    assert dockerfile.count("@sha256:") == 2
    assert 'org.opencontainers.image.revision="${GIT_REVISION}"' in dockerfile
    assert 'io.lance-etl.lance.version="8.0.0"' in dockerfile
    assert "cargo build --release --locked" in dockerfile
    assert "USER 65532:65532" in dockerfile


def test_deployment_fails_closed_and_exposes_runtime_health() -> None:
    """The stable workload uses a digest placeholder and hardened runtime health gates."""
    deployment: str = read_repository_file("deploy/search-api/deployment.yaml")
    assert "@sha256:" + "0" * 64 in deployment
    assert "readOnlyRootFilesystem: true" in deployment
    assert "allowPrivilegeEscalation: false" in deployment
    assert "automountServiceAccountToken: false" in deployment
    assert "startupProbe:" in deployment
    assert "readinessProbe:" in deployment
    assert "livenessProbe:" in deployment
    assert "terminationGracePeriodSeconds: 660" in deployment


def test_catalog_rollback_is_exact_and_audited() -> None:
    """Rollback accepts only retained success and compare-swaps the exact current tuple."""
    rollback: str = read_repository_file("deploy/rollback-serving.sh")
    assert "work.state = 'SUCCEEDED'" in rollback
    assert "work.kind IN ('SERVE', 'REBUILD')" in rollback
    assert "target.served_lance_uri = :'expected_uri'" in rollback
    assert "target.served_lance_version = :'expected_version'::bigint" in rollback
    assert "INSERT INTO target_work" in rollback
    assert "ROLLBACK;" in rollback
