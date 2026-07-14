"""Tests for immutable candidate evidence and fenced exact-version publication."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from pathlib import Path

import lance
import pyarrow as pa
import pytest

from lance_etl.etl.profile import PRODUCTION_PROFILE
from lance_etl.publication import (
    CandidateCounts,
    IndexOutcome,
    PrewarmResult,
    PublicationCoordinator,
    build_artifact_manifest,
    candidate_pin_name,
)
from lance_etl.publication.manifest import ArtifactManifest
from lance_etl.state import WorkClaim, WorkKind, WorkPhase


def candidate_schema() -> pa.Schema:
    """Return a minimal exact schema accepted by the production profile."""
    return pa.schema(
        [
            pa.field("vector_id", pa.string(), nullable=False),
            pa.field("lance_etl_window_seq", pa.int64(), nullable=False),
            pa.field("lance_etl_source_sequence", pa.int64(), nullable=False),
            pa.field("lance_etl_event_digest", pa.binary(32), nullable=False),
            pa.field("is_deleted", pa.bool_(), nullable=False),
            pa.field("vector", pa.list_(pa.float32(), 128)),
            pa.field("text", pa.string()),
            pa.field("cluster", pa.string()),
            pa.field("category", pa.string()),
            pa.field("event_timestamp", pa.timestamp("us")),
            pa.field("ttl", pa.duration("us")),
        ]
    )


def write_candidate(tmp_path: Path) -> lance.LanceDataset:
    """Create a tiny candidate at a stable local URI.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Open local Lance dataset.
    """
    schema = candidate_schema()
    table = pa.Table.from_pylist(
        [
            {
                "vector_id": "v1",
                "lance_etl_window_seq": 1,
                "lance_etl_source_sequence": 10,
                "lance_etl_event_digest": b"a" * 32,
                "is_deleted": False,
                "vector": [0.0] * 128,
            }
        ],
        schema=schema,
    )
    return lance.write_dataset(table, str(tmp_path / "candidate.lance"))


def index_outcomes() -> tuple[IndexOutcome, ...]:
    """Return valid required vector and tombstone index evidence."""
    return (
        IndexOutcome("vector_idx", "vector", "vector", True, True, 0, "ab" * 32),
        IndexOutcome("deleted_idx", "is_deleted", "bitmap", True, True, 0),
    )


def build_manifest(tmp_path: Path, work_id: uuid.UUID, target_id: uuid.UUID) -> ArtifactManifest:
    """Build validated manifest evidence for a tiny local candidate.

    Args:
        tmp_path: Pytest temporary directory.
        work_id: Durable work identity.
        target_id: Opaque target identity.

    Returns:
        Validated immutable manifest.
    """
    dataset = write_candidate(tmp_path)
    return build_artifact_manifest(
        dataset,
        work_id,
        target_id,
        dataset.uri,
        1,
        dataset.version,
        PRODUCTION_PROFILE,
        CandidateCounts(1, 1, 1),
        index_outcomes(),
    )


def test_manifest_is_canonical_and_content_addressed(tmp_path: Path) -> None:
    """Identical exact evidence has stable bytes, digest, URI, and immutable pin."""
    work_id = uuid.uuid4()
    target_id = uuid.uuid4()
    manifest = build_manifest(tmp_path, work_id, target_id)
    assert manifest.canonical_bytes() == manifest.canonical_bytes()
    assert manifest.content_uri().endswith(f"/{manifest.digest().hex()}.json")
    assert candidate_pin_name(work_id) == f"candidate-{work_id.hex}"


def test_manifest_blocks_duplicate_live_ids_and_required_index_failure(tmp_path: Path) -> None:
    """Uniqueness and required-index failures cannot produce publishable evidence."""
    dataset = write_candidate(tmp_path)
    arguments = (
        dataset,
        uuid.uuid4(),
        uuid.uuid4(),
        dataset.uri,
        1,
        dataset.version,
        PRODUCTION_PROFILE,
    )
    with pytest.raises(ValueError, match="uniqueness"):
        build_artifact_manifest(*arguments, CandidateCounts(2, 2, 1), index_outcomes())
    invalid = (IndexOutcome("deleted_idx", "is_deleted", "bitmap", True, False, 1, error_code="coverage"),)
    with pytest.raises(ValueError, match="required index"):
        build_artifact_manifest(*arguments, CandidateCounts(1, 1, 1), invalid)


@dataclass
class FakeArtifacts:
    """In-memory immutable artifact store test double."""

    writes: list[tuple[str, bytes, bytes]] = field(default_factory=list)
    fail_once: bool = False

    def put_if_absent(self, uri: str, payload: bytes, digest: bytes) -> None:
        """Record one immutable artifact write.

        Args:
            uri: Content-addressed URI.
            payload: Canonical bytes.
            digest: Expected digest.
        """
        self.writes.append((uri, payload, digest))
        if self.fail_once:
            self.fail_once = False
            raise OSError("injected artifact crash")


@dataclass
class FakeRuntime:
    """Exact prewarm and tag operation test double."""

    prewarm_results: tuple[PrewarmResult, ...]
    fail_head: bool = False
    fail_once_at: str | None = None
    calls: list[tuple[object, ...]] = field(default_factory=list)

    def pin_candidate(self, uri: str, version: int, pin: str) -> None:
        """Record candidate pin creation.

        Args:
            uri: Candidate URI.
            version: Candidate version.
            pin: Immutable pin.
        """
        self.calls.append(("pin", uri, version, pin))
        self.raise_once("pin")

    def prewarm(self, uri: str, version: int) -> tuple[PrewarmResult, ...]:
        """Return configured replica resolutions.

        Args:
            uri: Candidate URI.
            version: Candidate version.

        Returns:
            Configured results.
        """
        self.calls.append(("prewarm", uri, version))
        self.raise_once("prewarm")
        return self.prewarm_results

    def mirror_head(self, uri: str, version: int) -> None:
        """Record or fail the best-effort HEAD mirror.

        Args:
            uri: Published URI.
            version: Published version.
        """
        self.calls.append(("head", uri, version))
        if self.fail_head:
            raise OSError("transient mirror failure")

    def raise_once(self, step: str) -> None:
        """Raise one injected crash at a selected external step.

        Args:
            step: Current external step name.
        """
        if self.fail_once_at == step:
            self.fail_once_at = None
            raise OSError(f"injected {step} crash")


@dataclass
class FakeRepository:
    """Atomic publication repository test double."""

    accepted: bool = True
    fail_once: bool = False
    calls: list[tuple[object, ...]] = field(default_factory=list)

    def publish_serve(
        self,
        claim: WorkClaim,
        candidate_lance_uri: str,
        indexed_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
    ) -> bool:
        """Record one atomic catalog publication.

        Args:
            claim: Fenced work claim.
            candidate_lance_uri: Exact URI.
            indexed_lance_version: Exact version.
            artifact_manifest_uri: Immutable manifest URI.
            artifact_digest: Manifest digest.

        Returns:
            Configured fence outcome.
        """
        self.calls.append((claim, candidate_lance_uri, indexed_lance_version, artifact_manifest_uri, artifact_digest))
        if self.fail_once:
            self.fail_once = False
            raise OSError("injected ambiguous database result")
        return self.accepted


def test_coordinator_requires_exact_prewarm_and_treats_head_as_best_effort(tmp_path: Path) -> None:
    """Catalog publication follows exact prewarm while a failed HEAD mirror remains safe."""
    work_id = uuid.uuid4()
    target_id = uuid.uuid4()
    manifest = build_manifest(tmp_path, work_id, target_id)
    claim = WorkClaim(
        work_id, target_id, WorkKind.SERVE, WorkPhase.PREWARM, uuid.uuid4(), 2, 1, None, manifest.candidate_lance_uri, 1
    )
    result = PrewarmResult("replica-a", manifest.candidate_lance_uri, manifest.indexed_lance_version)
    repository = FakeRepository()
    artifacts = FakeArtifacts()
    runtime = FakeRuntime((result,), fail_head=True)
    assert PublicationCoordinator(repository, artifacts, runtime).publish(claim, manifest)
    assert len(repository.calls) == 1
    assert len(artifacts.writes) == 1
    assert runtime.calls[-1][0] == "head"


def test_coordinator_blocks_mismatched_replica_before_catalog_write(tmp_path: Path) -> None:
    """A replica resolving another version prevents the catalog compare-and-swap."""
    work_id = uuid.uuid4()
    target_id = uuid.uuid4()
    manifest = build_manifest(tmp_path, work_id, target_id)
    claim = WorkClaim(
        work_id, target_id, WorkKind.SERVE, WorkPhase.PREWARM, uuid.uuid4(), 2, 1, None, manifest.candidate_lance_uri, 1
    )
    result = PrewarmResult("replica-a", manifest.candidate_lance_uri, manifest.indexed_lance_version + 1)
    repository = FakeRepository()
    with pytest.raises(ValueError, match="different publication"):
        PublicationCoordinator(repository, FakeArtifacts(), FakeRuntime((result,))).publish(claim, manifest)
    assert repository.calls == []


@pytest.mark.parametrize("failed_step", ["pin", "artifact", "prewarm", "publish"])
def test_retry_converges_after_each_prepublication_external_crash(tmp_path: Path, failed_step: str) -> None:
    """A repeated fenced attempt converges after every ambiguous external boundary.

    Args:
        tmp_path: Pytest temporary directory.
        failed_step: External operation that fails once.
    """
    work_id = uuid.uuid4()
    target_id = uuid.uuid4()
    manifest = build_manifest(tmp_path, work_id, target_id)
    claim = WorkClaim(
        work_id,
        target_id,
        WorkKind.SERVE,
        WorkPhase.PREWARM,
        uuid.uuid4(),
        2,
        1,
        None,
        manifest.candidate_lance_uri,
        1,
    )
    result = PrewarmResult("replica-a", manifest.candidate_lance_uri, manifest.indexed_lance_version)
    repository = FakeRepository(fail_once=failed_step == "publish")
    artifacts = FakeArtifacts(fail_once=failed_step == "artifact")
    runtime = FakeRuntime((result,), fail_once_at=failed_step if failed_step in ("pin", "prewarm") else None)
    coordinator = PublicationCoordinator(repository, artifacts, runtime)
    with pytest.raises(OSError, match="injected"):
        coordinator.publish(claim, manifest)
    assert coordinator.publish(claim, manifest)
