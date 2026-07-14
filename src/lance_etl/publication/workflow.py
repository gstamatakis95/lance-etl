"""Fenced exact-version publication after immutable validation and replica prewarm."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from lance_etl.publication.manifest import ArtifactManifest, candidate_pin_name
from lance_etl.state import WorkClaim


class ImmutableArtifactStore(Protocol):
    """Content-addressed storage boundary for immutable publication evidence."""

    def put_if_absent(self, uri: str, payload: bytes, digest: bytes) -> None:
        """Create or verify immutable content at its content address.

        Args:
            uri: Content-addressed manifest URI.
            payload: Canonical manifest bytes.
            digest: Expected SHA-256 payload digest.
        """
        ...


class PublicationRuntime(Protocol):
    """Executor-owned Lance tag and serving-replica operations."""

    def pin_candidate(self, uri: str, version: int, pin: str) -> None:
        """Create or verify one immutable candidate pin.

        Args:
            uri: Exact candidate dataset URI.
            version: Exact candidate version.
            pin: Work-derived immutable tag name.
        """
        ...

    def prewarm(self, uri: str, version: int) -> tuple[PrewarmResult, ...]:
        """Prewarm all required serving replicas at one exact target.

        Args:
            uri: Exact candidate dataset URI.
            version: Exact candidate version.

        Returns:
            One exact resolution result per required replica.
        """
        ...

    def mirror_head(self, uri: str, version: int) -> None:
        """Best-effort mirror of the committed catalog to the convenience HEAD tag.

        Args:
            uri: Published dataset URI.
            version: Published exact version.
        """
        ...


class PublicationRepository(Protocol):
    """Atomic serving-catalog transaction required by publication."""

    def publish_serve(
        self,
        claim: WorkClaim,
        candidate_lance_uri: str,
        indexed_lance_version: int,
        artifact_manifest_uri: str,
        artifact_digest: bytes,
    ) -> bool:
        """Compare-and-swap the exact target catalog under the claim fence.

        Args:
            claim: Current fenced work claim.
            candidate_lance_uri: Persisted candidate URI.
            indexed_lance_version: Exact validated version.
            artifact_manifest_uri: Immutable manifest URI.
            artifact_digest: Canonical manifest digest.

        Returns:
            True for committed or idempotently reconciled publication.
        """
        ...


@dataclass(frozen=True, slots=True)
class PrewarmResult:
    """Exact resolution reported by one required serving replica."""

    replica: str
    lance_uri: str
    lance_version: int


def validate_prewarm(results: tuple[PrewarmResult, ...], candidate_lance_uri: str, indexed_lance_version: int) -> None:
    """Require every configured replica to confirm the same exact candidate.

    Args:
        results: Required replica resolutions.
        candidate_lance_uri: Expected URI.
        indexed_lance_version: Expected exact version.

    Raises:
        ValueError: If no replicas responded, names repeat, or any resolution differs.
    """
    if not results:
        raise ValueError("publication requires at least one serving replica prewarm")
    replicas = [result.replica for result in results]
    if len(replicas) != len(set(replicas)):
        raise ValueError("prewarm results contain duplicate replicas")
    mismatched = [
        result.replica
        for result in results
        if result.lance_uri != candidate_lance_uri or result.lance_version != indexed_lance_version
    ]
    if mismatched:
        raise ValueError(f"serving replicas resolved a different publication candidate: {sorted(mismatched)}")


@dataclass(frozen=True, slots=True)
class PublicationCoordinator:
    """Converge immutable pin, artifact, prewarm, catalog CAS, and HEAD mirror."""

    repository: PublicationRepository
    artifacts: ImmutableArtifactStore
    runtime: PublicationRuntime

    def publish(self, claim: WorkClaim, manifest: ArtifactManifest) -> bool:
        """Publish one exact validated candidate idempotently under a durable fence.

        Args:
            claim: Current SERVE or REBUILD claim.
            manifest: Exact candidate evidence produced on an executor.

        Returns:
            True when the serving catalog is durably committed, false for a stale fence.
        """
        if manifest.work_id != str(claim.work_id) or manifest.target_id != str(claim.target_id):
            raise ValueError("artifact manifest does not belong to the claimed work generation")
        payload = manifest.canonical_bytes()
        digest = manifest.digest()
        manifest_uri = manifest.content_uri()
        self.runtime.pin_candidate(
            manifest.candidate_lance_uri,
            manifest.indexed_lance_version,
            candidate_pin_name(claim.work_id),
        )
        self.artifacts.put_if_absent(manifest_uri, payload, digest)
        results = self.runtime.prewarm(manifest.candidate_lance_uri, manifest.indexed_lance_version)
        validate_prewarm(results, manifest.candidate_lance_uri, manifest.indexed_lance_version)
        published = self.repository.publish_serve(
            claim,
            manifest.candidate_lance_uri,
            manifest.indexed_lance_version,
            manifest_uri,
            digest,
        )
        if not published:
            return False
        try:
            self.runtime.mirror_head(manifest.candidate_lance_uri, manifest.indexed_lance_version)
        except Exception:
            return True
        return True
