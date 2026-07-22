"""Tests for local exact-version publication prewarming."""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pytest

import lance_etl.reconciler.prewarm as prewarm_module
from lance_etl.publication.workflow import PrewarmResult
from lance_etl.reconciler.prewarm import LocalExactVersionPrewarmer
from lance_etl.state import RoutingIdentity


def executing_spark() -> MagicMock:
    """Build a Spark fixture that executes the one-item RDD map locally.

    Returns:
        Spark mock preserving the executor seam.
    """
    spark: MagicMock = MagicMock()

    def parallelize(values: list[tuple[str, int]], partitions: int) -> MagicMock:
        """Create an immediate one-partition RDD fixture.

        Args:
            values: Candidate URI and exact-version tasks.
            partitions: Requested partition count.

        Returns:
            RDD mock that executes its mapping function immediately.
        """
        assert partitions == 1
        rdd: MagicMock = MagicMock()

        def map_values(mapper: Callable[[tuple[str, int]], PrewarmResult]) -> MagicMock:
            """Execute the candidate inspector against every fixture value.

            Args:
                mapper: Local candidate inspection function.

            Returns:
                RDD mock with collected results.
            """
            rdd.collect.return_value = [mapper(value) for value in values]
            return rdd

        rdd.map.side_effect = map_values
        return rdd

    spark.sparkContext.parallelize.side_effect = parallelize
    return spark


def test_local_prewarm_opens_exact_version_and_describes_indexes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A local executor returns evidence only after opening and warming the exact candidate.

    Args:
        monkeypatch: Scoped Lance dataset replacement.
    """
    dataset: MagicMock = MagicMock(version=7, uri="/tmp/candidate.lance")
    open_dataset: MagicMock = MagicMock(return_value=dataset)
    monkeypatch.setattr(prewarm_module.lance, "dataset", open_dataset)
    prewarmer: LocalExactVersionPrewarmer = LocalExactVersionPrewarmer(executing_spark())

    results: tuple[PrewarmResult, ...] = prewarmer.prewarm(
        RoutingIdentity("tenant1", "namespace1", "org1"),
        "/tmp/candidate.lance",
        7,
    )

    assert results == (PrewarmResult("local", "/tmp/candidate.lance", 7),)
    open_dataset.assert_called_once_with("/tmp/candidate.lance", version=7)
    dataset.describe_indices.assert_called_once_with()


@pytest.mark.parametrize(("candidate_uri", "candidate_version"), (("", 1), ("/tmp/candidate.lance", 0)))
def test_local_prewarm_rejects_incomplete_candidate(candidate_uri: str, candidate_version: int) -> None:
    """Local prewarm rejects an absent URI or non-positive exact version.

    Args:
        candidate_uri: Candidate URI fixture.
        candidate_version: Candidate version fixture.
    """
    prewarmer: LocalExactVersionPrewarmer = LocalExactVersionPrewarmer(executing_spark())
    with pytest.raises(RuntimeError, match="candidate URI and positive exact version"):
        prewarmer.prewarm(RoutingIdentity("tenant1", "namespace1", "org1"), candidate_uri, candidate_version)


def test_local_prewarm_rejects_different_resolved_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    """A mismatched resolved URI or version fails the local publication gate.

    Args:
        monkeypatch: Scoped Lance dataset replacement.
    """
    monkeypatch.setattr(
        prewarm_module.lance,
        "dataset",
        MagicMock(return_value=MagicMock(version=8, uri="/tmp/other.lance")),
    )
    prewarmer: LocalExactVersionPrewarmer = LocalExactVersionPrewarmer(executing_spark())
    with pytest.raises(RuntimeError, match="local prewarm resolved a different candidate"):
        prewarmer.prewarm(
            RoutingIdentity("tenant1", "namespace1", "org1"),
            "/tmp/candidate.lance",
            7,
        )
