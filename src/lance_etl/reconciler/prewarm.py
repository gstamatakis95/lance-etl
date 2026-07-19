"""Local exact-version publication prewarm."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import lance
from pyspark.sql import SparkSession

from lance_etl.publication.workflow import PrewarmResult, validate_prewarm
from lance_etl.state import RoutingIdentity


class ExactPrewarmer(Protocol):
    """Open and warm one exact local candidate before publication."""

    def prewarm(
        self,
        identity: RoutingIdentity,
        candidate_uri: str,
        candidate_version: int,
    ) -> tuple[PrewarmResult, ...]:
        """Warm and verify one exact candidate.

        Args:
            identity: Validated logical dataset route.
            candidate_uri: Exact immutable candidate URI.
            candidate_version: Exact immutable candidate version.

        Returns:
            Exact-version evidence from the local executor.
        """
        ...


@dataclass(frozen=True, slots=True)
class LocalExactVersionPrewarmer:
    """Verify one local candidate on a Spark executor without a search service."""

    spark: SparkSession

    def prewarm(
        self,
        identity: RoutingIdentity,
        candidate_uri: str,
        candidate_version: int,
    ) -> tuple[PrewarmResult, ...]:
        """Open and inspect one exact candidate on a local Spark executor.

        Args:
            identity: Validated logical dataset route.
            candidate_uri: Exact immutable candidate URI.
            candidate_version: Exact immutable candidate version.

        Returns:
            One local exact-version result.

        Raises:
            RuntimeError: If the candidate cannot be opened at the requested version.
        """
        identity.validate()
        if not candidate_uri or candidate_version < 1:
            raise RuntimeError("local prewarm requires a candidate URI and positive exact version")

        def inspect_candidate(item: tuple[str, int]) -> PrewarmResult:
            """Open an exact candidate and warm its index metadata.

            Args:
                item: Candidate URI and exact version.

            Returns:
                Verified local exact-version evidence.
            """
            uri: str
            version: int
            uri, version = item
            dataset: lance.LanceDataset = lance.dataset(uri, version=version)
            if dataset.version != version or dataset.uri.rstrip("/") != uri.rstrip("/"):
                raise RuntimeError("local prewarm resolved a different candidate")
            dataset.describe_indices()
            return PrewarmResult("local", uri, version)

        try:
            results: tuple[PrewarmResult, ...] = tuple(
                self.spark.sparkContext.parallelize([(candidate_uri, candidate_version)], 1)
                .map(inspect_candidate)
                .collect()
            )
            validate_prewarm(results, candidate_uri, candidate_version)
        except Exception as error:
            raise RuntimeError("local exact-version prewarm failed") from error
        if len(results) != 1:
            raise RuntimeError("local exact-version prewarm returned invalid evidence")
        return results
