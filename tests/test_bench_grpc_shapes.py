"""Proto-shape unit tests for the benchmark gRPC request construction, without a server.

Generates the Python stubs from the single repository proto once per module and asserts that every request the search
phase builds carries the final surface: a ``DatasetTarget`` (org, fixed tenant, fixed namespace, no date range) on all
five rpcs, the ``Prewarm`` and ``Clusters`` rpcs on the service descriptor, and the protojson body ghz replays.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import pytest
from google.protobuf import struct_pb2

from bench.grpc_client import dataset_target, generate_stubs, load_stubs, result_vector_ids, text_query, vector_query
from bench.search import ghz_payload

EXPECTED_RPCS: frozenset[str] = frozenset({"VectorSearch", "TextSearch", "HybridSearch", "Prewarm", "Clusters"})


@pytest.fixture(scope="module")
def stubs(tmp_path_factory: pytest.TempPathFactory) -> tuple[ModuleType, ModuleType]:
    """Generate and import the proto stubs once for the module.

    Args:
        tmp_path_factory: The pytest temporary-directory factory.

    Returns:
        The ``lance_etl_pb2`` and ``lance_etl_pb2_grpc`` modules.
    """
    gen_dir: Path = tmp_path_factory.mktemp("grpc_gen")
    return load_stubs(generate_stubs(gen_dir))


@pytest.fixture(scope="module")
def pb2(stubs: tuple[ModuleType, ModuleType]) -> ModuleType:
    """Return the generated proto message module.

    Args:
        stubs: The generated stub modules.

    Returns:
        The ``lance_etl_pb2`` module.
    """
    return stubs[0]


class TestServiceSurface:
    """The generated service exposes the final five-rpc surface."""

    def test_service_has_all_rpcs(self, pb2: ModuleType) -> None:
        """The SearchService descriptor lists all five rpcs including Prewarm and Clusters."""
        service = pb2.DESCRIPTOR.services_by_name["SearchService"]
        assert set(service.methods_by_name) == EXPECTED_RPCS

    def test_stub_class_has_rpc_attributes(self, stubs: tuple[ModuleType, ModuleType]) -> None:
        """The servicer base class carries one handler per rpc."""
        unused_pb2, pb2_grpc = stubs
        for rpc in EXPECTED_RPCS:
            assert hasattr(pb2_grpc.SearchServiceServicer, rpc)


class TestDatasetTarget:
    """The DatasetTarget helper matches what the benchmark ingest writes."""

    def test_fields(self, pb2: ModuleType) -> None:
        """The target carries the org, the fixed tenant, and the fixed namespace."""
        target: Any = dataset_target(pb2, "org3")
        assert target.org_id == "org3"
        assert target.tenant_id == "tenant0"
        assert target.namespace == "ns"

    def test_overrides(self, pb2: ModuleType) -> None:
        """Tenant and namespace can be overridden explicitly."""
        target: Any = dataset_target(pb2, "org0", tenant_id="t9", namespace="other")
        assert (target.tenant_id, target.namespace) == ("t9", "other")


class TestSearchRequests:
    """Every search request message embeds the target and the query parameters."""

    def test_vector_search_request(self, pb2: ModuleType) -> None:
        """VectorSearchRequest carries the target and a fully populated VectorQuery."""
        query: Any = vector_query(pb2, np.asarray([1.0, 2.0], dtype=np.float32), 10, 5, 2)
        request: Any = pb2.VectorSearchRequest(target=dataset_target(pb2, "org1"), query=query)
        assert request.target.org_id == "org1"
        assert list(request.query.vector) == [1.0, 2.0]
        assert request.query.k == 10
        assert request.query.column == "vector"
        assert request.query.nprobes == 5
        assert request.query.refine_factor == 2
        assert list(request.query.projection) == ["vector_id"]

    def test_vector_query_optionals_left_unset(self, pb2: ModuleType) -> None:
        """Nprobes and refine_factor stay absent when not requested."""
        query: Any = vector_query(pb2, np.asarray([0.0], dtype=np.float32), 1, None, None)
        assert not query.HasField("nprobes")
        assert not query.HasField("refine_factor")

    def test_text_search_request(self, pb2: ModuleType) -> None:
        """TextSearchRequest carries the target and the simple text query."""
        request: Any = pb2.TextSearchRequest(target=dataset_target(pb2, "org0"), query=text_query(pb2, "alpha beta", 7))
        assert request.target.namespace == "ns"
        assert request.query.simple == "alpha beta"
        assert request.query.k == 7
        assert list(request.query.columns) == ["text"]

    def test_hybrid_search_request(self, pb2: ModuleType) -> None:
        """HybridSearchRequest carries the target and both legs with inherited k."""
        request: Any = pb2.HybridSearchRequest(
            target=dataset_target(pb2, "org1"),
            vector=vector_query(pb2, np.asarray([1.0], dtype=np.float32), 0, 10, None),
            text=text_query(pb2, "gamma", 0),
            k=10,
        )
        assert request.target.org_id == "org1"
        assert request.vector.k == 0
        assert request.text.k == 0
        assert request.k == 10


class TestPrewarmAndClusters:
    """The Prewarm and Clusters request messages follow the final proto."""

    def test_prewarm_request(self, pb2: ModuleType) -> None:
        """A full-warm PrewarmRequest carries the target, metadata, and all_indexes."""
        request: Any = pb2.PrewarmRequest(
            target=dataset_target(pb2, "org0"), metadata=True, all_indexes=True, fts_with_position=True
        )
        assert request.target.tenant_id == "tenant0"
        assert request.metadata is True
        assert request.all_indexes is True
        assert request.fts_with_position is True
        assert list(request.index_names) == []

    def test_clusters_request_default_index(self, pb2: ModuleType) -> None:
        """ClustersRequest leaves the optional index_name absent by default."""
        request: Any = pb2.ClustersRequest(target=dataset_target(pb2, "org1"))
        assert request.target.org_id == "org1"
        assert not request.HasField("index_name")

    def test_clusters_request_named_index(self, pb2: ModuleType) -> None:
        """ClustersRequest can name an explicit index."""
        request: Any = pb2.ClustersRequest(target=dataset_target(pb2, "org1"), index_name="vector_idx")
        assert request.HasField("index_name")
        assert request.index_name == "vector_idx"

    def test_prewarm_response_shape(self, pb2: ModuleType) -> None:
        """PrewarmResponse exposes the fields the prewarm summary reads."""
        response: Any = pb2.PrewarmResponse(
            metadata_warmed=True,
            metadata_duration_ms=3,
            total_duration_ms=11,
            index_cache_size_bytes=42,
            indexes=[pb2.PrewarmedIndex(name="vector_idx", duration_ms=8, error="")],
        )
        assert response.metadata_warmed is True
        assert response.indexes[0].name == "vector_idx"

    def test_clusters_response_shape(self, pb2: ModuleType) -> None:
        """ClustersResponse exposes clusters, dimension, index_name, and num_partitions."""
        response: Any = pb2.ClustersResponse(
            clusters=[pb2.Cluster(id=0, centroid=[0.5, 1.5])],
            dimension=2,
            index_name="vector_idx",
            num_partitions=1,
        )
        assert len(response.clusters) == response.num_partitions
        assert len(response.clusters[0].centroid) == response.dimension


class TestGhzPayload:
    """The ghz replay body matches the final request shape."""

    def test_payload_has_target(self) -> None:
        """The body nests the full DatasetTarget and the vector query."""
        payload: dict[str, Any] = ghz_payload(np.asarray([1.0, 2.0], dtype=np.float32), 10)
        assert payload["target"] == {"org_id": "org0", "tenant_id": "tenant0", "namespace": "ns"}
        assert payload["query"]["vector"] == [1.0, 2.0]
        assert payload["query"]["nprobes"] == 10
        assert payload["query"]["projection"] == ["vector_id"]
        assert "org_id" not in payload


class TestResultParsing:
    """Result rows decode back to global vector ids."""

    def test_result_vector_ids_reads_string_values(self, pb2: ModuleType) -> None:
        """vector_id arrives as a string struct value and parses to int64."""
        results: list[Any] = []
        for value in ("7", "11"):
            row = struct_pb2.Struct()
            row.fields["vector_id"].string_value = value
            results.append(pb2.VectorSearchResult(row=row, distance=0.1))
        ids: np.ndarray = result_vector_ids(results)
        assert ids.tolist() == [7, 11]
        assert ids.dtype == np.int64
