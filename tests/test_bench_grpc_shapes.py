"""Proto-shape unit tests for the benchmark gRPC request construction, without a server.

Generates Python stubs from the repository proto once per module and proves the public service has only three product
search methods. Storage paths, versions, profiles, index knobs, row identifiers, offsets, Prewarm, and Clusters are
absent. Results carry typed logical identifiers and projections.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType
from typing import Any

import numpy as np
import pytest

from bench.grpc_client import dataset_target, generate_stubs, load_stubs, result_vector_ids, text_query, vector_query
from bench.search import ghz_payload

EXPECTED_RPCS: frozenset[str] = frozenset({"VectorSearch", "TextSearch", "HybridSearch"})


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
    """The generated service exposes only product search methods."""

    def test_service_has_all_rpcs(self, pb2: ModuleType) -> None:
        """The SearchService descriptor lists exactly vector, text, and hybrid search."""
        service = pb2.DESCRIPTOR.services_by_name["SearchService"]
        assert set(service.methods_by_name) == EXPECTED_RPCS

    def test_stub_class_has_rpc_attributes(self, stubs: tuple[ModuleType, ModuleType]) -> None:
        """The servicer base class carries one handler per rpc."""
        unused_pb2, pb2_grpc = stubs
        for rpc in EXPECTED_RPCS:
            assert hasattr(pb2_grpc.SearchServiceServicer, rpc)

    def test_intake_service_cannot_return(self, pb2: ModuleType) -> None:
        """The generated descriptor proves the removed Intake service is absent."""
        assert "IntakeService" not in pb2.DESCRIPTOR.services_by_name

    def test_admin_messages_cannot_return(self, pb2: ModuleType) -> None:
        """Public descriptors contain no cache warming or index-geometry messages."""
        messages = pb2.DESCRIPTOR.message_types_by_name
        assert "PrewarmRequest" not in messages
        assert "PrewarmResponse" not in messages
        assert "ClustersRequest" not in messages
        assert "ClustersResponse" not in messages


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
    """Search requests expose product semantics while profiles own execution policy."""

    def test_vector_search_request(self, pb2: ModuleType) -> None:
        """Vector k and projection belong to the request while the query contains only vector semantics."""
        query: Any = vector_query(pb2, np.asarray([1.0, 2.0], dtype=np.float32))
        request: Any = pb2.VectorSearchRequest(
            target=dataset_target(pb2, "org1"), query=query, k=10, projection=["category"]
        )
        assert request.target.org_id == "org1"
        assert list(request.query.vector) == [1.0, 2.0]
        assert request.k == 10
        assert list(request.projection) == ["category"]
        assert set(request.query.DESCRIPTOR.fields_by_name) == {"vector"}

    def test_request_has_no_storage_version_profile_or_execution_selectors(self, pb2: ModuleType) -> None:
        """No public request can bypass catalog resolution or choose an index execution plan."""
        forbidden = {
            "uri",
            "version",
            "tag",
            "profile",
            "nprobes",
            "refine_factor",
            "fast_search",
            "bypass_vector_index",
            "offset",
            "with_row_id",
        }
        for message_name in ("VectorQuery", "TextQuery", "VectorSearchRequest", "TextSearchRequest"):
            fields = set(pb2.DESCRIPTOR.message_types_by_name[message_name].fields_by_name)
            assert fields.isdisjoint(forbidden)

    def test_text_search_request(self, pb2: ModuleType) -> None:
        """TextSearchRequest carries the target and the simple text query."""
        request: Any = pb2.TextSearchRequest(
            target=dataset_target(pb2, "org0"), query=text_query(pb2, "alpha beta"), k=7
        )
        assert request.target.namespace == "ns"
        assert request.query.simple == "alpha beta"
        assert request.k == 7
        assert list(request.query.columns) == ["text"]

    def test_hybrid_search_request(self, pb2: ModuleType) -> None:
        """HybridSearchRequest carries one bounded k and a product fusion mode."""
        request: Any = pb2.HybridSearchRequest(
            target=dataset_target(pb2, "org1"),
            vector=vector_query(pb2, np.asarray([1.0], dtype=np.float32)),
            text=text_query(pb2, "gamma"),
            k=10,
            fusion_mode=pb2.HYBRID_FUSION_MODE_BALANCED,
        )
        assert request.target.org_id == "org1"
        assert request.k == 10
        assert request.fusion_mode == pb2.HYBRID_FUSION_MODE_BALANCED


class TestGhzPayload:
    """The ghz replay body matches the final request shape."""

    def test_payload_has_target(self) -> None:
        """The body nests the full DatasetTarget and the vector query."""
        payload: dict[str, Any] = ghz_payload(np.asarray([1.0, 2.0], dtype=np.float32))
        assert payload["target"] == {"org_id": "org0", "tenant_id": "tenant0", "namespace": "ns"}
        assert payload["query"]["vector"] == [1.0, 2.0]
        assert payload["k"] == 10
        assert set(payload["query"]) == {"vector"}
        assert "org_id" not in payload


class TestResultParsing:
    """Result rows decode back to global vector ids."""

    def test_result_vector_ids_reads_string_values(self, pb2: ModuleType) -> None:
        """Typed vector_id strings parse to the integer corpus identities."""
        results: list[Any] = [
            pb2.VectorSearchResult(vector_id="7", distance=0.1),
            pb2.VectorSearchResult(vector_id="11", distance=0.2),
        ]
        ids: np.ndarray = result_vector_ids(results)
        assert ids.tolist() == [7, 11]
        assert ids.dtype == np.int64

    def test_result_vector_ids_missing_field_raises(self, pb2: ModuleType) -> None:
        """An empty required logical identifier raises instead of being silently dropped."""
        results: list[Any] = [
            pb2.VectorSearchResult(vector_id="7", distance=0.1),
            pb2.VectorSearchResult(vector_id="", distance=0.2),
        ]
        with pytest.raises(ValueError, match="vector_id"):
            result_vector_ids(results)

    def test_result_vector_ids_mistyped_field_raises(self, pb2: ModuleType) -> None:
        """A non-numeric product identifier is rejected by integer-corpus recall scoring."""
        results: list[Any] = [pb2.VectorSearchResult(vector_id="not-an-integer", distance=0.1)]
        with pytest.raises(ValueError, match="vector_id"):
            result_vector_ids(results)
