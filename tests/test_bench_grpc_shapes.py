"""Proto-shape unit tests for the benchmark gRPC request construction, without a server.

Generates Python stubs from the repository proto once per module and proves the public service has only three product
search methods. Storage paths, versions, profiles, index knobs, row identifiers, offsets, Prewarm, and Clusters are
absent. Results carry typed logical identifiers and projections.
"""

from __future__ import annotations

from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import grpc
import numpy as np
import pytest

from bench.config import BenchConfig
from bench.grpc_client import (
    dataset_target,
    generate_stubs,
    load_expected_versions,
    load_stubs,
    result_record_ids,
    text_query,
    timed_call,
    validate_served_version,
    vector_query,
)
from bench.search import run_load_level

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


class TestVersionEvidence:
    """Expected-version evidence is exact-target and decoupled from any credential."""

    def test_expected_publication_is_exact_and_version_mismatch_fails(self, tmp_path: Path) -> None:
        """Evidence must cover exactly every target and every response must match it."""
        expected_path: Path = tmp_path / "expected.json"
        expected_path.write_text('{"tenant0/ns/org0": 17}', encoding="utf-8")
        config = BenchConfig(
            command="search",
            endpoint="127.0.0.1:50051",
            search_expected_versions_path=expected_path,
        )
        assert load_expected_versions(config) == {"org0": 17}
        assert validate_served_version(SimpleNamespace(served_version=17), "org0", 17) == 17
        with pytest.raises(RuntimeError, match="expected published version 17"):
            validate_served_version(SimpleNamespace(served_version=16), "org0", 17)

    def test_every_rpc_has_a_client_deadline(self) -> None:
        """The shared call helper passes a bounded timeout."""
        captured: dict[str, Any] = {}

        def callable_rpc(request: Any, timeout: float) -> str:
            """Capture invocation arguments.

            Args:
                request: Request object.
                timeout: Client deadline.

            Returns:
                Fixed response marker.
            """
            captured.update(request=request, timeout=timeout)
            return "ok"

        response, unused_latency = timed_call(callable_rpc, "request")
        assert response == "ok"
        assert unused_latency >= 0
        assert captured == {
            "request": "request",
            "timeout": 5.0,
        }


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


class FakeRpcError(grpc.RpcError):
    """A minimal ``grpc.RpcError`` stand-in carrying a fixed status code."""

    def __init__(self, code: grpc.StatusCode) -> None:
        """Store the fixed status code this fake error reports.

        Args:
            code: The gRPC status code :meth:`code` returns.
        """
        super().__init__("boom")
        self.status_code = code

    def code(self) -> grpc.StatusCode:
        """Return the fixed status code.

        Returns:
            The status code passed to the constructor.
        """
        return self.status_code


class TestLoad:
    """The fixed in-process load profile preserves publication fencing over a plaintext channel."""

    def test_level_measures_requests(self, pb2: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Every successful load response contributes latency and throughput evidence."""
        config = BenchConfig(
            command="search",
            endpoint="127.0.0.1:50051",
            search_expected_versions_path=tmp_path / "expected.json",
        )
        calls: list[Any] = []

        def vector_search_rpc(request: Any, timeout: float) -> Any:
            """Capture one load request.

            Args:
                request: Typed vector request.
                timeout: Bounded RPC deadline.

            Returns:
                Response fenced to the expected publication.
            """
            calls.append((request, timeout))
            return SimpleNamespace(served_version=17)

        monkeypatch.setattr("bench.search.LOAD_DURATION_SECONDS", 0.001)
        level: dict[str, Any] = run_load_level(
            SimpleNamespace(VectorSearch=vector_search_rpc),
            pb2,
            config,
            np.asarray([1.0, 2.0], dtype=np.float32),
            {"org0": 17},
            2,
        )

        assert level["concurrency"] == 2
        assert level["requests"] == len(calls)
        assert level["requests"] >= 2
        assert level["qps"] > 0
        assert all(call[0].target.org_id == "org0" for call in calls)
        assert all(call[1] == 5.0 for call in calls)

    def test_version_failure_propagates(self, pb2: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A load response from an unapproved version fails the benchmark."""
        config = BenchConfig(
            command="search",
            endpoint="127.0.0.1:50051",
            search_expected_versions_path=tmp_path / "expected.json",
        )

        def vector_search_rpc(request: Any, timeout: float) -> Any:
            """Return an unfenced response.

            Args:
                request: Typed vector request.
                timeout: Bounded RPC deadline.

            Returns:
                Response from the wrong publication.
            """
            del request, timeout
            return SimpleNamespace(served_version=16)

        monkeypatch.setattr("bench.search.LOAD_DURATION_SECONDS", 0.0)
        with pytest.raises(RuntimeError, match="expected published version 17"):
            run_load_level(
                SimpleNamespace(VectorSearch=vector_search_rpc),
                pb2,
                config,
                np.asarray([1.0, 2.0], dtype=np.float32),
                {"org0": 17},
                1,
            )

    def test_resource_exhausted_is_recorded_as_failed(
        self, pb2: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The server's admission rejection is recorded as a benign FAILED level."""
        config = BenchConfig(
            command="search",
            endpoint="127.0.0.1:50051",
            search_expected_versions_path=tmp_path / "expected.json",
        )

        def vector_search_rpc(request: Any, timeout: float) -> Any:
            """Raise the server's admission rejection.

            Args:
                request: Typed vector request.
                timeout: Bounded RPC deadline.

            Raises:
                FakeRpcError: Always, tagged ``RESOURCE_EXHAUSTED``.
            """
            del request, timeout
            raise FakeRpcError(grpc.StatusCode.RESOURCE_EXHAUSTED)

        monkeypatch.setattr("bench.search.LOAD_DURATION_SECONDS", 0.0)
        level: dict[str, Any] = run_load_level(
            SimpleNamespace(VectorSearch=vector_search_rpc),
            pb2,
            config,
            np.asarray([1.0, 2.0], dtype=np.float32),
            {"org0": 17},
            1,
        )
        assert level["status"] == "FAILED"
        assert "boom" in level["reason"]

    def test_other_rpc_error_propagates(self, pb2: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-admission gRPC failure (for example a mid-load server crash) is never swallowed."""
        config = BenchConfig(
            command="search",
            endpoint="127.0.0.1:50051",
            search_expected_versions_path=tmp_path / "expected.json",
        )

        def vector_search_rpc(request: Any, timeout: float) -> Any:
            """Raise a transport failure unrelated to admission control.

            Args:
                request: Typed vector request.
                timeout: Bounded RPC deadline.

            Raises:
                FakeRpcError: Always, tagged ``UNAVAILABLE``.
            """
            del request, timeout
            raise FakeRpcError(grpc.StatusCode.UNAVAILABLE)

        monkeypatch.setattr("bench.search.LOAD_DURATION_SECONDS", 0.0)
        with pytest.raises(FakeRpcError):
            run_load_level(
                SimpleNamespace(VectorSearch=vector_search_rpc),
                pb2,
                config,
                np.asarray([1.0, 2.0], dtype=np.float32),
                {"org0": 17},
                1,
            )


class TestResultParsing:
    """Result rows decode back to global record ids."""

    def test_result_record_ids_reads_string_values(self, pb2: ModuleType) -> None:
        """Typed record_id strings parse to the integer corpus identities."""
        results: list[Any] = [
            pb2.VectorSearchResult(record_id="7", distance=0.1),
            pb2.VectorSearchResult(record_id="11", distance=0.2),
        ]
        ids: np.ndarray = result_record_ids(results)
        assert ids.tolist() == [7, 11]
        assert ids.dtype == np.int64

    def test_result_record_ids_missing_field_raises(self, pb2: ModuleType) -> None:
        """An empty required logical identifier raises instead of being silently dropped."""
        results: list[Any] = [
            pb2.VectorSearchResult(record_id="7", distance=0.1),
            pb2.VectorSearchResult(record_id="", distance=0.2),
        ]
        with pytest.raises(ValueError, match="record_id"):
            result_record_ids(results)

    def test_result_record_ids_mistyped_field_raises(self, pb2: ModuleType) -> None:
        """A non-numeric product identifier is rejected by integer-corpus recall scoring."""
        results: list[Any] = [pb2.VectorSearchResult(record_id="not-an-integer", distance=0.1)]
        with pytest.raises(ValueError, match="record_id"):
            result_record_ids(results)
