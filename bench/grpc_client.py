"""gRPC client utilities for the Rust search service.

Client stubs are generated at runtime with ``grpcio-tools`` from the repository's single proto file. The proto is
copied flat into a generation directory before compilation because its natural package path (``lance_etl/v1``) would
collide with the installed ``lance_etl`` Python package. The flattened modules (``lance_etl_pb2`` /
``lance_etl_pb2_grpc``) are imported off ``sys.path`` instead. This is simpler and more deterministic than server
reflection, which would make the benchmark depend on the server having reflection enabled. The one proto carries both
the ``SearchService`` and the ``IntakeService``; the benchmark only drives the search side.

Every request message addresses its dataset through a ``DatasetTarget`` built by :func:`dataset_target`, matching the
``{base}/{org_id}/{tenant_id}/{namespace}.lance`` layout the benchmark ingest phase writes. :func:`prewarm_dataset`
drives the real ``Prewarm`` rpc and returns the server-reported warm-up timings.
"""

from __future__ import annotations

import importlib
import shutil
import sys
import time
from importlib.resources import files as resource_files
from pathlib import Path
from types import ModuleType
from typing import Any

import grpc
import numpy as np
from grpc_tools import protoc

from bench.config import NAMESPACE, PROTO_PATH, TENANT_ID


def generate_stubs(gen_dir: Path) -> Path:
    """Compile the lance_etl proto into Python stubs under a generation directory.

    Args:
        gen_dir: Directory receiving ``lance_etl_pb2.py`` and ``lance_etl_pb2_grpc.py``.

    Returns:
        The generation directory.

    Raises:
        RuntimeError: If protoc fails.
        FileNotFoundError: If the repository proto file is missing.
    """
    if not PROTO_PATH.exists():
        raise FileNotFoundError(f"lance_etl proto not found at {PROTO_PATH}")
    gen_dir.mkdir(parents=True, exist_ok=True)
    proto_dir: Path = gen_dir / "proto"
    proto_dir.mkdir(exist_ok=True)
    flat_proto: Path = proto_dir / "lance_etl.proto"
    shutil.copyfile(PROTO_PATH, flat_proto)
    include: str = str(resource_files("grpc_tools") / "_proto")
    arguments: list[str] = [
        "protoc",
        f"-I{proto_dir}",
        f"-I{include}",
        f"--python_out={gen_dir}",
        f"--grpc_python_out={gen_dir}",
        str(flat_proto),
    ]
    if protoc.main(arguments) != 0:
        raise RuntimeError(f"grpc_tools.protoc failed for {flat_proto}")
    return gen_dir


def load_stubs(gen_dir: Path) -> tuple[ModuleType, ModuleType]:
    """Import the generated proto and service stub modules.

    Args:
        gen_dir: The generation directory produced by :func:`generate_stubs`.

    Returns:
        The ``lance_etl_pb2`` and ``lance_etl_pb2_grpc`` modules.
    """
    if str(gen_dir) not in sys.path:
        sys.path.insert(0, str(gen_dir))
    pb2: ModuleType = importlib.import_module("lance_etl_pb2")
    pb2_grpc: ModuleType = importlib.import_module("lance_etl_pb2_grpc")
    return pb2, pb2_grpc


def open_stub(endpoint: str, pb2_grpc: ModuleType, lance_root: str, timeout_seconds: float = 5.0) -> Any:
    """Open an insecure channel to the search service and wait for readiness.

    Args:
        endpoint: ``host:port`` of the server.
        pb2_grpc: The generated service stub module.
        lance_root: The Lance base directory, used in the error guidance.
        timeout_seconds: Readiness wait budget.

    Returns:
        A ready ``SearchServiceStub``.

    Raises:
        RuntimeError: If the server is unreachable, with instructions to start it.
    """
    channel = grpc.insecure_channel(endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout_seconds)
    except grpc.FutureTimeoutError as error:
        raise RuntimeError(
            f"search server unreachable at {endpoint}; start it with "
            f'LANCE_ETL_BASE_URI="{lance_root}" SEARCH_API_PORT={endpoint.rsplit(":", 1)[-1]} '
            f"./rust/search-api/target/release/search-api"
        ) from error
    return pb2_grpc.SearchServiceStub(channel)


def dataset_target(pb2: ModuleType, org_id: str, tenant_id: str = TENANT_ID, namespace: str = NAMESPACE) -> Any:
    """Build the ``DatasetTarget`` addressing one per-tenant benchmark dataset.

    The server resolves the target to ``{LANCE_ETL_BASE_URI}/{org_id}/{tenant_id}/{namespace}.lance``, which is
    exactly the layout the benchmark ingest phase writes (see ``BenchConfig.dataset_uris``).

    Args:
        pb2: The generated proto module.
        org_id: The organization id.
        tenant_id: The tenant id. Defaults to the fixed benchmark tenant.
        namespace: The namespace. Defaults to the fixed benchmark namespace.

    Returns:
        The populated ``DatasetTarget`` message.
    """
    return pb2.DatasetTarget(org_id=org_id, tenant_id=tenant_id, namespace=namespace)


def vector_query(
    pb2: ModuleType,
    vector: np.ndarray,
    k: int,
    nprobes: int | None,
    refine_factor: int | None,
    column: str = "vector",
    projection: tuple[str, ...] = ("vector_id",),
) -> Any:
    """Build a ``VectorQuery`` message.

    Args:
        pb2: The generated proto module.
        vector: The query vector.
        k: Neighbors to return. 0 inherits the fused k in a hybrid request.
        nprobes: Probed IVF partitions, or ``None`` to leave unset.
        refine_factor: Re-ranking factor, or ``None`` to leave unset.
        column: The vector column name.
        projection: Columns to return.

    Returns:
        The populated message.
    """
    query = pb2.VectorQuery(vector=[float(value) for value in vector], k=k, column=column)
    query.projection.extend(projection)
    if nprobes is not None:
        query.nprobes = nprobes
    if refine_factor is not None:
        query.refine_factor = refine_factor
    return query


def text_query(pb2: ModuleType, terms: str, k: int, projection: tuple[str, ...] = ("vector_id",)) -> Any:
    """Build a simple ``TextQuery`` message over the ``text`` column.

    Args:
        pb2: The generated proto module.
        terms: The space-separated query terms.
        k: Hits to return. 0 inherits the fused k in a hybrid request.
        projection: Columns to return.

    Returns:
        The populated message.
    """
    query = pb2.TextQuery(simple=terms, k=k)
    query.columns.append("text")
    query.projection.extend(projection)
    return query


def result_vector_ids(results: Any) -> np.ndarray:
    """Extract the ``vector_id`` column from search results as global int ids.

    Args:
        results: The repeated result messages, each carrying a ``row`` struct.

    Returns:
        An int64 array of global vector ids in result order.
    """
    ids: list[int] = []
    for result in results:
        field = result.row.fields.get("vector_id")
        if field is not None and field.WhichOneof("kind") == "string_value":
            ids.append(int(field.string_value))
    return np.asarray(ids, dtype=np.int64)


def timed_call(callable_rpc: Any, request: Any) -> tuple[Any, float]:
    """Invoke one RPC and measure its latency.

    Args:
        callable_rpc: The stub method.
        request: The request message.

    Returns:
        The response and the latency in milliseconds.
    """
    started: float = time.perf_counter()
    response: Any = callable_rpc(request)
    return response, (time.perf_counter() - started) * 1000.0


def prewarm_dataset(stub: Any, pb2: ModuleType, org_id: str, fts_with_position: bool = False) -> dict[str, Any]:
    """Prewarm one org's dataset through the real ``Prewarm`` rpc.

    Warms the dataset metadata and every index, and returns the server-reported timings together with the
    client-measured rpc latency.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        org_id: The organization whose dataset is prewarmed.
        fts_with_position: Also pull FTS position data for inverted indexes.

    Returns:
        The prewarm outcome: server-side metadata/total durations, per-index durations and errors, the index cache
        size after the call, and the client-side rpc latency in milliseconds.
    """
    request = pb2.PrewarmRequest(
        target=dataset_target(pb2, org_id), metadata=True, all_indexes=True, fts_with_position=fts_with_position
    )
    response, rpc_ms = timed_call(stub.Prewarm, request)
    return {
        "rpc_ms": round(rpc_ms, 3),
        "metadata_warmed": response.metadata_warmed,
        "metadata_duration_ms": int(response.metadata_duration_ms),
        "total_duration_ms": int(response.total_duration_ms),
        "index_cache_size_bytes": int(response.index_cache_size_bytes),
        "indexes": [
            {"name": entry.name, "duration_ms": int(entry.duration_ms), "error": entry.error}
            for entry in response.indexes
        ],
    }


def fetch_clusters(stub: Any, pb2: ModuleType, org_id: str) -> tuple[Any, float]:
    """Read the IVF cluster centroids of one org's vector index through the ``Clusters`` rpc.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        org_id: The organization whose vector index is read.

    Returns:
        The ``ClustersResponse`` and the rpc latency in milliseconds.
    """
    request = pb2.ClustersRequest(target=dataset_target(pb2, org_id))
    return timed_call(stub.Clusters, request)
