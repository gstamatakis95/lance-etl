"""gRPC client utilities for the Rust search service.

Client stubs are generated at runtime with ``grpcio-tools`` from the repository's single proto file. The proto is
copied flat into a generation directory before compilation because its natural package path (``lance_etl/v1``) would
collide with the installed ``lance_etl`` Python package. The flattened modules (``lance_etl_pb2`` /
``lance_etl_pb2_grpc``) are imported off ``sys.path`` instead. This is simpler and more deterministic than server
reflection, which would make the benchmark depend on the server having reflection enabled. The proto carries the
``SearchService`` contract exercised by the benchmark.

Every request addresses a logical ``DatasetTarget`` built by :func:`dataset_target`. The service resolves its exact
URI, Lance version, and release profile through the serving catalog. The public client cannot select storage paths,
versions, tags, index execution knobs, or administrative cache operations.
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
) -> Any:
    """Build a ``VectorQuery`` message.

    Args:
        pb2: The generated proto module.
        vector: The query vector.

    Returns:
        Query semantics containing only the vector. Execution policy is catalog-owned.
    """
    return pb2.VectorQuery(vector=[float(value) for value in vector])


def text_query(pb2: ModuleType, terms: str, columns: tuple[str, ...] = ("text",)) -> Any:
    """Build a simple ``TextQuery`` message over the ``text`` column.

    Args:
        pb2: The generated proto module.
        terms: The space-separated query terms.
        columns: Allowlisted text columns selected by product semantics.

    Returns:
        The populated message.
    """
    query = pb2.TextQuery(simple=terms)
    query.columns.extend(columns)
    return query


def result_vector_ids(results: Any) -> np.ndarray:
    """Extract the ``vector_id`` column from search results as global int ids.

    The release API carries the stable logical identifier as a required typed string rather than a
    generic row struct. An empty or non-numeric identifier is contract drift for these integer-id
    corpora and fails loudly instead of silently deflating recall.

    Args:
        results: Repeated typed result messages.

    Returns:
        An int64 array of global vector ids in result order, one entry per input result.

    Raises:
        ValueError: If any result has an empty or non-numeric ``vector_id``.
    """
    ids: list[int] = []
    malformed: list[str] = []
    total: int = 0
    for result in results:
        try:
            ids.append(int(result.vector_id))
        except (TypeError, ValueError):
            malformed.append(f"result[{total}] vector_id={result.vector_id!r}")
        total += 1
    if malformed:
        raise ValueError(
            f"{len(malformed)} of {total} results carried an empty or non-numeric vector_id. Offenders: {malformed[:5]}"
        )
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


def vector_search(
    stub: Any,
    pb2: ModuleType,
    org_id: str,
    query: np.ndarray,
    k: int,
) -> tuple[Any, float]:
    """Run vector search through the current catalog publication.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        org_id: Organization to query.
        query: Query vector.
        k: Bounded result count.

    Returns:
        Response and client-side latency in milliseconds.
    """
    request = pb2.VectorSearchRequest(
        target=dataset_target(pb2, org_id),
        query=vector_query(pb2, query),
        k=k,
    )
    return timed_call(stub.VectorSearch, request)


def text_search(
    stub: Any,
    pb2: ModuleType,
    org_id: str,
    terms: str,
    k: int,
) -> tuple[Any, float]:
    """Run text search through the current catalog publication.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        org_id: Organization to query.
        terms: Simple product query terms.
        k: Bounded result count.

    Returns:
        Response and client-side latency in milliseconds.
    """
    request = pb2.TextSearchRequest(
        target=dataset_target(pb2, org_id),
        query=text_query(pb2, terms),
        k=k,
    )
    return timed_call(stub.TextSearch, request)


def hybrid_search(
    stub: Any,
    pb2: ModuleType,
    org_id: str,
    query: np.ndarray,
    terms: str,
    k: int,
) -> tuple[Any, float]:
    """Run hybrid search through the current catalog publication.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        org_id: The organization to query.
        query: The query vector for the vector leg.
        terms: The space-separated query terms for the text leg.
        k: Fused hits to return.

    Returns:
        Response and client-side latency in milliseconds.
    """
    request = pb2.HybridSearchRequest(
        target=dataset_target(pb2, org_id),
        vector=vector_query(pb2, query),
        text=text_query(pb2, terms),
        k=k,
    )
    return timed_call(stub.HybridSearch, request)
