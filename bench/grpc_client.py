"""gRPC client utilities for the Rust search service.

Client stubs are generated at runtime with ``grpcio-tools`` from the repository's proto file. The proto is copied flat
into a generation directory before compilation because its natural package path (``lance_etl/search/v1``) would
collide with the installed ``lance_etl`` Python package; the flattened modules (``search_pb2`` / ``search_pb2_grpc``)
are imported off ``sys.path`` instead. This is simpler and more deterministic than server reflection, which would make
the benchmark depend on the server having reflection enabled.

The proto currently has no Prewarm rpc; :func:`prewarm_dataset` is the clearly named hook to fill in when one lands,
and the search phase always records cold-vs-warm first-query latency regardless.
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

import numpy as np

from bench.config import PROTO_PATH


def generate_stubs(gen_dir: Path) -> Path:
    """Compile the search proto into Python stubs under a generation directory.

    Args:
        gen_dir: Directory receiving ``search_pb2.py`` and ``search_pb2_grpc.py``.

    Returns:
        The generation directory.

    Raises:
        RuntimeError: If protoc fails.
        FileNotFoundError: If the repository proto file is missing.
    """
    from grpc_tools import protoc

    if not PROTO_PATH.exists():
        raise FileNotFoundError(f"search proto not found at {PROTO_PATH}")
    gen_dir.mkdir(parents=True, exist_ok=True)
    proto_dir: Path = gen_dir / "proto"
    proto_dir.mkdir(exist_ok=True)
    flat_proto: Path = proto_dir / "search.proto"
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
        The ``search_pb2`` and ``search_pb2_grpc`` modules.
    """
    if str(gen_dir) not in sys.path:
        sys.path.insert(0, str(gen_dir))
    pb2: ModuleType = importlib.import_module("search_pb2")
    pb2_grpc: ModuleType = importlib.import_module("search_pb2_grpc")
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
    import grpc

    channel = grpc.insecure_channel(endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout_seconds)
    except grpc.FutureTimeoutError as error:
        template: str = f"{lance_root}/{{org_id}}/tenant0/ns.lance"
        raise RuntimeError(
            f"search server unreachable at {endpoint}; start it with "
            f'LANCE_ETL_BASE_URI="{template}" SEARCH_API_PORT={endpoint.rsplit(":", 1)[-1]} '
            f"./rust/search-api/target/release/search-api"
        ) from error
    return pb2_grpc.SearchServiceStub(channel)


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
        k: Neighbors to return; 0 inherits the fused k in a hybrid request.
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
        k: Hits to return; 0 inherits the fused k in a hybrid request.
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


def prewarm_dataset(stub: Any, pb2: ModuleType, org_id: str) -> None:
    """Prewarm hook for the search service.

    The proto exposed no Prewarm rpc when these stubs were generated, so this is a documented no-op. When a Prewarm rpc
    is added to ``SearchService``, call it here with ``org_id`` so ``--prewarm`` exercises it before any timing starts;
    the search phase already records cold-vs-warm first-query latency either way.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        org_id: The organization whose dataset should be prewarmed.
    """
    del stub, pb2, org_id
