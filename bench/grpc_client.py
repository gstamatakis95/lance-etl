"""gRPC client utilities for the Rust search service.

Client stubs are generated at runtime with ``grpcio-tools`` from the repository's single proto file. The proto is
copied flat into a generation directory before compilation because its natural package path (``lance_etl/v1``) would
collide with the installed ``lance_etl`` Python package. The flattened modules (``lance_etl_pb2`` /
``lance_etl_pb2_grpc``) are imported off ``sys.path`` instead. This is simpler and more deterministic than server
reflection, which would make the benchmark depend on the server having reflection enabled. The proto carries the
``SearchService`` contract exercised by the benchmark.

Every request addresses a logical ``DatasetTarget`` built by :func:`dataset_target`. The service resolves its exact
URI and exact Lance version through the serving catalog. The public client cannot select storage paths,
versions, tags, index execution knobs, or administrative cache operations.
"""

from __future__ import annotations

import importlib
import json
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

from bench.config import NAMESPACE, PROTO_PATH, TENANT_ID, BenchConfig

RPC_DEADLINE_SECONDS: float = 5.0


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


def open_stub(config: BenchConfig, pb2_grpc: ModuleType, timeout_seconds: float = 5.0) -> Any:
    """Open a CA-verified TLS channel to the external search service.

    Args:
        config: Benchmark configuration carrying the endpoint and trusted CA path.
        pb2_grpc: The generated service stub module.
        timeout_seconds: Readiness wait budget.

    Returns:
        A ready ``SearchServiceStub``.

    Raises:
        RuntimeError: If credentials are absent, the CA is unreadable, or the service is unreachable.
    """
    if not config.search_credentials_configured() or config.search_ca_path is None:
        raise RuntimeError("external search credentials are not configured")
    try:
        trusted_ca: bytes = config.search_ca_path.read_bytes()
    except OSError as error:
        raise RuntimeError(f"cannot read search CA file at {config.search_ca_path}") from error
    if not trusted_ca:
        raise RuntimeError(f"search CA file is empty at {config.search_ca_path}")
    channel: Any = grpc.secure_channel(config.endpoint, grpc.ssl_channel_credentials(root_certificates=trusted_ca))
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout_seconds)
    except grpc.FutureTimeoutError as error:
        channel.close()
        raise RuntimeError(f"verified TLS search service unreachable at {config.endpoint}") from error
    return pb2_grpc.SearchServiceStub(channel)


def authorization_metadata(config: BenchConfig, org_id: str) -> tuple[tuple[str, str], ...]:
    """Read the rotating bearer token for one exact target at request time.

    Args:
        config: Benchmark configuration carrying the token directory.
        org_id: Organization identity of the request target.

    Returns:
        gRPC authorization metadata.

    Raises:
        RuntimeError: If the target token file is absent, unreadable, or empty.
    """
    token_path: Path = config.search_token_path(org_id)
    try:
        token: str = token_path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise RuntimeError(f"cannot read bearer token for target {org_id!r} at {token_path}") from error
    if not token or any(character.isspace() for character in token):
        raise RuntimeError(f"bearer token for target {org_id!r} is empty or contains whitespace")
    return (("authorization", f"Bearer {token}"),)


def target_key(org_id: str) -> str:
    """Return the exact logical target key used by publication evidence.

    Args:
        org_id: Benchmark organization identity.

    Returns:
        The stable ``tenant/namespace/org`` key.
    """
    return f"{TENANT_ID}/{NAMESPACE}/{org_id}"


def load_expected_versions(config: BenchConfig) -> dict[str, int]:
    """Load and validate the expected catalog publication for every benchmark target.

    Args:
        config: External search configuration.

    Returns:
        Expected positive Lance version keyed by organization.

    Raises:
        RuntimeError: If the evidence file is missing, malformed, incomplete, or contains extra targets.
    """
    path: Path | None = config.search_expected_versions_path
    if path is None:
        raise RuntimeError("external search expected-version evidence is not configured")
    try:
        raw: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read expected search publication at {path}") from error
    if not isinstance(raw, dict):
        raise RuntimeError("expected search publication must be a JSON object")
    expected_keys: set[str] = {target_key(org) for org in config.org_ids()}
    if set(raw) != expected_keys:
        raise RuntimeError(
            f"expected search publication keys must equal {sorted(expected_keys)}, "
            f"got {sorted(str(key) for key in raw)}"
        )
    versions: dict[str, int] = {}
    org: Any
    for org in config.org_ids():
        value: object = raw[target_key(org)]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise RuntimeError(f"expected served version for {target_key(org)} must be a positive integer")
        versions[org] = value
    return versions


def validate_served_version(response: Any, org_id: str, expected_version: int) -> int:
    """Reject a response that did not use the operator-approved exact publication.

    Args:
        response: Typed public search response.
        org_id: Organization being measured.
        expected_version: Approved exact Lance version for that target.

    Returns:
        The validated served version.

    Raises:
        RuntimeError: If the response version differs from publication evidence.
    """
    served_version: int = int(response.served_version)
    if served_version != expected_version:
        raise RuntimeError(
            f"target {target_key(org_id)} served version {served_version}, "
            f"expected published version {expected_version}"
        )
    return served_version


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
    query: Any = pb2.TextQuery(simple=terms)
    query.columns.extend(columns)
    return query


def result_record_ids(results: Any) -> np.ndarray:
    """Extract the ``record_id`` column from search results as global int ids.

    The release API carries the stable logical identifier as a required typed string rather than a
    generic row struct. An empty or non-numeric identifier is contract drift for these integer-id
    corpora and fails loudly instead of silently deflating recall.

    Args:
        results: Repeated typed result messages.

    Returns:
        An int64 array of global record ids in result order, one entry per input result.

    Raises:
        ValueError: If any result has an empty or non-numeric ``record_id``.
    """
    ids: list[int] = []
    malformed: list[str] = []
    total: int = 0
    result: Any
    for result in results:
        try:
            ids.append(int(result.record_id))
        except (TypeError, ValueError):
            malformed.append(f"result[{total}] record_id={result.record_id!r}")
        total += 1
    if malformed:
        raise ValueError(
            f"{len(malformed)} of {total} results carried an empty or non-numeric record_id. Offenders: {malformed[:5]}"
        )
    return np.asarray(ids, dtype=np.int64)


def timed_call(
    callable_rpc: Any,
    request: Any,
    metadata: tuple[tuple[str, str], ...] = (),
    timeout_seconds: float = RPC_DEADLINE_SECONDS,
) -> tuple[Any, float]:
    """Invoke one RPC and measure its latency.

    Args:
        callable_rpc: The stub method.
        request: The request message.
        metadata: Authentication metadata attached to the request.
        timeout_seconds: Per-RPC client deadline.

    Returns:
        The response and the latency in milliseconds.
    """
    started: float = time.perf_counter()
    response: Any = callable_rpc(request, metadata=metadata, timeout=timeout_seconds)
    return response, (time.perf_counter() - started) * 1000.0


def vector_search(
    stub: Any,
    pb2: ModuleType,
    config: BenchConfig,
    org_id: str,
    query: np.ndarray,
    k: int,
    expected_version: int,
) -> tuple[Any, float]:
    """Run vector search through the current catalog publication.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration carrying target token files.
        org_id: Organization to query.
        query: Query vector.
        k: Bounded result count.
        expected_version: Operator-approved exact Lance publication.

    Returns:
        Response and client-side latency in milliseconds.
    """
    request: Any = pb2.VectorSearchRequest(
        target=dataset_target(pb2, org_id),
        query=vector_query(pb2, query),
        k=k,
    )
    response: Any
    elapsed_ms: Any
    response, elapsed_ms = timed_call(stub.VectorSearch, request, authorization_metadata(config, org_id))
    validate_served_version(response, org_id, expected_version)
    return response, elapsed_ms


def text_search(
    stub: Any,
    pb2: ModuleType,
    config: BenchConfig,
    org_id: str,
    terms: str,
    k: int,
    expected_version: int,
) -> tuple[Any, float]:
    """Run text search through the current catalog publication.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration carrying target token files.
        org_id: Organization to query.
        terms: Simple product query terms.
        k: Bounded result count.
        expected_version: Operator-approved exact Lance publication.

    Returns:
        Response and client-side latency in milliseconds.
    """
    request: Any = pb2.TextSearchRequest(
        target=dataset_target(pb2, org_id),
        query=text_query(pb2, terms),
        k=k,
    )
    response: Any
    elapsed_ms: Any
    response, elapsed_ms = timed_call(stub.TextSearch, request, authorization_metadata(config, org_id))
    validate_served_version(response, org_id, expected_version)
    return response, elapsed_ms


def hybrid_search(
    stub: Any,
    pb2: ModuleType,
    config: BenchConfig,
    org_id: str,
    query: np.ndarray,
    terms: str,
    k: int,
    expected_version: int,
) -> tuple[Any, float]:
    """Run hybrid search through the current catalog publication.

    Args:
        stub: The connected service stub.
        pb2: The generated proto module.
        config: Benchmark configuration carrying target token files.
        org_id: The organization to query.
        query: The query vector for the vector leg.
        terms: The space-separated query terms for the text leg.
        k: Fused hits to return.
        expected_version: Operator-approved exact Lance publication.

    Returns:
        Response and client-side latency in milliseconds.
    """
    request: Any = pb2.HybridSearchRequest(
        target=dataset_target(pb2, org_id),
        vector=vector_query(pb2, query),
        text=text_query(pb2, terms),
        k=k,
    )
    response: Any
    elapsed_ms: Any
    response, elapsed_ms = timed_call(stub.HybridSearch, request, authorization_metadata(config, org_id))
    validate_served_version(response, org_id, expected_version)
    return response, elapsed_ms
