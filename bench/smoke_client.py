"""Release smoke client for one plaintext exact-version search request."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

import grpc
from google.protobuf.json_format import ParseDict

from bench.grpc_client import RPC_DEADLINE_SECONDS, generate_and_load_stubs


def parser() -> argparse.ArgumentParser:
    """Build the release smoke-client parser.

    Returns:
        Parser for the plaintext endpoint, request-file, and expected-version inputs.
    """
    result = argparse.ArgumentParser(description="Run one exact-version search smoke request")
    result.add_argument("--endpoint", required=True)
    result.add_argument("--request-path", required=True, type=Path)
    result.add_argument("--expected-version", required=True, type=int)
    return result


def load_request(path: Path, pb2: ModuleType) -> Any:
    """Parse the release-controlled JSON request as a typed vector request.

    Args:
        path: JSON request file.
        pb2: Generated protobuf module.

    Returns:
        Typed ``VectorSearchRequest``.
    """
    payload: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("smoke request must be a JSON object")
    return ParseDict(payload, pb2.VectorSearchRequest(), ignore_unknown_fields=False)


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Execute one plaintext smoke request.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Non-secret evidence naming the exact served version and result count.

    Raises:
        RuntimeError: If the response did not serve the expected exact version.
    """
    if args.expected_version <= 0:
        raise ValueError("expected version must be positive")
    with tempfile.TemporaryDirectory(prefix="lance-etl-smoke-") as directory:
        pb2, pb2_grpc = generate_and_load_stubs(Path(directory))
        request: Any = load_request(args.request_path, pb2)
        channel = grpc.insecure_channel(args.endpoint)
        try:
            grpc.channel_ready_future(channel).result(timeout=RPC_DEADLINE_SECONDS)
            response: Any = pb2_grpc.SearchServiceStub(channel).VectorSearch(
                request,
                timeout=RPC_DEADLINE_SECONDS,
            )
        finally:
            channel.close()
    served_version: int = int(response.served_version)
    if served_version != args.expected_version:
        raise RuntimeError(f"served version {served_version}, expected {args.expected_version}")
    return {"status": "MEASURED", "served_version": served_version, "result_count": len(response.results)}


def main(argv: list[str] | None = None) -> int:
    """Run the smoke client.

    Args:
        argv: Optional arguments after the module name.

    Returns:
        Zero after an exact-version response.
    """
    evidence: dict[str, Any] = run(parser().parse_args(argv))
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
