"""Authenticated release smoke client that never places bearer tokens in process arguments."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from types import ModuleType
from typing import Any

import grpc
from google.protobuf.json_format import ParseDict

from bench.grpc_client import RPC_DEADLINE_SECONDS, generate_stubs, load_stubs


def parser() -> argparse.ArgumentParser:
    """Build the release smoke-client parser.

    Returns:
        Parser for verified TLS, token-file, request-file, and version inputs.
    """
    result = argparse.ArgumentParser(description="Run one authenticated exact-version search smoke request")
    result.add_argument("--endpoint", required=True)
    result.add_argument("--server-name", required=True)
    result.add_argument("--ca-path", required=True, type=Path)
    result.add_argument("--token-path", required=True, type=Path)
    result.add_argument("--request-path", required=True, type=Path)
    result.add_argument("--expected-version", required=True, type=int)
    return result


def read_token(path: Path) -> str:
    """Read a non-empty bearer token from a file.

    Args:
        path: Short-lived token file.

    Returns:
        Token contents without surrounding whitespace.

    Raises:
        ValueError: If the token is empty or contains embedded whitespace.
    """
    token: str = path.read_text(encoding="utf-8").strip()
    if not token or any(character.isspace() for character in token):
        raise ValueError("smoke bearer token is empty or contains whitespace")
    return token


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
    """Execute one TLS-verified, authenticated smoke request.

    Args:
        args: Parsed command-line arguments.

    Returns:
        Non-secret evidence naming the exact served version and result count.

    Raises:
        RuntimeError: If the response did not serve the expected exact version.
    """
    if args.expected_version <= 0:
        raise ValueError("expected version must be positive")
    trusted_ca: bytes = args.ca_path.read_bytes()
    if not trusted_ca:
        raise ValueError("search CA file is empty")
    with tempfile.TemporaryDirectory(prefix="lance-etl-smoke-") as directory:
        pb2, pb2_grpc = load_stubs(generate_stubs(Path(directory)))
        request: Any = load_request(args.request_path, pb2)
        credentials: grpc.ChannelCredentials = grpc.ssl_channel_credentials(root_certificates=trusted_ca)
        channel = grpc.secure_channel(
            args.endpoint,
            credentials,
            options=(("grpc.ssl_target_name_override", args.server_name),),
        )
        try:
            grpc.channel_ready_future(channel).result(timeout=RPC_DEADLINE_SECONDS)
            token: str = read_token(args.token_path)
            response: Any = pb2_grpc.SearchServiceStub(channel).VectorSearch(
                request,
                metadata=(("authorization", f"Bearer {token}"),),
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
