"""Readers and writer for binary file formats used by the BIGANN benchmark.

Supports the u8bin format used by BIGANN base and query files, the ibin format used by the
official ground-truth files from the big-ann-benchmarks CDN, and the bvecs format used by
the IRISA corpus-texmex distribution (bigann_base.bvecs.gz, bigann_query.bvecs.gz).

u8bin layout:
  8-byte header: uint32 nvecs, uint32 dim (little-endian).
  nvecs * dim bytes of uint8 row data (row-major).

ibin ground-truth layout:
  8-byte header: uint32 nqueries, uint32 k (little-endian).
  nqueries * k int32 neighbor ids (little-endian, row-major).
  nqueries * k float32 distances (little-endian, row-major).

bvecs (IRISA) layout (per-vector):
  4-byte little-endian uint32 dim (always 128 for BIGANN).
  dim bytes of uint8 payload.
  Record stride: 4 + dim = 132 bytes.

The streaming converter ``stream_bvecs_to_u8bin`` reads a bvecs.gz source via HTTP, writes
a local u8bin artifact for the first ``limit`` vectors, supports resuming an interrupted
transfer by re-decompressing the locally kept compressed .partial sidecar, and stops the
HTTP connection as soon as the limit is satisfied.
"""

from __future__ import annotations

import io
import logging
import struct
import tarfile
import urllib.request
import zlib
from http.client import HTTPResponse
from pathlib import Path
from typing import BinaryIO

import numpy as np

logger: logging.Logger = logging.getLogger(__name__)

U8BIN_HEADER_BYTES: int = 8
IBIN_HEADER_BYTES: int = 8
BVECS_DIM_BYTES: int = 4
BVECS_DIM: int = 128
BVECS_STRIDE: int = BVECS_DIM_BYTES + BVECS_DIM
STREAM_CHUNK_BYTES: int = 8 * 1024 * 1024
FETCH_TIMEOUT_SECONDS: int = 300
GZIP_WBITS: int = 47


def u8bin_header(path: Path) -> tuple[int, int]:
    """Read the nvecs and dim fields from a u8bin file header.

    Args:
        path: The u8bin file.

    Returns:
        A tuple of (nvecs, dim).

    Raises:
        ValueError: If the file is too short to hold the header.
    """
    with open(path, "rb") as handle:
        raw: bytes = handle.read(U8BIN_HEADER_BYTES)
    if len(raw) < U8BIN_HEADER_BYTES:
        raise ValueError(f"{path}: file too short for u8bin header ({len(raw)} bytes)")
    nvecs: int
    dim: int
    nvecs, dim = struct.unpack("<II", raw)
    return nvecs, dim


def u8bin_count_from_size(path: Path) -> int:
    """Count the rows in a u8bin file from its file size and header dim.

    Useful for partial downloads where the header nvecs has been patched to match the
    actual bytes written.

    Args:
        path: The u8bin file.

    Returns:
        The number of complete rows present based on file size.
    """
    nvecs, dim = u8bin_header(path)
    del nvecs
    file_size: int = path.stat().st_size
    data_bytes: int = file_size - U8BIN_HEADER_BYTES
    return data_bytes // dim


def read_u8bin_slice(path: Path, start: int, stop: int) -> np.ndarray:
    """Read a contiguous row slice from a u8bin file using mmap.

    The returned array is cast to float32 so it is compatible with the Lance vector
    column type (array<float>).

    Args:
        path: The u8bin file.
        start: Inclusive first row index.
        stop: Exclusive last row index.

    Returns:
        A float32 array of shape (stop - start, dim).

    Raises:
        ValueError: If the slice extends past the available rows.
    """
    nvecs, dim = u8bin_header(path)
    count: int = stop - start
    if start < 0 or count < 0 or stop > nvecs:
        raise ValueError(f"{path}: slice [{start}, {stop}) out of range for {nvecs} rows")
    if count == 0:
        return np.empty((0, dim), dtype=np.float32)
    offset: int = U8BIN_HEADER_BYTES + start * dim
    raw: np.ndarray = np.memmap(path, dtype=np.uint8, mode="r", offset=offset, shape=(count, dim))
    return raw.astype(np.float32)


def read_u8bin(path: Path, limit: int | None = None) -> np.ndarray:
    """Read base vectors from a u8bin file, optionally capped.

    Args:
        path: The u8bin file.
        limit: Optional cap on rows read from the start of the file.

    Returns:
        A float32 array of shape (rows, dim).
    """
    nvecs, _ = u8bin_header(path)
    stop: int = nvecs if limit is None else min(limit, nvecs)
    return read_u8bin_slice(path, 0, stop)


def read_ibin_neighbors(path: Path) -> np.ndarray:
    """Read the integer neighbor-id matrix from an ibin ground-truth file.

    The ibin format stores nqueries*k int32 neighbor ids followed by nqueries*k float32
    distances. Only the id matrix is returned because the benchmark recall computation
    needs only the ids.

    Args:
        path: The ibin ground-truth file.

    Returns:
        An int32 array of shape (nqueries, k).

    Raises:
        ValueError: If the file is too short for the header.
    """
    with open(path, "rb") as handle:
        raw: bytes = handle.read(IBIN_HEADER_BYTES)
    if len(raw) < IBIN_HEADER_BYTES:
        raise ValueError(f"{path}: file too short for ibin header ({len(raw)} bytes)")
    nqueries: int
    k: int
    nqueries, k = struct.unpack("<II", raw)
    ids: np.ndarray = np.memmap(path, dtype=np.int32, mode="r", offset=IBIN_HEADER_BYTES, shape=(nqueries, k))
    return np.array(ids)


def write_u8bin(path: Path, vectors: np.ndarray) -> None:
    """Write a float32 or uint8 array to a u8bin file.

    Vectors are clipped and cast to uint8 before writing. Used by tests for round-trip
    verification.

    Args:
        path: Destination file.
        vectors: A 2-D array of shape (nvecs, dim). Values are clipped to [0, 255].
    """
    if vectors.ndim != 2:
        raise ValueError(f"vectors must be 2-D, got shape {vectors.shape}")
    nvecs: int
    dim: int
    nvecs, dim = vectors.shape
    uint8_data: np.ndarray = np.clip(vectors, 0, 255).astype(np.uint8)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(struct.pack("<II", nvecs, dim))
        handle.write(uint8_data.tobytes())


def make_gzip_bvecs(vectors: np.ndarray) -> bytes:
    """Encode a uint8 array as a gzip-compressed bvecs byte string (for tests).

    Each vector is serialised as a 4-byte little-endian uint32 dim followed by dim uint8 bytes.

    Args:
        vectors: A 2-D uint8 array of shape (nvecs, dim).

    Returns:
        The gzip-compressed bvecs bytes.
    """
    if vectors.ndim != 2:
        raise ValueError(f"vectors must be 2-D, got shape {vectors.shape}")
    dim: int = vectors.shape[1]
    buf: io.BytesIO = io.BytesIO()
    gz: zlib.Compress = zlib.compressobj(wbits=31)
    dim_header: bytes = struct.pack("<I", dim)
    for row in vectors:
        buf.write(gz.compress(dim_header + row.astype(np.uint8).tobytes()))
    buf.write(gz.flush())
    return buf.getvalue()


def drain_bvecs_records(buffer: bytes, sink: BinaryIO, max_records: int) -> tuple[bytes, int]:
    """Parse complete bvecs records from a buffer and write their payloads as u8bin rows.

    Each record is validated against the expected BIGANN dimension before its 128-byte
    uint8 payload is appended to the sink. Parsing stops at ``max_records`` or at the first
    incomplete trailing record, whichever comes first.

    Args:
        buffer: Decompressed bvecs bytes, starting at a record boundary.
        sink: Open binary file positioned where the next u8bin row belongs.
        max_records: Maximum number of records to consume.

    Returns:
        A tuple of (leftover_bytes, records_written) where leftover_bytes is the
        unconsumed tail (an incomplete record or records beyond max_records).

    Raises:
        ValueError: If a record header does not announce the expected dimension.
    """
    offset: int = 0
    written: int = 0
    while written < max_records and offset + BVECS_STRIDE <= len(buffer):
        dim_val: int = struct.unpack_from("<I", buffer, offset)[0]
        if dim_val != BVECS_DIM:
            raise ValueError(f"bvecs record at byte {offset} has unexpected dim {dim_val}; expected {BVECS_DIM}")
        sink.write(buffer[offset + BVECS_DIM_BYTES : offset + BVECS_STRIDE])
        offset += BVECS_STRIDE
        written += 1
    return buffer[offset:], written


def rebuild_stream_state(
    compressed_partial: Path,
    u8bin_partial: Path,
    limit: int,
) -> tuple[zlib.Decompress, bytes, int]:
    """Reconstruct the full streaming state from the locally stored compressed bytes.

    Resume is derived from the compressed .partial file alone: the bytes already on disk
    are re-decompressed from the start (CPU-only, no network) and the u8bin partial is
    rewritten from scratch, so the decompressor state, the unconsumed record tail, and the
    written-vector count are always mutually consistent. Memory stays bounded by the
    streaming chunk size regardless of how large the partial is.

    Args:
        compressed_partial: The raw compressed .partial sidecar.
        u8bin_partial: The u8bin output partial to rewrite (header plus payloads).
        limit: Total number of vectors the final artifact will contain.

    Returns:
        A tuple of (decompressor, leftover_bytes, vectors_written) ready to continue the
        transfer from the end of the compressed partial.
    """
    dec: zlib.Decompress = zlib.decompressobj(wbits=GZIP_WBITS)
    leftover: bytes = b""
    vectors_written: int = 0
    with open(u8bin_partial, "wb") as sink:
        sink.write(struct.pack("<II", limit, BVECS_DIM))
        with open(compressed_partial, "rb") as src:
            while True:
                chunk: bytes = src.read(STREAM_CHUNK_BYTES)
                if not chunk:
                    break
                stream: bytes = leftover + dec.decompress(chunk)
                leftover, written = drain_bvecs_records(stream, sink, limit - vectors_written)
                vectors_written += written
    return dec, leftover, vectors_written


def stream_bvecs_to_u8bin(
    primary_url: str,
    fallback_url: str,
    destination: Path,
    limit: int,
) -> int:
    """Stream a gzip-compressed bvecs source URL and write the first ``limit`` vectors as u8bin.

    Acquisition strategy:
    - The HTTP transfer starts at ``primary_url``. If the primary source does not support
      Range headers for resume, ``fallback_url`` is tried instead.
    - Raw compressed bytes are appended to a .partial sidecar file alongside the u8bin
      output partial, so a crash can be resumed without re-downloading already-fetched bytes.
    - Resume state is derived solely from the compressed .partial file: its bytes are
      re-decompressed from the start (CPU-only) to rebuild the decompressor, the record
      tail, and the u8bin partial, then a Range request resumes from the partial's size.
    - The transfer is aborted as soon as ``limit`` vectors have been parsed.
    - On completion, the u8bin partial is atomically renamed to ``destination`` and the
      compressed partial sidecar is removed.

    The output u8bin has header (nvecs=limit, dim=128) and a payload of limit * 128 raw
    uint8 bytes (the per-vector dim headers from bvecs are dropped).

    Args:
        primary_url: HTTP/HTTPS URL of the .bvecs.gz file (IRISA or first mirror).
        fallback_url: HTTPS URL of a Range-capable mirror (e.g. HuggingFace).
        destination: Local u8bin output path.
        limit: Number of vectors to extract and write.

    Returns:
        The number of vectors written (always ``limit`` on success).

    Raises:
        RuntimeError: If neither source can deliver the compressed stream.
    """
    if destination.exists():
        return limit

    u8bin_partial: Path = destination.with_suffix(destination.suffix + ".partial")
    compressed_partial: Path = destination.with_suffix(destination.suffix + ".gz.partial")

    dec: zlib.Decompress
    leftover: bytes
    vectors_written: int
    compressed_offset: int

    if compressed_partial.exists() and compressed_partial.stat().st_size > 0:
        compressed_offset = compressed_partial.stat().st_size
        logger.info("resuming bvecs stream from compressed offset %d", compressed_offset)
        dec, leftover, vectors_written = rebuild_stream_state(compressed_partial, u8bin_partial, limit)
        logger.info("rebuilt stream state: %d vectors recovered locally", vectors_written)
    else:
        dec = zlib.decompressobj(wbits=GZIP_WBITS)
        leftover = b""
        vectors_written = 0
        compressed_offset = 0
        compressed_partial.parent.mkdir(parents=True, exist_ok=True)
        with open(u8bin_partial, "wb") as sink:
            sink.write(struct.pack("<II", limit, BVECS_DIM))
        logger.info("initialized u8bin partial header for %d vectors", limit)

    remaining: int = limit - vectors_written
    if remaining <= 0:
        finalize_u8bin(u8bin_partial, destination, compressed_partial)
        return limit

    source_url: str
    response: HTTPResponse
    try:
        source_url, response = open_range_response(primary_url, compressed_offset)
    except OSError as primary_error:
        logger.warning("primary URL failed (%s): %s; trying fallback", primary_url, primary_error)
        try:
            source_url, response = open_range_response(fallback_url, compressed_offset)
        except OSError as fallback_error:
            raise RuntimeError(
                f"both sources failed: primary={primary_error}, fallback={fallback_error}"
            ) from fallback_error

    logger.info(
        "streaming bvecs from %s at compressed offset %d (need %d more vectors)",
        source_url,
        compressed_offset,
        remaining,
    )

    bytes_downloaded: int = 0

    with response as http, open(compressed_partial, "ab") as comp_sink, open(u8bin_partial, "ab") as u8_sink:
        while remaining > 0:
            chunk: bytes = http.read(STREAM_CHUNK_BYTES)
            if not chunk:
                break
            bytes_downloaded += len(chunk)
            comp_sink.write(chunk)
            stream: bytes = leftover + dec.decompress(chunk)
            leftover, written = drain_bvecs_records(stream, u8_sink, remaining)
            vectors_written += written
            remaining -= written
            if bytes_downloaded % (256 * 1024 * 1024) < STREAM_CHUNK_BYTES:
                logger.info(
                    "progress: %.1f MB downloaded, %d / %d vectors written",
                    bytes_downloaded / 1e6,
                    vectors_written,
                    limit,
                )

    logger.info(
        "stream complete: %d vectors written, %.1f MB compressed downloaded", vectors_written, bytes_downloaded / 1e6
    )
    if vectors_written < limit:
        raise RuntimeError(
            f"source exhausted after {vectors_written}/{limit} vectors; "
            "the compressed .partial sidecar is kept so a retry resumes the transfer"
        )
    finalize_u8bin(u8bin_partial, destination, compressed_partial)
    return vectors_written


def open_range_response(url: str, start_byte: int) -> tuple[str, HTTPResponse]:
    """Open an HTTP connection with a Range header, returning (url, response).

    Args:
        url: The source URL.
        start_byte: First byte to request (inclusive). 0 means start of file.

    Returns:
        A tuple of (url, http_response). The caller must close the response.

    Raises:
        OSError: If the connection fails or the server does not honour the Range header.
    """
    headers: dict[str, str] = {}
    if start_byte > 0:
        headers["Range"] = f"bytes={start_byte}-"
    request = urllib.request.Request(url, headers=headers)
    response = urllib.request.urlopen(request, timeout=FETCH_TIMEOUT_SECONDS)
    return url, response


def finalize_u8bin(partial: Path, destination: Path, compressed_partial: Path) -> None:
    """Atomically rename the u8bin partial to destination and clean up sidecars.

    Args:
        partial: The in-progress u8bin partial file.
        destination: The final artifact path.
        compressed_partial: The raw compressed bytes sidecar.
    """
    partial.rename(destination)
    if compressed_partial.exists():
        compressed_partial.unlink()
    logger.info("finalized u8bin artifact: %s", destination)


def convert_bvecs_gz_to_u8bin(source: bytes, destination: Path, limit: int) -> None:
    """Decompress an in-memory gzip-compressed bvecs blob and write as u8bin (for tests).

    Args:
        source: Raw gzip-compressed bvecs bytes.
        destination: Output u8bin path.
        limit: Number of vectors to write.
    """
    dec: zlib.Decompress = zlib.decompressobj(wbits=GZIP_WBITS)
    raw: bytes = dec.decompress(source) + dec.flush()
    vectors_written: int = 0
    offset: int = 0
    payload_parts: list[bytes] = []
    while vectors_written < limit and offset + BVECS_STRIDE <= len(raw):
        dim_val: int = struct.unpack_from("<I", raw, offset)[0]
        if dim_val != BVECS_DIM:
            raise ValueError(f"unexpected dim {dim_val} at offset {offset}")
        payload_parts.append(raw[offset + BVECS_DIM_BYTES : offset + BVECS_STRIDE])
        offset += BVECS_STRIDE
        vectors_written += 1
    destination.parent.mkdir(parents=True, exist_ok=True)
    with open(destination, "wb") as sink:
        sink.write(struct.pack("<II", vectors_written, BVECS_DIM))
        for part in payload_parts:
            sink.write(part)


def read_ivecs_from_tarball(tarball: Path, member_name: str) -> np.ndarray:
    """Extract one ivecs member from a tar archive and return its neighbor-id matrix.

    The ivecs format stores per-vector records of (uint32 k, k x int32 ids). Each record
    begins with a 4-byte little-endian uint32 k header followed by k int32 neighbor ids.
    This function reads the full matrix from the named member and returns an array of shape
    (nqueries, k) with dtype int32.

    Args:
        tarball: Path to the .tar.gz archive (e.g. bigann_gnd.tar.gz).
        member_name: Archive member path (e.g. ``gnd/idx_100M.ivecs``).

    Returns:
        An int32 array of shape (nqueries, k).

    Raises:
        KeyError: If the member does not exist inside the archive.
        ValueError: If the ivecs data is malformed or record headers are inconsistent.
    """
    with tarfile.open(tarball, "r:gz") as tar:
        member_file = tar.extractfile(member_name)
        if member_file is None:
            raise KeyError(f"member {member_name!r} not found or is not a regular file in {tarball}")
        with member_file:
            raw: bytes = member_file.read()
    if len(raw) < 4:
        raise ValueError(f"{member_name}: too short to contain an ivecs header")
    k: int = struct.unpack_from("<I", raw, 0)[0]
    stride: int = 4 + k * 4
    if len(raw) % stride != 0:
        raise ValueError(f"{member_name}: size {len(raw)} is not a multiple of ivecs stride {stride} (k={k})")
    nqueries: int = len(raw) // stride
    words: np.ndarray = np.frombuffer(raw, dtype="<i4").reshape(nqueries, k + 1)
    if not np.all(words[:, 0] == k):
        raise ValueError(f"{member_name}: inconsistent k headers; expected k={k} in every row")
    return np.ascontiguousarray(words[:, 1:]).astype(np.int32)
