"""Unit tests for the binary corpus I/O helpers in bench/bigann_io.py.

Covers the u8bin write/read round-trip, slice reads, ibin parsing, gzipped-bvecs conversion, and
streaming resume-from-partial. All tests use tiny in-memory files written to tmp_path so no
network access is required.
"""

from __future__ import annotations

import io
import struct
import unittest.mock
import zlib
from contextlib import AbstractContextManager, closing
from pathlib import Path

import numpy as np
import pytest

from bench.bigann_io import (
    BVECS_DIM,
    BVECS_STRIDE,
    GZIP_WBITS,
    U8BIN_HEADER_BYTES,
    convert_bvecs_gz_to_u8bin,
    make_gzip_bvecs,
    read_ibin_neighbors,
    read_u8bin,
    read_u8bin_slice,
    rebuild_stream_state,
    stream_bvecs_to_u8bin,
    u8bin_count_from_size,
    u8bin_header,
    write_u8bin,
)


def make_u8bin(path: Path, vectors: np.ndarray) -> None:
    """Write a u8bin file from a uint8 or float32 array.

    Args:
        path: Destination file.
        vectors: 2-D array of shape (nvecs, dim).
    """
    write_u8bin(path, vectors)


def make_ibin(path: Path, ids: np.ndarray, distances: np.ndarray) -> None:
    """Write a minimal ibin ground-truth file.

    Args:
        path: Destination file.
        ids: int32 array of shape (nqueries, k).
        distances: float32 array of shape (nqueries, k).
    """
    nqueries, k = ids.shape
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        handle.write(struct.pack("<II", nqueries, k))
        handle.write(ids.astype(np.int32).tobytes())
        handle.write(distances.astype(np.float32).tobytes())


def make_bvecs128_corpus(nvecs: int, seed: int = 0) -> np.ndarray:
    """Generate a small uint8 corpus of shape (nvecs, 128) for BIGANN-format tests.

    Args:
        nvecs: Number of vectors to generate.
        seed: RNG seed for reproducibility.

    Returns:
        A uint8 array of shape (nvecs, 128).
    """
    rng: np.random.Generator = np.random.default_rng(seed)
    return rng.integers(0, 256, size=(nvecs, BVECS_DIM), dtype=np.uint8)


def bytes_response(data: bytes) -> AbstractContextManager[io.BytesIO]:
    """Wrap response bytes in a closing in-memory stream.

    Args:
        data: The response body bytes.

    Returns:
        A context manager yielding a readable in-memory stream.
    """
    return closing(io.BytesIO(data))


def test_u8bin_header(tmp_path: Path) -> None:
    """u8bin_header returns the correct nvecs and dim from the file header."""
    vectors = np.arange(30, dtype=np.uint8).reshape(5, 6)
    path = tmp_path / "test.u8bin"
    make_u8bin(path, vectors)
    nvecs, dim = u8bin_header(path)
    assert nvecs == 5
    assert dim == 6


def test_u8bin_roundtrip(tmp_path: Path) -> None:
    """write_u8bin then read_u8bin returns an array equal to the original (within uint8 precision)."""
    rng = np.random.default_rng(42)
    vectors = rng.integers(0, 256, size=(20, 8), dtype=np.uint8)
    path = tmp_path / "roundtrip.u8bin"
    write_u8bin(path, vectors)
    result = read_u8bin(path)
    assert result.dtype == np.float32
    assert result.shape == (20, 8)
    np.testing.assert_array_equal(result, vectors.astype(np.float32))


def test_u8bin_roundtrip_float_input(tmp_path: Path) -> None:
    """write_u8bin clips float32 values to [0, 255] and casts to uint8 before writing."""
    vectors = np.array([[0.0, 127.9, 255.0, 300.0], [-5.0, 128.0, 200.0, 254.0]], dtype=np.float32)
    path = tmp_path / "float_input.u8bin"
    write_u8bin(path, vectors)
    result = read_u8bin(path)
    expected = np.clip(vectors, 0, 255).astype(np.uint8).astype(np.float32)
    np.testing.assert_array_equal(result, expected)


def test_read_u8bin_with_limit(tmp_path: Path) -> None:
    """read_u8bin with limit returns only the first limit rows."""
    vectors = np.arange(200, dtype=np.uint8).reshape(25, 8)
    path = tmp_path / "limited.u8bin"
    write_u8bin(path, vectors)
    result = read_u8bin(path, limit=10)
    assert result.shape == (10, 8)
    np.testing.assert_array_equal(result, vectors[:10].astype(np.float32))


def test_read_u8bin_slice_middle(tmp_path: Path) -> None:
    """read_u8bin_slice returns the correct middle rows."""
    vectors = np.arange(160, dtype=np.uint8).reshape(20, 8)
    path = tmp_path / "slice.u8bin"
    write_u8bin(path, vectors)
    result = read_u8bin_slice(path, 5, 10)
    assert result.shape == (5, 8)
    np.testing.assert_array_equal(result, vectors[5:10].astype(np.float32))


def test_read_u8bin_slice_full(tmp_path: Path) -> None:
    """read_u8bin_slice with [0, n) returns the entire file."""
    vectors = np.arange(40, dtype=np.uint8).reshape(5, 8)
    path = tmp_path / "full_slice.u8bin"
    write_u8bin(path, vectors)
    result = read_u8bin_slice(path, 0, 5)
    np.testing.assert_array_equal(result, vectors.astype(np.float32))


def test_read_u8bin_slice_empty(tmp_path: Path) -> None:
    """read_u8bin_slice with count=0 returns an empty array with the correct dim."""
    vectors = np.zeros((10, 8), dtype=np.uint8)
    path = tmp_path / "empty_slice.u8bin"
    write_u8bin(path, vectors)
    result = read_u8bin_slice(path, 3, 3)
    assert result.shape == (0, 8)


def test_read_u8bin_slice_out_of_range(tmp_path: Path) -> None:
    """read_u8bin_slice raises ValueError when the slice extends past the file."""
    vectors = np.zeros((5, 8), dtype=np.uint8)
    path = tmp_path / "oob.u8bin"
    write_u8bin(path, vectors)
    with pytest.raises(ValueError, match="out of range"):
        read_u8bin_slice(path, 3, 7)


def test_u8bin_count_from_size(tmp_path: Path) -> None:
    """u8bin_count_from_size counts complete rows from file size and header dim."""
    vectors = np.zeros((12, 16), dtype=np.uint8)
    path = tmp_path / "count.u8bin"
    write_u8bin(path, vectors)
    count = u8bin_count_from_size(path)
    assert count == 12


def test_u8bin_header_too_short(tmp_path: Path) -> None:
    """u8bin_header raises ValueError for a file shorter than 8 bytes."""
    path = tmp_path / "short.u8bin"
    path.write_bytes(b"\x00\x01\x02")
    with pytest.raises(ValueError, match="too short"):
        u8bin_header(path)


def test_write_u8bin_not_2d(tmp_path: Path) -> None:
    """write_u8bin raises ValueError for non-2D arrays."""
    path = tmp_path / "bad.u8bin"
    with pytest.raises(ValueError, match="2-D"):
        write_u8bin(path, np.zeros(10, dtype=np.uint8))


def test_read_ibin_neighbors(tmp_path: Path) -> None:
    """read_ibin_neighbors returns the correct int32 neighbor-id matrix."""
    ids = np.array([[0, 1, 2], [3, 4, 5], [6, 7, 8]], dtype=np.int32)
    distances = np.zeros_like(ids, dtype=np.float32)
    path = tmp_path / "gt.ibin"
    make_ibin(path, ids, distances)
    result = read_ibin_neighbors(path)
    assert result.shape == (3, 3)
    np.testing.assert_array_equal(result, ids)


def test_read_ibin_neighbors_dtype(tmp_path: Path) -> None:
    """read_ibin_neighbors returns int32 regardless of the stored values."""
    ids = np.arange(50, dtype=np.int32).reshape(5, 10)
    distances = np.ones((5, 10), dtype=np.float32)
    path = tmp_path / "gt2.ibin"
    make_ibin(path, ids, distances)
    result = read_ibin_neighbors(path)
    assert result.dtype == np.int32
    np.testing.assert_array_equal(result, ids)


def test_read_ibin_too_short(tmp_path: Path) -> None:
    """read_ibin_neighbors raises ValueError when the header is truncated."""
    path = tmp_path / "bad_ibin.ibin"
    path.write_bytes(b"\x01\x00")
    with pytest.raises(ValueError, match="too short"):
        read_ibin_neighbors(path)


def test_convert_bvecs_gz_to_u8bin_roundtrip(tmp_path: Path) -> None:
    """convert_bvecs_gz_to_u8bin decompresses a bvecs.gz blob and writes a valid u8bin file.

    Verifies that every payload byte survives the gzip-bvecs -> u8bin round-trip without
    corruption, and that the output header matches the requested limit.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    nvecs: int = 20
    limit: int = 15
    vectors: np.ndarray = make_bvecs128_corpus(nvecs, seed=1)
    gz_bytes: bytes = make_gzip_bvecs(vectors)
    dest: Path = tmp_path / "out.u8bin"
    convert_bvecs_gz_to_u8bin(gz_bytes, dest, limit)
    result: np.ndarray = read_u8bin(dest)
    assert result.shape == (limit, BVECS_DIM)
    np.testing.assert_array_equal(result, vectors[:limit].astype(np.float32))


def test_convert_bvecs_gz_to_u8bin_exact_limit(tmp_path: Path) -> None:
    """convert_bvecs_gz_to_u8bin with limit == nvecs returns all vectors."""
    nvecs: int = 10
    vectors: np.ndarray = make_bvecs128_corpus(nvecs, seed=2)
    gz_bytes: bytes = make_gzip_bvecs(vectors)
    dest: Path = tmp_path / "exact.u8bin"
    convert_bvecs_gz_to_u8bin(gz_bytes, dest, nvecs)
    result: np.ndarray = read_u8bin(dest)
    assert result.shape == (nvecs, BVECS_DIM)
    np.testing.assert_array_equal(result, vectors.astype(np.float32))


def test_stream_bvecs_to_u8bin_oneshot(tmp_path: Path) -> None:
    """stream_bvecs_to_u8bin writes exactly limit vectors when given the full stream.

    Uses a mock _open_range_response that returns the complete gzip-compressed bvecs bytes
    wrapped in a BytesIO context manager, so no network access occurs.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    nvecs: int = 30
    limit: int = 20
    vectors: np.ndarray = make_bvecs128_corpus(nvecs, seed=3)
    gz_bytes: bytes = make_gzip_bvecs(vectors)
    dest: Path = tmp_path / "oneshot.u8bin"

    def fake_open_range(url: str, start_byte: int) -> tuple[str, AbstractContextManager[io.BytesIO]]:
        """Return an in-memory response wrapping compressed data from the requested offset."""
        return url, bytes_response(gz_bytes[start_byte:])

    with unittest.mock.patch("bench.bigann_io.open_range_response", side_effect=fake_open_range):
        written: int = stream_bvecs_to_u8bin("http://primary/", "http://fallback/", dest, limit)

    assert written == limit
    result: np.ndarray = read_u8bin(dest)
    assert result.shape == (limit, BVECS_DIM)
    np.testing.assert_array_equal(result, vectors[:limit].astype(np.float32))


def test_stream_bvecs_to_u8bin_resume(tmp_path: Path) -> None:
    """Resuming from a compressed .partial sidecar yields identical output to a one-shot run.

    The test simulates an interrupted download by writing the first half of the gzip
    stream as the compressed sidecar, deliberately cutting mid-record and leaving no other
    state behind. A second call to stream_bvecs_to_u8bin must rebuild the decompressor and
    the u8bin partial from the sidecar alone, fetch only the remaining compressed bytes via
    the Range offset, and produce the same u8bin as a clean one-shot run.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    nvecs: int = 30
    limit: int = 20
    vectors: np.ndarray = make_bvecs128_corpus(nvecs, seed=4)
    gz_bytes: bytes = make_gzip_bvecs(vectors)

    dest_oneshot: Path = tmp_path / "oneshot.u8bin"
    dest_resume: Path = tmp_path / "resume.u8bin"
    requested_offsets: list[int] = []

    def fake_open_range(url: str, start_byte: int) -> tuple[str, AbstractContextManager[io.BytesIO]]:
        """Return bytes from start_byte onward in the gzip stream, recording the offset."""
        requested_offsets.append(start_byte)
        return url, bytes_response(gz_bytes[start_byte:])

    with unittest.mock.patch("bench.bigann_io.open_range_response", side_effect=fake_open_range):
        stream_bvecs_to_u8bin("http://primary/", "http://fallback/", dest_oneshot, limit)

    u8bin_partial: Path = dest_resume.with_suffix(dest_resume.suffix + ".partial")
    compressed_partial: Path = dest_resume.with_suffix(dest_resume.suffix + ".gz.partial")

    split_byte: int = len(gz_bytes) // 2
    compressed_partial.parent.mkdir(parents=True, exist_ok=True)
    compressed_partial.write_bytes(gz_bytes[:split_byte])

    dec_check, leftover_check, recovered = rebuild_stream_state(compressed_partial, u8bin_partial, limit)
    dec_probe: zlib.Decompress = zlib.decompressobj(wbits=GZIP_WBITS)
    decompressed_so_far: bytes = dec_probe.decompress(gz_bytes[:split_byte])
    assert recovered == min(limit, len(decompressed_so_far) // BVECS_STRIDE)
    assert len(leftover_check) == len(decompressed_so_far) - recovered * BVECS_STRIDE
    del dec_check

    with unittest.mock.patch("bench.bigann_io.open_range_response", side_effect=fake_open_range):
        written: int = stream_bvecs_to_u8bin("http://primary/", "http://fallback/", dest_resume, limit)

    assert written == limit
    assert requested_offsets[-1] == split_byte
    result_resume: np.ndarray = read_u8bin(dest_resume)
    result_oneshot: np.ndarray = read_u8bin(dest_oneshot)
    assert result_resume.shape == (limit, BVECS_DIM)
    np.testing.assert_array_equal(result_resume, result_oneshot)

    assert not u8bin_partial.exists()
    assert not compressed_partial.exists()


def test_make_gzip_bvecs_roundtrip() -> None:
    """make_gzip_bvecs produces bytes that decompress back to the original bvecs records."""
    vectors: np.ndarray = make_bvecs128_corpus(5, seed=5)
    gz_bytes: bytes = make_gzip_bvecs(vectors)
    dec: zlib.Decompress = zlib.decompressobj(wbits=GZIP_WBITS)
    raw: bytes = dec.decompress(gz_bytes) + dec.flush()
    assert len(raw) == 5 * BVECS_STRIDE
    for i in range(5):
        record_offset: int = i * BVECS_STRIDE
        dim_val: int = struct.unpack_from("<I", raw, record_offset)[0]
        assert dim_val == BVECS_DIM
        payload: bytes = raw[record_offset + 4 : record_offset + BVECS_STRIDE]
        np.testing.assert_array_equal(np.frombuffer(payload, dtype=np.uint8), vectors[i])


def test_rebuild_stream_state_continues_decoding(tmp_path: Path) -> None:
    """rebuild_stream_state reconstructs a decompressor that can continue decoding.

    Feeds the first half of a gzip stream into the partial file, rebuilds the stream state,
    then verifies the rebuilt u8bin partial plus the continued decode of the second half
    reproduces every vector payload of the full corpus.

    Args:
        tmp_path: Pytest-provided temporary directory.
    """
    vectors: np.ndarray = make_bvecs128_corpus(10, seed=6)
    limit: int = 10
    gz_bytes: bytes = make_gzip_bvecs(vectors)
    split: int = len(gz_bytes) // 2

    compressed_partial: Path = tmp_path / "test.gz.partial"
    u8bin_partial: Path = tmp_path / "test.u8bin.partial"
    compressed_partial.write_bytes(gz_bytes[:split])

    dec_rebuilt, leftover, recovered = rebuild_stream_state(compressed_partial, u8bin_partial, limit)
    remainder: bytes = leftover + dec_rebuilt.decompress(gz_bytes[split:]) + dec_rebuilt.flush()

    recovered_payload: bytes = u8bin_partial.read_bytes()[U8BIN_HEADER_BYTES:]
    assert len(recovered_payload) == recovered * BVECS_DIM

    tail_payloads: list[bytes] = []
    offset: int = 0
    while offset + BVECS_STRIDE <= len(remainder):
        tail_payloads.append(remainder[offset + 4 : offset + BVECS_STRIDE])
        offset += BVECS_STRIDE

    all_payload: bytes = recovered_payload + b"".join(tail_payloads)
    np.testing.assert_array_equal(
        np.frombuffer(all_payload, dtype=np.uint8).reshape(limit, BVECS_DIM),
        vectors,
    )


def test_u8bin_header_bytes_constant() -> None:
    """U8BIN_HEADER_BYTES is 8, matching the two uint32 fields in the u8bin header."""
    assert U8BIN_HEADER_BYTES == 8


def test_bvecs_stride_constant() -> None:
    """BVECS_STRIDE is 132: 4-byte dim header plus 128 uint8 payload bytes."""
    assert BVECS_STRIDE == 4 + BVECS_DIM
    assert BVECS_STRIDE == 132
