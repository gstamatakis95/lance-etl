"""Download, verify, and extract the SIFT1M corpus.

The canonical distribution is ``sift.tar.gz`` from the IRISA TexMex corpus, historically served over FTP; the HTTP URL
is tried first and ``urllib`` falls back to the FTP mirror, then to per-file HuggingFace mirrors. Verification is
two-fold: structural (every file must parse as fvecs/ivecs with the known SIFT1M dimensions and counts) and a sha256
recorded on first success and compared on later runs; ``--sha256`` pins the archive digest explicitly because IRISA
publishes no authoritative checksum.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import tarfile
import urllib.request
from pathlib import Path
from typing import Any

from bench.config import (
    SIFT_BASE_COUNT,
    SIFT_DIM,
    SIFT_FILE_NAMES,
    SIFT_GT_DEPTH,
    SIFT_QUERY_COUNT,
    BenchConfig,
)
from bench.fvecs import vecs_count, vecs_dimension
from bench.results import ensure_dir, read_json, save_phase, write_json

logger: logging.Logger = logging.getLogger(__name__)

TARBALL_URLS: tuple[str, ...] = (
    "http://corpus-texmex.irisa.fr/sift.tar.gz",
    "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz",
)
MIRROR_TEMPLATES: tuple[str, ...] = (
    "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/{name}",
    "https://huggingface.co/datasets/maknee/sift1m/resolve/main/sift/{name}",
)
EXPECTED_SHAPES: dict[str, tuple[int, int]] = {
    "sift_base.fvecs": (SIFT_DIM, SIFT_BASE_COUNT),
    "sift_query.fvecs": (SIFT_DIM, SIFT_QUERY_COUNT),
    "sift_groundtruth.ivecs": (SIFT_GT_DEPTH, SIFT_QUERY_COUNT),
}
FETCH_TIMEOUT_SECONDS: int = 120


def sha256_of(path: Path) -> str:
    """Compute the sha256 digest of a file.

    Args:
        path: The file to hash.

    Returns:
        The hex digest.
    """
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch_url(url: str, destination: Path) -> None:
    """Stream a URL to a local file.

    Args:
        url: The source URL; http, https, and ftp schemes are supported by urllib.
        destination: The local target path, written atomically via a temp suffix.
    """
    partial: Path = destination.with_suffix(destination.suffix + ".partial")
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as response, open(partial, "wb") as sink:
        shutil.copyfileobj(response, sink)
    partial.rename(destination)


def validate_sift_directory(directory: Path) -> dict[str, Any]:
    """Validate the structure of every extracted SIFT1M file.

    Args:
        directory: The directory holding the three corpus files.

    Returns:
        Per-file dimension and count metadata.

    Raises:
        ValueError: If a file is missing or its dimension or count differs from the published SIFT1M shape.
    """
    info: dict[str, Any] = {}
    for name, (dimension, count) in EXPECTED_SHAPES.items():
        path: Path = directory / name
        if not path.exists():
            raise ValueError(f"missing corpus file {path}")
        actual_dimension: int = vecs_dimension(path)
        actual_count: int = vecs_count(path)
        if actual_dimension != dimension or actual_count != count:
            raise ValueError(
                f"{path}: expected dim {dimension} x {count} rows, found dim {actual_dimension} x {actual_count}"
            )
        info[name] = {"dimension": actual_dimension, "count": actual_count}
    return info


def verify_recorded_checksums(directory: Path) -> bool:
    """Compare current file digests against the recorded checksum manifest.

    Args:
        directory: The corpus directory holding ``checksums.json``.

    Returns:
        ``True`` when a manifest exists and every digest matches.
    """
    manifest: Path = directory / "checksums.json"
    if not manifest.exists():
        return False
    recorded: dict[str, Any] = read_json(manifest)
    return all(sha256_of(directory / name) == recorded.get(name) for name in SIFT_FILE_NAMES)


def record_checksums(directory: Path) -> dict[str, str]:
    """Record the sha256 of every corpus file into ``checksums.json``.

    Args:
        directory: The corpus directory.

    Returns:
        The name-to-digest mapping that was recorded.
    """
    digests: dict[str, str] = {name: sha256_of(directory / name) for name in SIFT_FILE_NAMES}
    write_json(directory / "checksums.json", dict(digests))
    return digests


def extract_corpus(archive: Path, directory: Path) -> None:
    """Extract the three corpus files from the sift tarball.

    Args:
        archive: The downloaded ``sift.tar.gz``.
        directory: The destination corpus directory.
    """
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            base_name: str = Path(member.name).name
            if member.isfile() and base_name in SIFT_FILE_NAMES:
                source = tar.extractfile(member)
                if source is None:
                    continue
                with source, open(directory / base_name, "wb") as sink:
                    shutil.copyfileobj(source, sink)


def download_tarball(config: BenchConfig) -> tuple[Path | None, str | None]:
    """Try each tarball URL until one succeeds.

    Args:
        config: Benchmark configuration.

    Returns:
        The archive path and source URL, or ``(None, None)`` when every URL failed.
    """
    archive: Path = config.workspace / "sift.tar.gz"
    if archive.exists():
        return archive, "cached"
    for url in TARBALL_URLS:
        try:
            logger.info("downloading %s", url)
            fetch_url(url, archive)
            return archive, url
        except OSError as error:
            logger.warning("download failed for %s: %s", url, error)
    return None, None


def download_mirrored_files(config: BenchConfig, directory: Path) -> str:
    """Fetch the three corpus files from per-file HTTP mirrors.

    Args:
        config: Benchmark configuration.
        directory: The destination corpus directory.

    Returns:
        The mirror template that served the files.

    Raises:
        RuntimeError: If no mirror could serve every file.
    """
    del config
    for template in MIRROR_TEMPLATES:
        try:
            for name in SIFT_FILE_NAMES:
                target: Path = directory / name
                if not target.exists():
                    logger.info("downloading %s", template.format(name=name))
                    fetch_url(template.format(name=name), target)
            return template
        except OSError as error:
            logger.warning("mirror failed for %s: %s", template, error)
    raise RuntimeError("every download source failed; fetch sift.tar.gz manually into the workspace")


def run_download(config: BenchConfig) -> dict[str, Any]:
    """Fetch, checksum-verify, and extract the SIFT1M corpus idempotently.

    Args:
        config: Benchmark configuration.

    Returns:
        The phase result document.
    """
    directory: Path = ensure_dir(config.sift_dir())
    if all((directory / name).exists() for name in SIFT_FILE_NAMES):
        info: dict[str, Any] = validate_sift_directory(directory)
        checks_match: bool = verify_recorded_checksums(directory)
        if not checks_match:
            record_checksums(directory)
        return save_phase(config, "download", {"skipped": True, "files": info, "checksums_verified": checks_match})

    source: str | None
    archive, source = download_tarball(config)
    if archive is not None:
        archive_digest: str = sha256_of(archive)
        if config.sha256 is not None and archive_digest != config.sha256:
            raise ValueError(f"sift.tar.gz sha256 {archive_digest} does not match pinned {config.sha256}")
        extract_corpus(archive, directory)
    else:
        source = download_mirrored_files(config, directory)
        archive_digest = ""
    info = validate_sift_directory(directory)
    digests: dict[str, str] = record_checksums(directory)
    return save_phase(
        config,
        "download",
        {"skipped": False, "source": source, "archive_sha256": archive_digest, "files": info, "checksums": digests},
    )
