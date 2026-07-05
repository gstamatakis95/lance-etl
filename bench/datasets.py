"""Dataset adapter seam decoupling the benchmark phases from any one corpus.

Every phase consumes a :class:`DatasetAdapter` resolved from the ``--dataset`` flag through :func:`adapter_for`, so a
new corpus (GIST1M, DEEP, a text collection) plugs in by implementing the adapter surface and registering an instance
with :func:`register_adapter`. The adapter owns acquisition (:meth:`DatasetAdapter.download`), raw vector access for
both the driver and Spark executor tasks (:meth:`DatasetAdapter.base_vectors`,
:meth:`DatasetAdapter.base_vector_slice`, :meth:`DatasetAdapter.query_vectors`), the optional published ground truth
(:meth:`DatasetAdapter.ground_truth`, ``None`` means the prepare phase computes exact brute-force truth), and the
per-row document text hook (:meth:`DatasetAdapter.text_for_row`, defaulting to the cluster-seeded corpus in
:mod:`bench.corpus`). Adapters must be picklable because prepare broadcasts them into ``mapInArrow`` closures.

:class:`Sift1mAdapter` carries all SIFT1M specifics that previously lived across the download and prepare phases: the
IRISA tarball URLs, the per-file HuggingFace mirrors, the published shapes, the checksum manifest handling, and the
fvecs/ivecs readers. :class:`BigannAdapter` serves the billion-scale BIGANN corpus downloading only the first
``limit`` vectors via HTTP Range requests with resume support. The registry keys are ``sift1m`` and ``bigann``.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import tarfile
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import numpy as np

from bench.bigann_io import (
    convert_bvecs_gz_to_u8bin,
    read_ivecs_from_tarball,
    read_u8bin,
    read_u8bin_slice,
    stream_bvecs_to_u8bin,
)
from bench.config import (
    SIFT_BASE_COUNT,
    SIFT_DIM,
    SIFT_FILE_NAMES,
    SIFT_GT_DEPTH,
    SIFT_QUERY_COUNT,
    BenchConfig,
)
from bench.corpus import row_text
from bench.fvecs import read_fvecs, read_ivecs, read_vecs_rows, vecs_count, vecs_dimension
from bench.results import read_json, write_json

logger: logging.Logger = logging.getLogger(__name__)

FETCH_TIMEOUT_SECONDS: int = 120
SIFT_TARBALL_URLS: tuple[str, ...] = (
    "http://corpus-texmex.irisa.fr/sift.tar.gz",
    "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz",
)
SIFT_MIRROR_TEMPLATES: tuple[str, ...] = (
    "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/{name}",
    "https://huggingface.co/datasets/maknee/sift1m/resolve/main/sift/{name}",
)
SIFT_EXPECTED_SHAPES: dict[str, tuple[int, int]] = {
    "sift_base.fvecs": (SIFT_DIM, SIFT_BASE_COUNT),
    "sift_query.fvecs": (SIFT_DIM, SIFT_QUERY_COUNT),
    "sift_groundtruth.ivecs": (SIFT_GT_DEPTH, SIFT_QUERY_COUNT),
}


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
        url: The source URL. HTTP, HTTPS, and FTP schemes are supported by urllib.
        destination: The local target path, written atomically via a temp suffix.
    """
    partial: Path = destination.with_suffix(destination.suffix + ".partial")
    with urllib.request.urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as response, open(partial, "wb") as sink:
        shutil.copyfileobj(response, sink)
    partial.rename(destination)


class DatasetAdapter(ABC):
    """Abstract corpus behind the benchmark: acquisition, vectors, ground truth, and document text.

    Implementations must be picklable: prepare ships the adapter into Spark executor closures so each task can read
    its own base-vector slice through :meth:`base_vector_slice`.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the registry name selected by ``--dataset``."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return the base/query vector dimension."""

    @property
    @abstractmethod
    def base_count(self) -> int:
        """Return the total number of base vectors the full corpus provides."""

    @property
    def metric(self) -> str:
        """Return the distance metric of the corpus, used for the vector index build."""
        return "L2"

    @property
    def gt_depth(self) -> int:
        """Return the ground-truth depth: neighbors per query in published or brute-force truth."""
        return SIFT_GT_DEPTH

    @property
    def ground_truth_source(self) -> str:
        """Return the manifest label recorded when the published ground truth is used verbatim."""
        return "published"

    @abstractmethod
    def download(self, corpus_root: Path, sha256: str | None = None) -> dict[str, Any]:
        """Fetch and verify the corpus into the shared corpus cache, idempotently.

        Args:
            corpus_root: The shared corpus cache directory, shared across workspaces.
            sha256: Optional pinned digest of the canonical archive.

        Returns:
            The download phase payload describing what happened.
        """

    @abstractmethod
    def base_vectors(self, corpus_root: Path, limit: int | None = None) -> np.ndarray:
        """Return base vectors from the start of the corpus.

        Args:
            corpus_root: The shared corpus cache directory.
            limit: Optional cap on rows returned.

        Returns:
            A float32 ``(rows, dimension)`` array.
        """

    @abstractmethod
    def base_vector_slice(self, corpus_root: Path, start: int, count: int) -> np.ndarray:
        """Return one contiguous base-vector slice, called inside Spark executor tasks.

        Args:
            corpus_root: The shared corpus cache directory.
            start: First global row index of the slice.
            count: Rows in the slice.

        Returns:
            A float32 ``(count, dimension)`` array.
        """

    @abstractmethod
    def query_vectors(self, corpus_root: Path) -> np.ndarray:
        """Return the full query matrix.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            A float32 ``(num_queries, dimension)`` array.
        """

    @abstractmethod
    def ground_truth(self, corpus_root: Path) -> np.ndarray | None:
        """Return the published full-corpus ground truth, or ``None`` when none exists.

        ``None`` instructs the prepare phase to compute exact brute-force ground truth instead.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            An int64 ``(num_queries, gt_depth)`` array of global ids, or ``None``.
        """

    def text_for_row(
        self,
        cluster_vocab: list[list[str]],
        common_vocab: list[str],
        cluster_id: int,
        global_index: int,
        seed: int,
        cluster_terms: int,
    ) -> str:
        """Return the document text of one row. Defaults to the cluster-seeded corpus.

        Datasets with real document text (text corpora) override this and ignore the cluster vocabularies.

        Args:
            cluster_vocab: Per-cluster vocabularies.
            common_vocab: Shared common-word pool.
            cluster_id: The row's coarse cluster.
            global_index: The row's global index.
            seed: The corpus seed.
            cluster_terms: Cluster-specific words per document.

        Returns:
            The document text.
        """
        return row_text(cluster_vocab, common_vocab, cluster_id, global_index, seed, cluster_terms=cluster_terms)


@dataclass
class Sift1mAdapter(DatasetAdapter):
    """The canonical SIFT1M corpus from the IRISA TexMex collection.

    The canonical distribution is ``sift.tar.gz``, historically served over FTP. The HTTP URL is tried first, urllib
    falls back to the FTP mirror, then to per-file HuggingFace mirrors. Verification is two-fold: structural (every
    file must parse as fvecs/ivecs with the published SIFT1M shapes) and a sha256 manifest recorded on first success
    and compared on later runs. A pinned archive digest may be supplied because IRISA publishes no authoritative
    checksum.
    """

    @property
    def name(self) -> str:
        """Return the registry name ``sift1m``."""
        return "sift1m"

    @property
    def dimension(self) -> int:
        """Return the SIFT descriptor dimension, 128."""
        return SIFT_DIM

    @property
    def base_count(self) -> int:
        """Return the published base-vector count, one million."""
        return SIFT_BASE_COUNT

    @property
    def ground_truth_source(self) -> str:
        """Return ``ivecs``, the historical manifest label of the published ground-truth file."""
        return "ivecs"

    def corpus_dir(self, corpus_root: Path) -> Path:
        """Return the directory holding the extracted SIFT1M files.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The ``sift`` directory under the corpus root.
        """
        return corpus_root / "sift"

    def validate_directory(self, directory: Path) -> dict[str, Any]:
        """Validate the structure of every extracted SIFT1M file.

        Args:
            directory: The directory holding the three corpus files.

        Returns:
            Per-file dimension and count metadata.

        Raises:
            ValueError: If a file is missing or its dimension or count differs from the published SIFT1M shape.
        """
        info: dict[str, Any] = {}
        for name, (dimension, count) in SIFT_EXPECTED_SHAPES.items():
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

    def verify_recorded_checksums(self, directory: Path) -> bool:
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

    def record_checksums(self, directory: Path) -> dict[str, str]:
        """Record the sha256 of every corpus file into ``checksums.json``.

        Args:
            directory: The corpus directory.

        Returns:
            The name-to-digest mapping that was recorded.
        """
        digests: dict[str, str] = {name: sha256_of(directory / name) for name in SIFT_FILE_NAMES}
        write_json(directory / "checksums.json", dict(digests))
        return digests

    def extract_corpus(self, archive: Path, directory: Path) -> None:
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

    def download_tarball(self, corpus_root: Path) -> tuple[Path | None, str | None]:
        """Try each tarball URL until one succeeds.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The archive path and source URL, or ``(None, None)`` when every URL failed.
        """
        archive: Path = corpus_root / "sift.tar.gz"
        if archive.exists():
            return archive, "cached"
        for url in SIFT_TARBALL_URLS:
            try:
                logger.info("downloading %s", url)
                fetch_url(url, archive)
                return archive, url
            except OSError as error:
                logger.warning("download failed for %s: %s", url, error)
        return None, None

    def download_mirrored_files(self, directory: Path) -> str:
        """Fetch the three corpus files from per-file HTTP mirrors.

        Args:
            directory: The destination corpus directory.

        Returns:
            The mirror template that served the files.

        Raises:
            RuntimeError: If no mirror could serve every file.
        """
        for template in SIFT_MIRROR_TEMPLATES:
            try:
                for name in SIFT_FILE_NAMES:
                    target: Path = directory / name
                    if not target.exists():
                        logger.info("downloading %s", template.format(name=name))
                        fetch_url(template.format(name=name), target)
                return template
            except OSError as error:
                logger.warning("mirror failed for %s: %s", template, error)
        raise RuntimeError("every download source failed; fetch sift.tar.gz manually into the corpus root")

    def download(self, corpus_root: Path, sha256: str | None = None) -> dict[str, Any]:
        """Fetch, checksum-verify, and extract the SIFT1M corpus idempotently.

        Args:
            corpus_root: The shared corpus cache directory.
            sha256: Optional pinned digest of ``sift.tar.gz``.

        Returns:
            The download phase payload.

        Raises:
            ValueError: If the archive digest does not match the pinned digest.
        """
        directory: Path = self.corpus_dir(corpus_root)
        directory.mkdir(parents=True, exist_ok=True)
        if all((directory / name).exists() for name in SIFT_FILE_NAMES):
            info: dict[str, Any] = self.validate_directory(directory)
            checks_match: bool = self.verify_recorded_checksums(directory)
            if not checks_match:
                self.record_checksums(directory)
            return {"skipped": True, "files": info, "checksums_verified": checks_match}

        source: str | None
        archive, source = self.download_tarball(corpus_root)
        if archive is not None:
            archive_digest: str = sha256_of(archive)
            if sha256 is not None and archive_digest != sha256:
                raise ValueError(f"sift.tar.gz sha256 {archive_digest} does not match pinned {sha256}")
            self.extract_corpus(archive, directory)
        else:
            source = self.download_mirrored_files(directory)
            archive_digest = ""
        info = self.validate_directory(directory)
        digests: dict[str, str] = self.record_checksums(directory)
        return {
            "skipped": False,
            "source": source,
            "archive_sha256": archive_digest,
            "files": info,
            "checksums": digests,
        }

    def base_vectors(self, corpus_root: Path, limit: int | None = None) -> np.ndarray:
        """Read base vectors from ``sift_base.fvecs``.

        Args:
            corpus_root: The shared corpus cache directory.
            limit: Optional cap on rows read from the start of the file.

        Returns:
            A float32 ``(rows, 128)`` array.
        """
        return read_fvecs(self.corpus_dir(corpus_root) / "sift_base.fvecs", limit=limit)

    def base_vector_slice(self, corpus_root: Path, start: int, count: int) -> np.ndarray:
        """Read one contiguous slice of ``sift_base.fvecs``.

        Args:
            corpus_root: The shared corpus cache directory.
            start: First global row index of the slice.
            count: Rows in the slice.

        Returns:
            A float32 ``(count, 128)`` array.
        """
        return read_vecs_rows(self.corpus_dir(corpus_root) / "sift_base.fvecs", start, count, "<f4")

    def query_vectors(self, corpus_root: Path) -> np.ndarray:
        """Read the full query matrix from ``sift_query.fvecs``.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            A float32 ``(10000, 128)`` array.
        """
        return read_fvecs(self.corpus_dir(corpus_root) / "sift_query.fvecs")

    def ground_truth(self, corpus_root: Path) -> np.ndarray | None:
        """Read the published ground truth from ``sift_groundtruth.ivecs``.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            An int64 ``(10000, 100)`` array of global ids.
        """
        path: Path = self.corpus_dir(corpus_root) / "sift_groundtruth.ivecs"
        return read_ivecs(path)[:, : self.gt_depth].astype(np.int64)


DATASET_ADAPTERS: dict[str, DatasetAdapter] = {}


def register_adapter(adapter: DatasetAdapter) -> DatasetAdapter:
    """Register a dataset adapter under its name for ``--dataset`` resolution.

    Args:
        adapter: The adapter instance.

    Returns:
        The same adapter, for chaining.
    """
    DATASET_ADAPTERS[adapter.name] = adapter
    return adapter


def adapter_for(config: BenchConfig) -> DatasetAdapter:
    """Resolve the dataset adapter selected by the configuration.

    For the BIGANN adapter the configured ``--limit`` is bound onto the returned instance,
    because the prefix size determines the artifact paths, the streamed byte range, and
    which published ground-truth member applies.

    Args:
        config: Benchmark configuration carrying the ``--dataset`` name.

    Returns:
        The registered adapter, parameterized by the configuration where applicable.

    Raises:
        ValueError: If no adapter is registered under the configured name.
    """
    adapter: DatasetAdapter | None = DATASET_ADAPTERS.get(config.dataset)
    if adapter is None:
        known: str = ", ".join(sorted(DATASET_ADAPTERS))
        raise ValueError(f"unknown dataset {config.dataset!r}; registered adapters: {known}")
    if isinstance(adapter, BigannAdapter):
        return replace(adapter, limit=config.limit)
    return adapter


BIGANN_DIM: int = 128
BIGANN_TOTAL_VECTORS: int = 1_000_000_000
BIGANN_QUERY_COUNT: int = 10_000
BIGANN_GT_DEPTH: int = 1_000
BIGANN_BASE_PRIMARY_URL: str = "http://corpus-texmex.irisa.fr/bigann_base.bvecs.gz"
BIGANN_BASE_FALLBACK_URL: str = "https://huggingface.co/datasets/jkhe/bigann/resolve/main/bigann_base.bvecs.gz"
BIGANN_BASE_FALLBACK_SHA256: str = "f04fa9977f930c811570646ce84649150b72bd707c54ff0dccae7d515e079479"
BIGANN_QUERY_PRIMARY_URL: str = "http://corpus-texmex.irisa.fr/bigann_query.bvecs.gz"
BIGANN_QUERY_FALLBACK_URL: str = "https://huggingface.co/datasets/jkhe/bigann/resolve/main/bigann_query.bvecs.gz"
BIGANN_GND_PRIMARY_URL: str = "http://corpus-texmex.irisa.fr/bigann_gnd.tar.gz"
BIGANN_GND_FALLBACK_URL: str = "https://huggingface.co/datasets/jkhe/bigann/resolve/main/bigann_gnd.tar.gz"
BIGANN_GT_MILLION_SIZES: frozenset[int] = frozenset({1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000})


def gt_member_name(limit: int) -> str | None:
    """Return the ivecs member path inside bigann_gnd.tar.gz for a given limit, or None.

    The IRISA ground-truth archive contains members for exactly the 10 prefix sizes:
    1M, 2M, 5M, 10M, 20M, 50M, 100M, 200M, 500M, and 1000M vectors.

    Args:
        limit: Number of base vectors (e.g. 100_000_000 for 100M).

    Returns:
        The tar member path string, or None when limit does not match any published size.
    """
    millions: int = limit // 1_000_000
    if millions not in BIGANN_GT_MILLION_SIZES or millions * 1_000_000 != limit:
        return None
    return f"gnd/idx_{millions}M.ivecs"


@dataclass
class BigannAdapter(DatasetAdapter):
    """The BIGANN billion-scale corpus from the IRISA corpus-texmex distribution.

    Base vectors are streamed from bigann_base.bvecs.gz hosted at corpus-texmex.irisa.fr
    (primary) with a HuggingFace HTTPS mirror as fallback. The stream is decompressed
    incrementally and only the first ``limit`` vectors are written to a local u8bin artifact,
    so a 100M-vector run downloads roughly 10 GB of compressed data instead of 98 GB.

    Resume is supported: compressed bytes are persisted to a .gz.partial sidecar. A
    subsequent call re-decompresses the local partial from the start (CPU-only) to rebuild
    the decompressor state and the output artifact, then resumes the HTTP transfer via a
    Range header from the sidecar's byte size.

    Query vectors come from bigann_query.bvecs.gz (10K queries, ~1 MB). The ground-truth
    tarball bigann_gnd.tar.gz is downloaded once; individual ivecs members are extracted on
    demand. Official ground truth exists for exactly 10 prefix sizes (1M, 2M, 5M, 10M, 20M,
    50M, 100M, 200M, 500M, 1000M vectors). For all other limits the prepare phase computes
    exact brute-force ground truth.

    Attributes:
        limit: Number of base vectors to use. Defaults to the BIGANN total (1B).
    """

    limit: int = BIGANN_TOTAL_VECTORS

    @property
    def name(self) -> str:
        """Return the registry name ``bigann``."""
        return "bigann"

    @property
    def dimension(self) -> int:
        """Return the BIGANN descriptor dimension, 128."""
        return BIGANN_DIM

    @property
    def base_count(self) -> int:
        """Return the configured base-vector count."""
        return self.limit

    @property
    def metric(self) -> str:
        """Return the distance metric, L2."""
        return "L2"

    @property
    def gt_depth(self) -> int:
        """Return the published ground-truth depth, 1000 neighbors per query."""
        return BIGANN_GT_DEPTH

    def corpus_dir(self, corpus_root: Path) -> Path:
        """Return the directory holding the BIGANN files.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The ``bigann`` directory under the corpus root.
        """
        return corpus_root / "bigann"

    def base_path(self, corpus_root: Path) -> Path:
        """Return the local path of the base u8bin artifact for this limit.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The base u8bin file path, named by limit for safe co-existence of multiple sizes.
        """
        return self.corpus_dir(corpus_root) / f"base.{self.limit}.u8bin"

    def query_path(self, corpus_root: Path) -> Path:
        """Return the local path of the query u8bin artifact.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The query u8bin file path.
        """
        return self.corpus_dir(corpus_root) / "query.10K.u8bin"

    def gnd_tarball_path(self, corpus_root: Path) -> Path:
        """Return the local path of the ground-truth tarball.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The bigann_gnd.tar.gz path under the corpus directory.
        """
        return self.corpus_dir(corpus_root) / "bigann_gnd.tar.gz"

    def download_query(self, corpus_root: Path) -> str:
        """Download and convert the query bvecs.gz to u8bin, idempotently.

        Tries the IRISA primary URL first, then the HuggingFace fallback. The query file is
        small (~1 MB compressed) so no streaming resume is needed; it is fetched in full and
        converted in memory.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The source URL that served the file, or ``"cached"`` if already present.
        """
        query: Path = self.query_path(corpus_root)
        if query.exists():
            return "cached"
        gz_partial: Path = query.with_suffix(".bvecs.gz")
        for url in (BIGANN_QUERY_PRIMARY_URL, BIGANN_QUERY_FALLBACK_URL):
            try:
                logger.info("downloading query bvecs.gz from %s", url)
                fetch_url(url, gz_partial)
                convert_bvecs_gz_to_u8bin(gz_partial.read_bytes(), query, BIGANN_QUERY_COUNT)
                gz_partial.unlink(missing_ok=True)
                return url
            except OSError as err:
                logger.warning("query download from %s failed: %s", url, err)
        raise RuntimeError("all query sources failed; check network connectivity")

    def download_gnd(self, corpus_root: Path) -> str:
        """Download the ground-truth tarball, idempotently.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            The source URL that served the tarball, or ``"cached"`` if already present.
        """
        tarball: Path = self.gnd_tarball_path(corpus_root)
        if tarball.exists():
            return "cached"
        for url in (BIGANN_GND_PRIMARY_URL, BIGANN_GND_FALLBACK_URL):
            try:
                logger.info("downloading ground-truth tarball from %s", url)
                fetch_url(url, tarball)
                return url
            except OSError as err:
                logger.warning("gnd download from %s failed: %s", url, err)
        raise RuntimeError("all ground-truth sources failed; check network connectivity")

    def download(self, corpus_root: Path, sha256: str | None = None) -> dict[str, Any]:
        """Stream and convert the BIGANN corpus prefix, queries, and ground truth.

        The base bvecs.gz is streamed from the IRISA primary URL with the HuggingFace mirror
        as fallback. Only the first ``limit`` vectors are decompressed and written as u8bin;
        the HTTP transfer is aborted once the limit is reached. A pinned sha256 is verified
        against the final base u8bin artifact.

        Args:
            corpus_root: The shared corpus cache directory.
            sha256: Optional pinned digest of the local base u8bin artifact.

        Returns:
            The download phase payload.

        Raises:
            ValueError: If the pinned sha256 does not match the downloaded base file.
        """
        directory: Path = self.corpus_dir(corpus_root)
        directory.mkdir(parents=True, exist_ok=True)
        base: Path = self.base_path(corpus_root)
        query: Path = self.query_path(corpus_root)
        checksums_path: Path = directory / f"checksums-{self.limit}.json"
        if base.exists() and query.exists():
            recorded: dict[str, Any] = read_json(checksums_path) if checksums_path.exists() else {}
            return {"skipped": True, "limit": self.limit, "checksums_verified": bool(recorded)}
        if not base.exists():
            logger.info("streaming bigann base prefix (%d vectors) from IRISA bvecs.gz", self.limit)
            stream_bvecs_to_u8bin(
                BIGANN_BASE_PRIMARY_URL,
                BIGANN_BASE_FALLBACK_URL,
                base,
                self.limit,
            )
        base_digest: str = sha256_of(base)
        if sha256 is not None and base_digest != sha256:
            raise ValueError(f"bigann base sha256 {base_digest} does not match pinned {sha256}")
        query_source: str = self.download_query(corpus_root)
        query_digest: str = sha256_of(query)
        member: str | None = gt_member_name(self.limit)
        gnd_source: str = ""
        gnd_digest: str = ""
        if member is not None:
            gnd_source = self.download_gnd(corpus_root)
            gnd_digest = sha256_of(self.gnd_tarball_path(corpus_root))
        digests: dict[str, str] = {"base": base_digest, "query": query_digest}
        if gnd_digest:
            digests["ground_truth_tarball"] = gnd_digest
        write_json(checksums_path, digests)
        return {
            "skipped": False,
            "limit": self.limit,
            "query_source": query_source,
            "gnd_source": gnd_source,
            "checksums": digests,
        }

    def base_vectors(self, corpus_root: Path, limit: int | None = None) -> np.ndarray:
        """Read base vectors from the local u8bin artifact, cast to float32.

        Args:
            corpus_root: The shared corpus cache directory.
            limit: Optional cap on rows read from the start.

        Returns:
            A float32 array of shape (rows, 128).
        """
        return read_u8bin(self.base_path(corpus_root), limit=limit)

    def base_vector_slice(self, corpus_root: Path, start: int, count: int) -> np.ndarray:
        """Read one contiguous slice of the base u8bin file, cast to float32.

        Args:
            corpus_root: The shared corpus cache directory.
            start: First global row index of the slice.
            count: Rows in the slice.

        Returns:
            A float32 array of shape (count, 128).
        """
        return read_u8bin_slice(self.base_path(corpus_root), start, start + count)

    def query_vectors(self, corpus_root: Path) -> np.ndarray:
        """Read the full query matrix from the local query u8bin artifact.

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            A float32 array of shape (10000, 128).
        """
        return read_u8bin(self.query_path(corpus_root))

    def ground_truth(self, corpus_root: Path) -> np.ndarray | None:
        """Return the published ground truth for this limit, or ``None`` if unavailable.

        Official ground truth is available for exactly 10 prefix sizes: 1M, 2M, 5M, 10M,
        20M, 50M, 100M, 200M, 500M, and 1000M vectors. For all other limits the prepare
        phase computes exact brute-force ground truth. The GT tarball must already be present
        (downloaded during the download phase).

        Args:
            corpus_root: The shared corpus cache directory.

        Returns:
            An int64 array of shape (10000, 1000), or ``None``.
        """
        member: str | None = gt_member_name(self.limit)
        if member is None:
            return None
        tarball: Path = self.gnd_tarball_path(corpus_root)
        if not tarball.exists():
            return None
        return read_ivecs_from_tarball(tarball, member).astype(np.int64)

    def text_for_row(
        self,
        cluster_vocab: list[list[str]],
        common_vocab: list[str],
        cluster_id: int,
        global_index: int,
        seed: int,
        cluster_terms: int,
    ) -> str:
        """Return deterministic cluster-seeded text for rows in text-enabled runs.

        BIGANN is a pure vector corpus and does not carry document text. This method
        delegates to the default cluster corpus generator so the adapter remains
        compatible with text-enabled benchmark modes when ``--no-text`` is not set.

        Args:
            cluster_vocab: Per-cluster vocabularies.
            common_vocab: Shared common-word pool.
            cluster_id: The row's coarse cluster.
            global_index: The row's global index.
            seed: The corpus seed.
            cluster_terms: Cluster-specific words per document.

        Returns:
            The cluster-seeded document text.
        """
        return row_text(cluster_vocab, common_vocab, cluster_id, global_index, seed, cluster_terms=cluster_terms)


register_adapter(Sift1mAdapter())
register_adapter(BigannAdapter())
