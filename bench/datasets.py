"""Dataset adapter seam decoupling the benchmark phases from any one corpus.

Every phase consumes a :class:`DatasetAdapter` resolved from the ``--dataset`` flag through :func:`adapter_for`, so a
new corpus (GIST1M, DEEP, a text collection) plugs in by implementing the adapter surface and registering an instance
with :func:`register_adapter`. The adapter owns acquisition (:meth:`DatasetAdapter.download`), raw vector access for
both the driver and Spark executor tasks (:meth:`DatasetAdapter.base_vectors`,
:meth:`DatasetAdapter.base_vector_slice`, :meth:`DatasetAdapter.query_vectors`), the optional published ground truth
(:meth:`DatasetAdapter.ground_truth`, ``None`` means the prepare phase computes exact brute-force truth), and the
per-row document text hook (:meth:`DatasetAdapter.text_for_row`, defaulting to the synthetic cluster-seeded corpus in
:mod:`bench.corpus`). Adapters must be picklable because prepare broadcasts them into ``mapInArrow`` closures.

:class:`Sift1mAdapter` carries all SIFT1M specifics that previously lived across the download and prepare phases: the
IRISA tarball URLs, the per-file HuggingFace mirrors, the published shapes, the checksum manifest handling, and the
fvecs/ivecs readers. :class:`SyntheticAdapter` generates a deterministic in-memory Gaussian corpus with no download at
all, which backs the offline tiny-data integration tests.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import tarfile
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

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
    def download(self, workspace: Path, sha256: str | None = None) -> dict[str, Any]:
        """Fetch and verify the corpus into the workspace, idempotently.

        Args:
            workspace: The benchmark workspace directory.
            sha256: Optional pinned digest of the canonical archive.

        Returns:
            The download phase payload describing what happened.
        """

    @abstractmethod
    def base_vectors(self, workspace: Path, limit: int | None = None) -> np.ndarray:
        """Return base vectors from the start of the corpus.

        Args:
            workspace: The benchmark workspace directory.
            limit: Optional cap on rows returned.

        Returns:
            A float32 ``(rows, dimension)`` array.
        """

    @abstractmethod
    def base_vector_slice(self, workspace: Path, start: int, count: int) -> np.ndarray:
        """Return one contiguous base-vector slice, called inside Spark executor tasks.

        Args:
            workspace: The benchmark workspace directory.
            start: First global row index of the slice.
            count: Rows in the slice.

        Returns:
            A float32 ``(count, dimension)`` array.
        """

    @abstractmethod
    def query_vectors(self, workspace: Path) -> np.ndarray:
        """Return the full query matrix.

        Args:
            workspace: The benchmark workspace directory.

        Returns:
            A float32 ``(num_queries, dimension)`` array.
        """

    @abstractmethod
    def ground_truth(self, workspace: Path) -> np.ndarray | None:
        """Return the published full-corpus ground truth, or ``None`` when none exists.

        ``None`` instructs the prepare phase to compute exact brute-force ground truth instead.

        Args:
            workspace: The benchmark workspace directory.

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
        """Return the document text of one row. Defaults to the synthetic cluster-seeded corpus.

        Datasets with real document text (text corpora) override this and ignore the synthetic vocabularies.

        Args:
            cluster_vocab: Per-cluster synthetic vocabularies.
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

    def corpus_dir(self, workspace: Path) -> Path:
        """Return the directory holding the extracted SIFT1M files.

        Args:
            workspace: The benchmark workspace directory.

        Returns:
            The ``sift`` directory under the workspace.
        """
        return workspace / "sift"

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

    def download_tarball(self, workspace: Path) -> tuple[Path | None, str | None]:
        """Try each tarball URL until one succeeds.

        Args:
            workspace: The benchmark workspace directory.

        Returns:
            The archive path and source URL, or ``(None, None)`` when every URL failed.
        """
        archive: Path = workspace / "sift.tar.gz"
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
        raise RuntimeError("every download source failed; fetch sift.tar.gz manually into the workspace")

    def download(self, workspace: Path, sha256: str | None = None) -> dict[str, Any]:
        """Fetch, checksum-verify, and extract the SIFT1M corpus idempotently.

        Args:
            workspace: The benchmark workspace directory.
            sha256: Optional pinned digest of ``sift.tar.gz``.

        Returns:
            The download phase payload.

        Raises:
            ValueError: If the archive digest does not match the pinned digest.
        """
        directory: Path = self.corpus_dir(workspace)
        directory.mkdir(parents=True, exist_ok=True)
        if all((directory / name).exists() for name in SIFT_FILE_NAMES):
            info: dict[str, Any] = self.validate_directory(directory)
            checks_match: bool = self.verify_recorded_checksums(directory)
            if not checks_match:
                self.record_checksums(directory)
            return {"skipped": True, "files": info, "checksums_verified": checks_match}

        source: str | None
        archive, source = self.download_tarball(workspace)
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

    def base_vectors(self, workspace: Path, limit: int | None = None) -> np.ndarray:
        """Read base vectors from ``sift_base.fvecs``.

        Args:
            workspace: The benchmark workspace directory.
            limit: Optional cap on rows read from the start of the file.

        Returns:
            A float32 ``(rows, 128)`` array.
        """
        return read_fvecs(self.corpus_dir(workspace) / "sift_base.fvecs", limit=limit)

    def base_vector_slice(self, workspace: Path, start: int, count: int) -> np.ndarray:
        """Read one contiguous slice of ``sift_base.fvecs``.

        Args:
            workspace: The benchmark workspace directory.
            start: First global row index of the slice.
            count: Rows in the slice.

        Returns:
            A float32 ``(count, 128)`` array.
        """
        return read_vecs_rows(self.corpus_dir(workspace) / "sift_base.fvecs", start, count, "<f4")

    def query_vectors(self, workspace: Path) -> np.ndarray:
        """Read the full query matrix from ``sift_query.fvecs``.

        Args:
            workspace: The benchmark workspace directory.

        Returns:
            A float32 ``(10000, 128)`` array.
        """
        return read_fvecs(self.corpus_dir(workspace) / "sift_query.fvecs")

    def ground_truth(self, workspace: Path) -> np.ndarray | None:
        """Read the published ground truth from ``sift_groundtruth.ivecs``.

        Args:
            workspace: The benchmark workspace directory.

        Returns:
            An int64 ``(10000, 100)`` array of global ids.
        """
        path: Path = self.corpus_dir(workspace) / "sift_groundtruth.ivecs"
        return read_ivecs(path)[:, : self.gt_depth].astype(np.int64)


@dataclass
class SyntheticAdapter(DatasetAdapter):
    """A deterministic in-memory Gaussian corpus requiring no download.

    Vectors are regenerated on demand from the seed, so the adapter pickles as a handful of integers and every Spark
    executor task reproduces exactly the same corpus. There is no published ground truth, so the prepare phase always
    computes exact brute-force truth. The adapter backs the offline tiny-data integration tests and serves as the
    template for plugging in future datasets.

    Attributes:
        dataset_name: The registry name.
        vector_dimension: The vector dimension. Must be divisible by 8 for the IVF_RQ index build.
        base_rows: Number of base vectors.
        query_rows: Number of query vectors.
        seed: Seed of the deterministic generation.
    """

    dataset_name: str = "synthetic"
    vector_dimension: int = 16
    base_rows: int = 2_000
    query_rows: int = 50
    seed: int = 7

    @property
    def name(self) -> str:
        """Return the configured registry name."""
        return self.dataset_name

    @property
    def dimension(self) -> int:
        """Return the configured vector dimension."""
        return self.vector_dimension

    @property
    def base_count(self) -> int:
        """Return the configured base-vector count."""
        return self.base_rows

    @property
    def gt_depth(self) -> int:
        """Return the brute-force ground-truth depth, clamped to the corpus size."""
        return min(SIFT_GT_DEPTH, self.base_rows)

    def base_matrix(self) -> np.ndarray:
        """Generate the full deterministic base matrix.

        Returns:
            A float32 ``(base_rows, dimension)`` array.
        """
        rng: np.random.Generator = np.random.default_rng([self.seed, 11])
        return rng.normal(size=(self.base_rows, self.vector_dimension)).astype(np.float32)

    def download(self, workspace: Path, sha256: str | None = None) -> dict[str, Any]:
        """Report that a synthetic corpus needs no acquisition.

        Args:
            workspace: The benchmark workspace directory. Unused.
            sha256: Ignored. Nothing is fetched.

        Returns:
            A skip payload.
        """
        del workspace, sha256
        return {"skipped": True, "reason": "synthetic dataset; nothing to download"}

    def base_vectors(self, workspace: Path, limit: int | None = None) -> np.ndarray:
        """Return base vectors from the start of the generated corpus.

        Args:
            workspace: The benchmark workspace directory. Unused.
            limit: Optional cap on rows returned.

        Returns:
            A float32 ``(rows, dimension)`` array.
        """
        del workspace
        matrix: np.ndarray = self.base_matrix()
        return matrix if limit is None else matrix[:limit]

    def base_vector_slice(self, workspace: Path, start: int, count: int) -> np.ndarray:
        """Return one contiguous slice of the generated corpus.

        Args:
            workspace: The benchmark workspace directory. Unused.
            start: First global row index of the slice.
            count: Rows in the slice.

        Returns:
            A float32 ``(count, dimension)`` array.
        """
        del workspace
        return self.base_matrix()[start : start + count]

    def query_vectors(self, workspace: Path) -> np.ndarray:
        """Return the deterministic query matrix.

        Args:
            workspace: The benchmark workspace directory. Unused.

        Returns:
            A float32 ``(query_rows, dimension)`` array.
        """
        del workspace
        rng: np.random.Generator = np.random.default_rng([self.seed, 12])
        return rng.normal(size=(self.query_rows, self.vector_dimension)).astype(np.float32)

    def ground_truth(self, workspace: Path) -> np.ndarray | None:
        """Return ``None``: the prepare phase computes exact brute-force ground truth.

        Args:
            workspace: The benchmark workspace directory. Unused.

        Returns:
            Always ``None``.
        """
        del workspace
        return None


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

    Args:
        config: Benchmark configuration carrying the ``--dataset`` name.

    Returns:
        The registered adapter.

    Raises:
        ValueError: If no adapter is registered under the configured name.
    """
    adapter: DatasetAdapter | None = DATASET_ADAPTERS.get(config.dataset)
    if adapter is None:
        known: str = ", ".join(sorted(DATASET_ADAPTERS))
        raise ValueError(f"unknown dataset {config.dataset!r}; registered adapters: {known}")
    return adapter


register_adapter(Sift1mAdapter())
register_adapter(SyntheticAdapter())
