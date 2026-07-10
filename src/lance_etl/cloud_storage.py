"""Cloud-agnostic object-store helpers shared by the pipeline jobs.

The pipeline can run against AWS S3, Google Cloud Storage, or Azure Blob Storage. pylance reaches the datasets through
its own Rust object-store layer, driven by each job's ``storage_options``, so it is already portable. This module
provides :func:`resolve_filesystem` for callers that need a pyarrow filesystem handle and :func:`discover_datasets`,
which recursively enumerates ``*.lance`` datasets at any depth under a base URI for the compact and index subcommands.
Discovery runs on the driver by default and fans the per-prefix listings out across Spark executors when a session is
supplied, which is what keeps a fleet of around a million tiny datasets enumerable in minutes instead of hours.

:func:`resolve_filesystem` builds the right ``pyarrow.fs`` filesystem for any of the three providers from the same
``storage_options`` mapping pylance uses. When no explicit credentials are supplied it delegates to
``pyarrow.fs.FileSystem.from_uri``, which honours the process environment (``AWS_*`` env vars for S3,
``GOOGLE_APPLICATION_CREDENTIALS`` / ADC for GCS). Note that ``AzureFileSystem`` has no ambient-credential path —
``account_name`` is always required, so Azure without explicit ``storage_options`` falls through to
``pa_fs.FileSystem.from_uri`` as well.

Provider caveats:

- GCS: ``GcsFileSystem`` requires ``access_token`` and ``credential_token_expiration`` to be paired. Passing only
  ``access_token`` raises ``ValueError`` (``pyarrow/_gcsfs.pyx:108-111``). If you authenticate with a service-account
  file use ``GOOGLE_APPLICATION_CREDENTIALS`` and omit both keys so Application Default Credentials picks it up.
- Azure: ``AzureFileSystem`` requires ``account_name`` as a positional argument (``pyarrow/_azurefs.pyx:110``).
  Supplying no ``account_name`` raises ``TypeError``. This module rejects that combination early with a clear error.
  With a managed identity supply ``account_name`` and omit the key.

The ``pyarrow.fs`` constructor keyword names for GCS and Azure have shifted across pyarrow versions. Verify them against
the installed pyarrow if explicit credentials are passed for those providers.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from enum import StrEnum
from typing import Any
from urllib.parse import urlparse

import pyarrow.fs as pa_fs
from pyspark.sql import SparkSession


class CloudProvider(StrEnum):
    """The object-store backend selected by a URI scheme."""

    AWS = "aws"
    GCS = "gcs"
    AZURE = "azure"
    LOCAL = "local"


SCHEME_PROVIDERS: dict[str, CloudProvider] = {
    "s3": CloudProvider.AWS,
    "s3a": CloudProvider.AWS,
    "gs": CloudProvider.GCS,
    "gcs": CloudProvider.GCS,
    "az": CloudProvider.AZURE,
    "azure": CloudProvider.AZURE,
    "abfs": CloudProvider.AZURE,
    "abfss": CloudProvider.AZURE,
    "adl": CloudProvider.AZURE,
}

S3_OPTION_KEYS: dict[str, str] = {
    "access_key_id": "access_key",
    "aws_access_key_id": "access_key",
    "secret_access_key": "secret_key",
    "aws_secret_access_key": "secret_key",
    "session_token": "session_token",
    "aws_session_token": "session_token",
    "region": "region",
    "aws_region": "region",
    "endpoint": "endpoint_override",
    "endpoint_url": "endpoint_override",
    "aws_endpoint": "endpoint_override",
}
GCS_OPTION_KEYS: dict[str, str] = {
    "access_token": "access_token",
    "google_access_token": "access_token",
    "credential_token_expiration": "credential_token_expiration",
    "endpoint": "endpoint_override",
    "endpoint_url": "endpoint_override",
    "default_bucket_location": "default_bucket_location",
}
AZURE_OPTION_KEYS: dict[str, str] = {
    "account_name": "account_name",
    "azure_storage_account_name": "account_name",
    "account_key": "account_key",
    "azure_storage_account_key": "account_key",
    "sas_token": "sas_token",
    "sas_key": "sas_token",
    "azure_storage_sas_token": "sas_token",
    "azure_storage_sas_key": "sas_token",
}
PROVIDER_OPTION_KEYS: dict[CloudProvider, dict[str, str]] = {
    CloudProvider.AWS: S3_OPTION_KEYS,
    CloudProvider.GCS: GCS_OPTION_KEYS,
    CloudProvider.AZURE: AZURE_OPTION_KEYS,
}


def provider_for_uri(uri: str) -> CloudProvider:
    """Return the cloud provider implied by a URI scheme.

    Args:
        uri: The object or dataset URI.

    Returns:
        The matching provider, or ``LOCAL`` for file and unknown schemes.
    """
    return SCHEME_PROVIDERS.get(urlparse(uri).scheme.lower(), CloudProvider.LOCAL)


def object_path(uri: str) -> str:
    """Return the provider-relative path pyarrow expects for a URI.

    The leading ``container@account`` form used by some Azure URIs is reduced to the container, which is what
    ``pyarrow.fs`` addresses.

    Args:
        uri: The object URI.

    Returns:
        The bucket-or-container-relative path.
    """
    parsed = urlparse(uri)
    netloc: str = parsed.netloc.split("@", 1)[0]
    return f"{netloc}{parsed.path}"


def map_storage_options(storage_options: dict[str, Any] | None, mapping: dict[str, str]) -> dict[str, Any]:
    """Translate pylance ``storage_options`` to pyarrow constructor keywords.

    Args:
        storage_options: The options passed to the job, or ``None``.
        mapping: The provider's option-name to pyarrow-keyword mapping.

    Returns:
        Only the keywords pyarrow accepts, omitting anything not present.
    """
    options: dict[str, Any] = storage_options or {}
    resolved: dict[str, Any] = {}
    for source, target in mapping.items():
        if source in options:
            resolved[target] = options[source]
    return resolved


def validate_gcs_kwargs(kwargs: dict[str, Any]) -> None:
    """Raise ``ValueError`` when ``access_token`` is present without its required pair.

    ``GcsFileSystem`` requires ``access_token`` and ``credential_token_expiration`` to be supplied together
    (``pyarrow/_gcsfs.pyx:108-111``). Passing only the token raises inside the Cython constructor with a confusing
    message. This helper surfaces the problem early with an actionable one.

    Args:
        kwargs: The mapped GCS constructor keywords to validate.
    """
    has_token = "access_token" in kwargs
    has_expiry = "credential_token_expiration" in kwargs
    if has_token and not has_expiry:
        raise ValueError(
            "GcsFileSystem requires 'credential_token_expiration' whenever 'access_token' is supplied. "
            "Add 'credential_token_expiration' to storage_options, or remove 'access_token' and rely on "
            "GOOGLE_APPLICATION_CREDENTIALS / Application Default Credentials instead."
        )


def resolve_filesystem(uri: str, storage_options: dict[str, Any] | None) -> tuple[Any, str]:
    """Resolve a pyarrow filesystem and path for an object URI on any provider.

    When ``storage_options`` contains explicit credentials the appropriate provider-specific ``pyarrow.fs`` constructor
    is called with those credentials. When no explicit credentials are present the function delegates entirely to
    ``pa_fs.FileSystem.from_uri``, which lets pyarrow discover credentials through the process environment (AWS
    credential chain, GCS Application Default Credentials). Azure has no ambient-credential path in pyarrow, so it
    always requires ``account_name`` in ``storage_options``.

    Args:
        uri: The object URI to address.
        storage_options: The same options passed to pylance, or ``None``.

    Returns:
        A ``(filesystem, path)`` pair for the URI.

    Raises:
        ValueError: If GCS ``access_token`` is supplied without ``credential_token_expiration``, or if Azure
            ``storage_options`` contains no ``account_name``.
    """
    provider: CloudProvider = provider_for_uri(uri)
    kwargs: dict[str, Any] = map_storage_options(storage_options, PROVIDER_OPTION_KEYS.get(provider, {}))
    if provider is CloudProvider.LOCAL or not kwargs:
        filesystem, path = pa_fs.FileSystem.from_uri(uri)
        return filesystem, path

    path = object_path(uri)
    if provider is CloudProvider.AWS:
        return pa_fs.S3FileSystem(**kwargs), path
    if provider is CloudProvider.GCS:
        validate_gcs_kwargs(kwargs)
        return pa_fs.GcsFileSystem(**kwargs), path
    if "account_name" not in kwargs:
        raise ValueError(
            "AzureFileSystem requires 'account_name'. "
            "Add 'account_name' or 'azure_storage_account_name' to storage_options."
        )
    return pa_fs.AzureFileSystem(**kwargs), path


def list_dataset_paths(filesystem: Any, base_path: str, subpath: str | None = None) -> set[str]:
    """Flat-recursively list one location and collapse its entries to base-relative dataset paths.

    One recursive listing enumerates every object under ``base_path`` (or ``base_path/subpath``), and each entry's
    path is scanned for its first component ending in ``.lance`` — entries inside a dataset collapse to the dataset
    path, and sidecar directories such as ``{dataset}.lance.artifacts`` do not match because their final component
    does not end in ``.lance``. On object stores the flat recursive listing is the request-optimal strategy for
    shallow datasets (one paginated LIST page per ~1000 objects), which is why discovery never walks
    directory-by-directory.

    Takes an already-resolved filesystem handle so a caller that lists many subpaths (such as the executor fan-out
    in :func:`discover_datasets`) can resolve the filesystem once per task and reuse it across every listing,
    instead of paying the resolution cost per subpath.

    Args:
        filesystem: An already-resolved ``pyarrow.fs`` filesystem, as returned by :func:`resolve_filesystem`.
        base_path: The provider-relative base path matching ``filesystem``, as returned by
            :func:`resolve_filesystem`.
        subpath: Base-relative prefix to list instead of the whole base, used by the executor fan-out.

    Returns:
        Distinct dataset paths relative to ``base_path``.
    """
    base: str = base_path.rstrip("/")
    target: str = f"{base}/{subpath}" if subpath else base
    selector: pa_fs.FileSelector = pa_fs.FileSelector(target, recursive=True, allow_not_found=True)
    datasets: set[str] = set()
    for info in filesystem.get_file_info(selector):
        relative: str = info.path[len(base) :].lstrip("/")
        components: list[str] = relative.split("/")
        for depth, component in enumerate(components):
            if component.endswith(".lance"):
                datasets.add("/".join(components[: depth + 1]))
                break
    return datasets


def dataset_paths_under(base_uri: str, storage_options: dict[str, Any] | None, subpath: str | None = None) -> set[str]:
    """Resolve a filesystem for ``base_uri`` and list one location's base-relative dataset paths.

    Thin wrapper around :func:`list_dataset_paths` for single-shot callers. Resolves its own filesystem so the
    function is safe to run inside a Spark executor task without pickling a filesystem handle. Callers that need to
    list multiple subpaths in the same task should resolve the filesystem once with :func:`resolve_filesystem` and
    call :func:`list_dataset_paths` directly for each subpath instead of calling this function in a loop.

    Args:
        base_uri: Root location under which datasets live, in any supported URI scheme or a local path.
        storage_options: The same options passed to pylance, or ``None``.
        subpath: Base-relative prefix to list instead of the whole base, used by the executor fan-out.

    Returns:
        Distinct dataset paths relative to ``base_uri``.
    """
    filesystem, base_path = resolve_filesystem(base_uri, storage_options)
    return list_dataset_paths(filesystem, base_path, subpath)


def discover_datasets(
    base_uri: str,
    storage_options: dict[str, Any] | None = None,
    spark: SparkSession | None = None,
    partitions: int = 64,
) -> list[str]:
    """Recursively discover Lance datasets at any depth under a base URI.

    Depth-agnostic: the historical ``{org}/{tenant}/{namespace}.lance`` layout and deeper custom partition
    hierarchies such as ``{org}/{tenant}/{namespace}/{event_date}.lance`` are both picked up, via the collapse
    rules of :func:`dataset_paths_under`.

    Without a Spark session the whole walk runs on the driver, exactly as before. With one, the driver performs a
    single non-recursive listing of the base — first-level entries ending ``.lance`` are datasets directly (their
    internals are never listed) — and fans the remaining first-level directory prefixes out across executors, each
    task flat-listing its own subtree. Total request count is unchanged; wall time divides by executor parallelism
    and the driver holds dataset URIs instead of every object entry. One whale prefix still lists as a single
    paginated task, which is acceptable skew.

    Args:
        base_uri: Root location under which datasets live, in any supported URI scheme or a local path.
        storage_options: The same options passed to pylance, or ``None``.
        spark: Active session for the executor fan-out, or ``None`` for the pure-driver walk.
        partitions: Upper bound on Spark partitions for the fan-out, capped at the prefix count.

    Returns:
        The discovered dataset URIs, rooted at ``base_uri`` and sorted.
    """
    root: str = base_uri.rstrip("/")
    if spark is None:
        return sorted(f"{root}/{path}" for path in dataset_paths_under(base_uri, storage_options))

    filesystem, base_path = resolve_filesystem(base_uri, storage_options)
    base: str = base_path.rstrip("/")
    selector: pa_fs.FileSelector = pa_fs.FileSelector(base, recursive=False, allow_not_found=True)
    datasets: set[str] = set()
    prefixes: list[str] = []
    for info in filesystem.get_file_info(selector):
        name: str = info.path[len(base) :].lstrip("/")
        if name.endswith(".lance"):
            datasets.add(name)
        elif info.type == pa_fs.FileType.Directory:
            prefixes.append(name)

    if prefixes:
        options: dict[str, Any] | None = storage_options

        def list_partition(part: Iterable[str]) -> Iterator[set[str]]:
            """List the first-level prefixes assigned to this executor task.

            Resolves the filesystem once for the whole task and reuses it across every prefix in ``part``,
            rather than paying the filesystem-resolution cost once per prefix.

            Args:
                part: Base-relative directory prefixes for this partition.

            Yields:
                One base-relative dataset-path set per prefix.
            """
            task_filesystem, task_base_path = resolve_filesystem(base_uri, options)
            for prefix in part:
                yield list_dataset_paths(task_filesystem, task_base_path, prefix)

        slices: int = max(1, min(len(prefixes), partitions))
        found_sets: list[set[str]] = (
            spark.sparkContext.parallelize(sorted(prefixes), slices).mapPartitions(list_partition).collect()
        )
        for found in found_sets:
            datasets.update(found)

    return sorted(f"{root}/{path}" for path in datasets)
