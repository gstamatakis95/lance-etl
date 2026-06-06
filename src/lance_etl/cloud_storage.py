"""Cloud-agnostic object-store helpers shared by the pipeline jobs.

The pipeline can run against AWS S3, Google Cloud Storage, or Azure Blob Storage. pylance reaches the datasets through
its own Rust object-store layer, driven by each job's ``storage_options``, so it is already portable. This module exists
for the places that need a filesystem of their own: the IVF_RQ artifact sidecar in the indexing job, which reads and
writes plain files next to a dataset (the trained centroids, the RaBitQ model, and a manifest), and
:func:`discover_datasets`, which recursively enumerates ``*.lance`` datasets at any depth under a base URI for the
compact and index subcommands. That work runs only on the driver.

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

from enum import StrEnum
from typing import Any
from urllib.parse import urlparse

import pyarrow.fs as pa_fs


class CloudProvider(StrEnum):
    """The object-store backend selected by a URI scheme."""

    AWS = "aws"
    GCS = "gcs"
    AZURE = "azure"
    LOCAL = "local"


AWS_SCHEMES: tuple[str, ...] = ("s3", "s3a")
GCS_SCHEMES: tuple[str, ...] = ("gs", "gcs")
AZURE_SCHEMES: tuple[str, ...] = ("az", "azure", "abfs", "abfss", "adl")

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


def provider_for_uri(uri: str) -> CloudProvider:
    """Return the cloud provider implied by a URI scheme.

    Args:
        uri: The object or dataset URI.

    Returns:
        The matching provider, or ``LOCAL`` for file and unknown schemes.
    """
    scheme: str = urlparse(uri).scheme.lower()
    if scheme in AWS_SCHEMES:
        return CloudProvider.AWS
    if scheme in GCS_SCHEMES:
        return CloudProvider.GCS
    if scheme in AZURE_SCHEMES:
        return CloudProvider.AZURE
    return CloudProvider.LOCAL


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

    if provider is CloudProvider.LOCAL:
        filesystem, path = pa_fs.FileSystem.from_uri(uri)
        return filesystem, path

    path = object_path(uri)

    if provider is CloudProvider.AWS:
        kwargs: dict[str, Any] = map_storage_options(storage_options, S3_OPTION_KEYS)
        if not kwargs:
            filesystem, resolved_path = pa_fs.FileSystem.from_uri(uri)
            return filesystem, resolved_path
        return pa_fs.S3FileSystem(**kwargs), path

    if provider is CloudProvider.GCS:
        kwargs = map_storage_options(storage_options, GCS_OPTION_KEYS)
        if not kwargs:
            filesystem, resolved_path = pa_fs.FileSystem.from_uri(uri)
            return filesystem, resolved_path
        validate_gcs_kwargs(kwargs)
        return pa_fs.GcsFileSystem(**kwargs), path

    kwargs = map_storage_options(storage_options, AZURE_OPTION_KEYS)
    if not kwargs:
        filesystem, resolved_path = pa_fs.FileSystem.from_uri(uri)
        return filesystem, resolved_path
    if "account_name" not in kwargs:
        raise ValueError(
            "AzureFileSystem requires 'account_name'. "
            "Add 'account_name' or 'azure_storage_account_name' to storage_options."
        )
    return pa_fs.AzureFileSystem(**kwargs), path


def discover_datasets(base_uri: str, storage_options: dict[str, Any] | None = None) -> list[str]:
    """Recursively discover Lance datasets at any depth under a base URI.

    Walks the base location with one recursive listing on the resolved filesystem (local or any supported object
    store) and returns every distinct path whose component name ends in ``.lance``, however deep it sits. This keeps
    discovery depth-agnostic: the historical ``{org}/{tenant}/{namespace}.lance`` layout and deeper custom partition
    hierarchies such as ``{org}/{tenant}/{namespace}/{event_date}.lance`` are both picked up. Entries inside a dataset
    are collapsed to the dataset path, and sidecar directories such as ``{dataset}.lance.artifacts`` do not match
    because their final component does not end in ``.lance``.

    Args:
        base_uri: Root location under which datasets live, in any supported URI scheme or a local path.
        storage_options: The same options passed to pylance, or ``None``.

    Returns:
        The discovered dataset URIs, rooted at ``base_uri`` and sorted.
    """
    filesystem, base_path = resolve_filesystem(base_uri, storage_options)
    base: str = base_path.rstrip("/")
    selector: pa_fs.FileSelector = pa_fs.FileSelector(base, recursive=True, allow_not_found=True)
    datasets: set[str] = set()
    for info in filesystem.get_file_info(selector):
        relative: str = info.path[len(base) :].lstrip("/")
        components: list[str] = relative.split("/")
        for depth, component in enumerate(components):
            if component.endswith(".lance"):
                datasets.add("/".join(components[: depth + 1]))
                break
    root: str = base_uri.rstrip("/")
    return sorted(f"{root}/{path}" for path in datasets)


def write_object(filesystem: Any, path: str, data: bytes) -> None:
    """Write bytes to a path on a resolved filesystem.

    The parent directory is created first because local filesystems require it. On object stores ``create_dir`` is a
    harmless no-op since directories are implicit there.

    Args:
        filesystem: The pyarrow filesystem.
        path: The destination path.
        data: The bytes to write.
    """
    parent: str = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        filesystem.create_dir(parent, recursive=True)
    with filesystem.open_output_stream(path) as stream:
        stream.write(data)


def read_object(filesystem: Any, path: str) -> bytes:
    """Read all bytes from a path on a resolved filesystem.

    Args:
        filesystem: The pyarrow filesystem.
        path: The source path.

    Returns:
        The object's bytes.
    """
    with filesystem.open_input_stream(path) as stream:
        return stream.read()


def object_exists(filesystem: Any, path: str) -> bool:
    """Report whether a path is an existing file on a resolved filesystem.

    Args:
        filesystem: The pyarrow filesystem.
        path: The path to test.

    Returns:
        ``True`` if a file exists at the path.
    """
    return filesystem.get_file_info(path).type == pa_fs.FileType.File
