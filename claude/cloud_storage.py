"""Cloud-agnostic object-store helpers shared by the pipeline jobs.

The pipeline can run against AWS S3, Google Cloud Storage, or Azure Blob
Storage. pylance reaches the datasets through its own Rust object-store layer,
driven by each job's ``storage_options``, so it is already portable. This module
exists for the one place that needs a filesystem of its own: the IVF_RQ artifact
sidecar in the indexing job, which reads and writes plain files next to a
dataset (the trained centroids, the RaBitQ model, and a manifest). That work
runs only on the driver.

:func:`resolve_filesystem` builds the right ``pyarrow.fs`` filesystem for any of
the three providers from the same ``storage_options`` mapping pylance uses,
falling back to the ambient credential chain (an instance role, GCS Application
Default Credentials, or an Azure managed identity) when no explicit credentials
are supplied. Ambient credentials are the recommended setup on cloud compute and
the most portable, so prefer them.

Two provider caveats that follow from pyarrow's constructors rather than this
code:

- GCS: pyarrow's ``GcsFileSystem`` accepts an OAuth ``access_token`` but not a
  service-account JSON file or its serialized contents. If you authenticate to
  GCS with a service-account file (``google_service_account`` in
  ``storage_options``), make that file visible to the driver through the
  ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable so Application Default
  Credentials picks it up; pylance reads the same file for the dataset.
- Azure: ``AzureFileSystem`` requires ``account_name``. With a managed identity
  you may supply only ``account_name`` and omit the key.

The ``pyarrow.fs`` constructor keyword names for GCS and Azure have shifted
across pyarrow versions; verify them against the installed pyarrow if explicit
credentials are passed for those providers.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import pyarrow.fs as pa_fs


class CloudProvider(str, Enum):
    """The object-store backend selected by a URI scheme."""

    AWS = "aws"
    GCS = "gcs"
    AZURE = "azure"
    LOCAL = "local"


AWS_SCHEMES: Tuple[str, ...] = ("s3", "s3a")
GCS_SCHEMES: Tuple[str, ...] = ("gs", "gcs")
AZURE_SCHEMES: Tuple[str, ...] = ("az", "azure", "abfs", "abfss", "adl")

S3_OPTION_KEYS: Dict[str, str] = {
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
GCS_OPTION_KEYS: Dict[str, str] = {
    "access_token": "access_token",
    "google_access_token": "access_token",
    "endpoint": "endpoint_override",
    "endpoint_url": "endpoint_override",
    "default_bucket_location": "default_bucket_location",
}
AZURE_OPTION_KEYS: Dict[str, str] = {
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

    The leading ``container@account`` form used by some Azure URIs is reduced to
    the container, which is what ``pyarrow.fs`` addresses.

    Args:
        uri: The object URI.

    Returns:
        The bucket-or-container-relative path.
    """
    parsed = urlparse(uri)
    netloc: str = parsed.netloc.split("@", 1)[0]
    return f"{netloc}{parsed.path}"


def map_storage_options(
    storage_options: Optional[Dict[str, Any]], mapping: Dict[str, str]
) -> Dict[str, Any]:
    """Translate pylance ``storage_options`` to pyarrow constructor keywords.

    Args:
        storage_options: The options passed to the job, or ``None``.
        mapping: The provider's option-name to pyarrow-keyword mapping.

    Returns:
        Only the keywords pyarrow accepts, omitting anything not present.
    """
    options: Dict[str, Any] = storage_options or {}
    resolved: Dict[str, Any] = {}
    for source, target in mapping.items():
        if source in options:
            resolved[target] = options[source]
    return resolved


def resolve_filesystem(
    uri: str, storage_options: Optional[Dict[str, Any]]
) -> Tuple[Any, str]:
    """Resolve a pyarrow filesystem and path for an object URI on any provider.

    Explicit credentials from ``storage_options`` are applied when present;
    otherwise the provider's filesystem is constructed with no credentials so it
    uses the ambient credential chain. Local and unknown schemes are resolved by
    pyarrow directly.

    Args:
        uri: The object URI to address.
        storage_options: The same options passed to pylance, or ``None``.

    Returns:
        A ``(filesystem, path)`` pair for the URI.
    """
    provider: CloudProvider = provider_for_uri(uri)
    if provider is CloudProvider.LOCAL:
        filesystem, path = pa_fs.FileSystem.from_uri(uri)
        return filesystem, path

    path = object_path(uri)
    if provider is CloudProvider.AWS:
        kwargs: Dict[str, Any] = map_storage_options(storage_options, S3_OPTION_KEYS)
        return (pa_fs.S3FileSystem(**kwargs) if kwargs else pa_fs.S3FileSystem()), path
    if provider is CloudProvider.GCS:
        kwargs = map_storage_options(storage_options, GCS_OPTION_KEYS)
        return (pa_fs.GcsFileSystem(**kwargs) if kwargs else pa_fs.GcsFileSystem()), path
    kwargs = map_storage_options(storage_options, AZURE_OPTION_KEYS)
    return pa_fs.AzureFileSystem(**kwargs), path


def write_object(filesystem: Any, path: str, data: bytes) -> None:
    """Write bytes to a path on a resolved filesystem.

    Args:
        filesystem: The pyarrow filesystem.
        path: The destination path.
        data: The bytes to write.
    """
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
