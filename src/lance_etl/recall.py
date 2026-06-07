"""Offline recall audit replaying sampled vector queries against pinned Lance dataset versions.

The Rust gRPC search service samples a fraction of vector queries onto Datadog spans, capturing the query vector, the
RPC search parameters, the typed filter AST as JSON, the committed Lance dataset version that served the query, and the
served result ids in rank order. This job fetches those spans, replays each query as an exact brute-force scan against
the dataset checked out at the recorded version, and reports recall@k per RPC-parameter bucket and per organization.

Span fetching goes through the :class:`SpanSource` protocol. :class:`DatadogSpanSource` talks to the Datadog Spans
search API v2 with stdlib ``urllib.request`` so no new heavy dependency is introduced. :class:`InMemorySpanSource`
feeds the same pipeline from a list of attribute dictionaries and exists for tests.

The driver groups parsed samples by ``(dataset_uri, dataset_version)`` and fans the groups out to Spark executors with
``parallelize().map()``, mirroring the established executor patterns in ``etl.py`` and ``indexing.py``. Each executor
opens its dataset checked out at the recorded version (falling back to the latest version with a drift flag when the
recorded version was cleaned up) and scores every sample in the group.

Filter replay and the no-raw-SQL rule: the repository forbids accepting raw SQL strings in the gRPC filter API because
client-supplied strings cannot be trusted. This module honors that rule even though it hands the Lance scanner a SQL
string, because the string never crosses a trust boundary. It is generated here, internally, from the typed filter AST
that the Rust service captured from its own typed ``Filter`` proto. Clients never supply strings at any point. Every
column identifier is validated against both the ``[A-Za-z_][A-Za-z0-9_]*`` allowlist and the dataset schema, and every
literal is rendered through the typed value renderer, so the generated string is a pure function of validated typed
data.

Brute-force scoring streams ``(id, vector)`` batches through numpy, keeps a per-batch partial top-k merged into a
running top-k, and computes ``recall@k = |served ids in true top-k| / min(k, candidate_count)``. The denominator is
capped at the candidate count so a perfect retrieval over a filtered set smaller than k still scores 1.0.

Beyond recall@k the job grades each served ranking with nDCG@k and MRR derived from the same distance-ordered
brute-force ground truth (no new labels). Graded relevance is the position in the exact top-k: the item at true rank
``j`` (1-based) is assigned grade ``n - j + 1`` where ``n = min(k, candidate_count)``, so the exact nearest result
carries the largest grade and an item outside the exact top-k carries grade ``0``. nDCG@k is the served ranking's DCG
over those grades divided by the ideal DCG of the exact ranking, and MRR is the reciprocal of the served rank at which
the single exact top result (the first element of the ground-truth order) appears, or ``0`` when it is absent.

The job also scores text and hybrid samples emitted by the Rust sampler. A text sample carries ``recall.query_type =
"text"``, the serialized ``recall.text_query`` node tree, and ``recall.text_columns``. The job recomputes an exact
Okapi BM25 ranking over the named text columns at the pinned dataset version as the ground-truth top-k, then grades the
served ids with the same recall/nDCG/MRR functions. Full-text search returns exact results, so text recall is expected
to be ~1.0 and is primarily a staleness and version-correctness signal rather than an approximation-quality signal: a
served result set that disagrees with the pinned-version exact BM25 ranking points at version drift, a filter or
offset mismatch, or a tokenizer divergence between the index and this reference. A hybrid sample additionally carries
``recall.query_vector`` and ``recall.fusion``: the job computes both the exact vector top-k and the exact BM25 top-k at
the pinned version and fuses them with the recorded fusion strategy (reciprocal-rank fusion or normalized weighted sum)
before grading the served ids. Metrics and the aggregate tables are reported per query type alongside the existing
per-RPC-parameter and per-organization buckets.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import urllib.request
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

import lance
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession

from lance_etl.telemetry import Telemetry, TelemetryConfig

logger: logging.Logger = logging.getLogger(__name__)

FILTER_COLUMN_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PATH_COMPONENT_PATTERN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9._-]+$")
COMPARE_OPS: dict[str, str] = {"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
DISTANCE_TYPES: frozenset[str] = frozenset({"l2", "cosine", "dot", "hamming"})
QUERY_TYPES: frozenset[str] = frozenset({"vector", "text", "hybrid"})
TEXT_OPERATORS: frozenset[str] = frozenset({"or", "and"})
SPANS_SEARCH_PATH: str = "api/v2/spans/events/search"
TOKEN_PATTERN: re.Pattern[str] = re.compile(r"\w+", re.UNICODE)
BM25_K1: float = 1.2
BM25_B: float = 0.75
DEFAULT_RRF_K: float = 60.0


class SampleParseError(ValueError):
    """A recall span failed to parse into a :class:`RecallSample`.

    Attributes:
        reason: A bounded-cardinality reason key used for skip counting.
    """

    def __init__(self, reason: str) -> None:
        """Initialize the error.

        Args:
            reason: The bounded-cardinality reason key.
        """
        super().__init__(reason)
        self.reason: str = reason


class FilterTranslationError(ValueError):
    """A captured filter AST failed strict validation during SQL generation."""


class TextQueryTranslationError(ValueError):
    """A captured text-query AST could not be translated into an exact BM25 reference plan."""


class FusionReplayError(ValueError):
    """A captured hybrid fusion specification could not be replayed."""


@dataclass(frozen=True)
class RecallSample:
    """One sampled vector query parsed from a recall span.

    Attributes:
        sample_id: The sampler-minted UUID identifying the capture.
        captured_at_unix_ms: Capture instant in epoch milliseconds.
        org_id: Organization routing component.
        tenant_id: Tenant routing component.
        namespace: Namespace routing component.
        dataset_version: The committed Lance version that served the query.
        k: The requested result count.
        query_type: The captured query type, one of :data:`QUERY_TYPES`. Legacy captures default to ``vector``.
        query_vector: The query vector values. Empty for pure text queries.
        result_ids: Served result ids in rank order, or None when the capture recorded null.
        result_distances: Served vector result distances in rank order, empty for text queries.
        result_scores: Served text or fused result scores in rank order, empty for vector queries.
        text_query: The captured text-query node tree as a dictionary, or None for vector queries.
        text_columns: The text columns the query searched, empty for vector queries.
        fusion: The captured hybrid fusion specification, or None for non-hybrid queries.
        nprobes_min: Lower nprobes bound, or None for the index default.
        nprobes_max: Upper nprobes bound, or None for the index default.
        refine_factor: Refine factor, or None when unset.
        distance_type: Distance override, or None for the index metric.
        filter_ast: The captured typed filter AST, or None when the query was unfiltered.
    """

    sample_id: str
    captured_at_unix_ms: int
    org_id: str
    tenant_id: str
    namespace: str
    dataset_version: int
    k: int
    query_vector: tuple[float, ...]
    result_ids: tuple[Any, ...] | None
    result_distances: tuple[float, ...]
    query_type: str = "vector"
    result_scores: tuple[float, ...] = ()
    text_query: dict[str, Any] | None = None
    text_columns: tuple[str, ...] = ()
    fusion: dict[str, Any] | None = None
    nprobes_min: int | None = None
    nprobes_max: int | None = None
    refine_factor: int | None = None
    distance_type: str | None = None
    filter_ast: dict[str, Any] | None = None


@dataclass(frozen=True)
class SampleScore:
    """The scoring outcome for one sample.

    Attributes:
        sample_id: The sample's capture UUID.
        org_id: Organization routing component, used for the org-level table rows.
        k: The requested result count.
        query_type: The sample's query type, used for the per-query-type table rows and metric tag.
        nprobes_min: Lower nprobes bound, or None for the index default.
        nprobes_max: Upper nprobes bound, or None for the index default.
        refine_factor: Refine factor, or None when unset.
        recall: The measured recall@k, or None when the sample was skipped.
        ndcg: The measured nDCG@k, or None when the sample was skipped.
        mrr: The measured reciprocal rank of the exact top result, or None when the sample was skipped.
        version_drift: True when the recorded version was unavailable and scoring fell back to latest.
        skip_reason: A bounded-cardinality reason when the sample was skipped, otherwise None.
    """

    sample_id: str
    org_id: str
    k: int
    nprobes_min: int | None
    nprobes_max: int | None
    refine_factor: int | None
    recall: float | None
    version_drift: bool
    skip_reason: str | None
    query_type: str = "vector"
    ndcg: float | None = None
    mrr: float | None = None


@dataclass(frozen=True)
class AggregateRow:
    """One aggregate bucket of the recall report.

    Attributes:
        bucket: Human-readable bucket label for the stdout table.
        samples: Number of successfully scored samples in the bucket.
        mean_recall: Mean recall@k over scored samples, or None when none scored.
        mean_ndcg: Mean nDCG@k over scored samples, or None when none scored.
        mean_mrr: Mean MRR over scored samples, or None when none scored.
        p50: Median recall@k over scored samples, or None when none scored.
        p95: 95th-percentile recall@k over scored samples, or None when none scored.
        drift_count: Samples in the bucket scored against a drifted (latest) version.
        skip_count: Samples in the bucket that were skipped.
        nprobes_min: RPC bucket key carried for metric tagging, None outside RPC buckets.
        nprobes_max: RPC bucket key carried for metric tagging, None outside RPC buckets.
        refine_factor: RPC bucket key carried for metric tagging, None outside RPC buckets.
        query_type: Query-type bucket key carried for metric tagging, None outside query-type buckets.
        is_rpc_bucket: True for RPC-parameter buckets, which are emitted as metrics tagged with the RPC parameters.
        is_query_type_bucket: True for query-type buckets, which are emitted as metrics tagged with the query type.
    """

    bucket: str
    samples: int
    mean_recall: float | None
    mean_ndcg: float | None
    mean_mrr: float | None
    p50: float | None
    p95: float | None
    drift_count: int
    skip_count: int
    nprobes_min: int | None = None
    nprobes_max: int | None = None
    refine_factor: int | None = None
    query_type: str | None = None
    is_rpc_bucket: bool = False
    is_query_type_bucket: bool = False


@dataclass(frozen=True)
class RecallReport:
    """The full output of one recall-audit run.

    Attributes:
        rows: Aggregate rows in table order: overall, then RPC buckets, then org buckets.
        scores: Every per-sample scoring outcome.
        parse_skips: Count of records skipped at parse time, keyed by reason.
    """

    rows: list[AggregateRow]
    scores: list[SampleScore]
    parse_skips: dict[str, int]


@dataclass
class RecallJobConfig:
    """Configuration for :class:`RecallAuditJob`.

    Attributes:
        base_uri: Root location under which per-tenant datasets live. The dataset URI for a sample is
            ``{base_uri}/{org_id}/{tenant_id}/{namespace}.lance``.
        telemetry: Telemetry configuration, the only telemetry object pickled into executor closures.
        storage_options: Object-store options forwarded to pylance.
        id_column: Name of the unique id column matched against the served result ids.
        vector_column: Name of the fixed-size-list vector column scanned for brute-force distances.
        max_samples: Cap on the number of span records fetched from the source.
        batch_size: Scanner batch size for the brute-force scan.
    """

    base_uri: str
    telemetry: TelemetryConfig
    storage_options: dict[str, Any] | None = None
    id_column: str = "vector_id"
    vector_column: str = "vector"
    max_samples: int = 10_000
    batch_size: int = 8192


class SpanSource(Protocol):
    """A source of recall-span attribute dictionaries."""

    def fetch(self, from_ms: int, to_ms: int, max_samples: int) -> Iterator[dict[str, Any]]:
        """Yield flat recall attribute dictionaries captured within a window.

        Args:
            from_ms: Window start in epoch milliseconds, inclusive.
            to_ms: Window end in epoch milliseconds, inclusive.
            max_samples: Cap on the number of records yielded.

        Yields:
            One flat attribute dictionary per sampled span, keys prefixed ``recall.``.
        """
        ...


@dataclass
class InMemorySpanSource:
    """A span source backed by an in-memory record list, for tests.

    Attributes:
        records: The flat attribute dictionaries to serve.
    """

    records: list[dict[str, Any]] = field(default_factory=list)

    def fetch(self, from_ms: int, to_ms: int, max_samples: int) -> Iterator[dict[str, Any]]:
        """Yield records whose capture instant falls within the window.

        Records whose ``recall.captured_at_unix_ms`` does not parse as an integer are yielded anyway so the parser can
        count them as malformed, mirroring how a server-side time filter cannot reject what it cannot read.

        Args:
            from_ms: Window start in epoch milliseconds, inclusive.
            to_ms: Window end in epoch milliseconds, inclusive.
            max_samples: Cap on the number of records yielded.

        Yields:
            The matching flat attribute dictionaries.
        """
        yielded: int = 0
        for record in self.records:
            if yielded >= max_samples:
                return
            raw: Any = record.get("recall.captured_at_unix_ms")
            try:
                captured: int | None = int(raw)
            except (TypeError, ValueError):
                captured = None
            if captured is not None and not (from_ms <= captured <= to_ms):
                continue
            yielded += 1
            yield record


def build_spans_request_body(from_ms: int, to_ms: int, limit: int, cursor: str | None) -> dict[str, Any]:
    """Build one Datadog Spans search API v2 request body.

    Args:
        from_ms: Window start in epoch milliseconds, inclusive.
        to_ms: Window end in epoch milliseconds, inclusive.
        limit: Page size for this request.
        cursor: Pagination cursor from the previous response, or None for the first page.

    Returns:
        The JSON-serializable request body filtering on ``@recall.sample:true``.
    """
    page: dict[str, Any] = {"limit": limit}
    if cursor is not None:
        page["cursor"] = cursor
    return {
        "data": {
            "type": "search_request",
            "attributes": {
                "filter": {"query": "@recall.sample:true", "from": str(from_ms), "to": str(to_ms)},
                "page": page,
                "sort": "timestamp",
            },
        }
    }


def flatten_recall_attributes(span: dict[str, Any]) -> dict[str, Any]:
    """Extract the flat ``recall.*`` attribute dictionary from one Spans API item.

    The Spans API nests custom span attributes under ``attributes.custom``, where the dotted capture keys may appear
    either flat (``recall.sample_id``) or as a nested ``recall`` object. Both shapes are normalized to flat
    ``recall.*`` keys.

    Args:
        span: One item from the Spans search response ``data`` array.

    Returns:
        The flat attribute dictionary, possibly empty when the span carries no recall attributes.
    """
    attributes: dict[str, Any] = span.get("attributes") or {}
    custom: dict[str, Any] = attributes.get("custom") or {}
    flat: dict[str, Any] = {}
    for container in (attributes, custom):
        for key, value in container.items():
            if isinstance(key, str) and key.startswith("recall."):
                flat[key] = value
    nested: Any = custom.get("recall", attributes.get("recall"))
    if isinstance(nested, dict):
        for key, value in nested.items():
            flat[f"recall.{key}"] = value
    return flat


@dataclass
class DatadogSpanSource:
    """A span source backed by the Datadog Spans search API v2.

    Credentials are read from the ``DD_API_KEY`` and ``DD_APP_KEY`` environment variables at fetch time. Requests use
    stdlib ``urllib.request`` so no additional HTTP dependency is required.

    Attributes:
        site: Datadog site domain, for example ``datadoghq.com`` or ``datadoghq.eu``.
        page_limit: Page size cap per search request.
        timeout_seconds: Per-request socket timeout.
    """

    site: str = "datadoghq.com"
    page_limit: int = 1000
    timeout_seconds: float = 30.0

    def search_url(self) -> str:
        """Return the Spans search endpoint URL for the configured site.

        Returns:
            The full HTTPS endpoint URL.
        """
        return f"https://api.{self.site}/{SPANS_SEARCH_PATH}"

    def fetch(self, from_ms: int, to_ms: int, max_samples: int) -> Iterator[dict[str, Any]]:
        """Yield flat recall attribute dictionaries from Datadog, following pagination cursors.

        Args:
            from_ms: Window start in epoch milliseconds, inclusive.
            to_ms: Window end in epoch milliseconds, inclusive.
            max_samples: Cap on the number of records yielded.

        Yields:
            One flat attribute dictionary per sampled span.

        Raises:
            ValueError: If ``DD_API_KEY`` or ``DD_APP_KEY`` is missing from the environment.
        """
        api_key: str = os.environ.get("DD_API_KEY", "")
        app_key: str = os.environ.get("DD_APP_KEY", "")
        if not api_key or not app_key:
            raise ValueError("DatadogSpanSource requires DD_API_KEY and DD_APP_KEY in the environment")
        headers: dict[str, str] = {
            "DD-API-KEY": api_key,
            "DD-APPLICATION-KEY": app_key,
            "Content-Type": "application/json",
        }
        cursor: str | None = None
        yielded: int = 0
        while yielded < max_samples:
            limit: int = min(self.page_limit, max_samples - yielded)
            body: bytes = json.dumps(build_spans_request_body(from_ms, to_ms, limit, cursor)).encode("utf-8")
            request: urllib.request.Request = urllib.request.Request(
                self.search_url(), data=body, headers=headers, method="POST"
            )
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                payload: dict[str, Any] = json.loads(response.read().decode("utf-8"))
            spans: list[dict[str, Any]] = payload.get("data") or []
            for span in spans:
                yield flatten_recall_attributes(span)
                yielded += 1
                if yielded >= max_samples:
                    return
            cursor = ((payload.get("meta") or {}).get("page") or {}).get("after")
            if not spans or not cursor:
                return


def attr_string(attrs: dict[str, Any], key: str) -> str:
    """Read a required non-empty string attribute.

    Args:
        attrs: The flat attribute dictionary.
        key: The attribute key.

    Returns:
        The string value.

    Raises:
        SampleParseError: If the attribute is missing, empty, or not a string.
    """
    value: Any = attrs.get(key)
    if not isinstance(value, str) or not value:
        raise SampleParseError(f"missing:{key}")
    return value


def attr_int(attrs: dict[str, Any], key: str) -> int:
    """Read a required integer attribute, accepting string-encoded integers.

    Args:
        attrs: The flat attribute dictionary.
        key: The attribute key.

    Returns:
        The integer value.

    Raises:
        SampleParseError: If the attribute is missing or does not parse as an integer.
    """
    value: Any = attrs.get(key)
    if value is None:
        raise SampleParseError(f"missing:{key}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise SampleParseError(f"invalid:{key}") from exc


def attr_optional_int(attrs: dict[str, Any], key: str) -> int | None:
    """Read an optional integer attribute, accepting string-encoded integers.

    Args:
        attrs: The flat attribute dictionary.
        key: The attribute key.

    Returns:
        The integer value, or None when the attribute is absent.

    Raises:
        SampleParseError: If the attribute is present but does not parse as an integer.
    """
    if attrs.get(key) is None:
        return None
    return attr_int(attrs, key)


def attr_path_component(attrs: dict[str, Any], key: str) -> str:
    """Read a required routing-path component, validated against the path allowlist.

    Args:
        attrs: The flat attribute dictionary.
        key: The attribute key.

    Returns:
        The validated component.

    Raises:
        SampleParseError: If the attribute is missing or fails path-component validation.
    """
    value: str = attr_string(attrs, key)
    if not PATH_COMPONENT_PATTERN.match(value):
        raise SampleParseError(f"invalid:{key}")
    return value


def parse_query_vector(attrs: dict[str, Any]) -> tuple[float, ...]:
    """Parse the JSON-encoded query vector attribute.

    Args:
        attrs: The flat attribute dictionary.

    Returns:
        The query vector values.

    Raises:
        SampleParseError: If the attribute is missing, is not a JSON number array, or is empty.
    """
    raw: str = attr_string(attrs, "recall.query_vector")
    parsed: Any = decode_json_attr(raw, "recall.query_vector")
    if not isinstance(parsed, list) or not parsed:
        raise SampleParseError("invalid:recall.query_vector")
    values: list[float] = []
    for item in parsed:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise SampleParseError("invalid:recall.query_vector")
        values.append(float(item))
    return tuple(values)


def decode_json_attr(raw: Any, key: str) -> Any:
    """Decode an attribute value that is either a JSON string or an already-parsed object.

    Args:
        raw: The raw attribute value.
        key: The attribute key, used to build the error reason.

    Returns:
        The decoded value.

    Raises:
        SampleParseError: If a string value does not parse as JSON.
    """
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except json.JSONDecodeError as exc:
        raise SampleParseError(f"invalid:{key}") from exc


def parse_float_tuple_attr(attrs: dict[str, Any], key: str) -> tuple[float, ...]:
    """Parse an optional JSON number-array attribute into a float tuple.

    Args:
        attrs: The flat attribute dictionary.
        key: The attribute key.

    Returns:
        The values in rank order, empty when the attribute is absent.

    Raises:
        SampleParseError: If the attribute is present but is not a JSON number array.
    """
    raw: Any = attrs.get(key)
    if raw is None:
        return ()
    parsed: Any = decode_json_attr(raw, key)
    if not isinstance(parsed, list):
        raise SampleParseError(f"invalid:{key}")
    try:
        return tuple(float(item) for item in parsed)
    except (TypeError, ValueError) as exc:
        raise SampleParseError(f"invalid:{key}") from exc


def parse_result_ids(attrs: dict[str, Any]) -> tuple[Any, ...] | None:
    """Parse the JSON-encoded served result ids, which may be null.

    Args:
        attrs: The flat attribute dictionary.

    Returns:
        The served ids in rank order, or None when the capture recorded null or omitted the attribute.

    Raises:
        SampleParseError: If the attribute is present but is neither a JSON array nor null.
    """
    raw: Any = attrs.get("recall.result_ids")
    if raw is None:
        return None
    parsed: Any = decode_json_attr(raw, "recall.result_ids")
    if parsed is None:
        return None
    if not isinstance(parsed, list):
        raise SampleParseError("invalid:recall.result_ids")
    return tuple(parsed)


def parse_filter_ast(attrs: dict[str, Any]) -> dict[str, Any] | None:
    """Parse the JSON-encoded filter AST attribute.

    Structural validation of the AST itself happens at translation time on the executor, where the dataset schema is
    available for column-membership checks.

    Args:
        attrs: The flat attribute dictionary.

    Returns:
        The AST as a dictionary, or None when the query was unfiltered.

    Raises:
        SampleParseError: If the attribute is present but is not a JSON object.
    """
    raw: Any = attrs.get("recall.filter")
    if raw is None:
        return None
    parsed: Any = decode_json_attr(raw, "recall.filter")
    if not isinstance(parsed, dict):
        raise SampleParseError("invalid:recall.filter")
    return parsed


def parse_text_query(attrs: dict[str, Any]) -> dict[str, Any]:
    """Parse the JSON-encoded text-query node tree attribute.

    Structural validation of the node tree happens at scoring time on the executor, where the dataset schema is
    available for column-membership checks.

    Args:
        attrs: The flat attribute dictionary.

    Returns:
        The text-query node tree as a dictionary.

    Raises:
        SampleParseError: If the attribute is missing or is not a JSON object.
    """
    raw: Any = attrs.get("recall.text_query")
    if raw is None:
        raise SampleParseError("missing:recall.text_query")
    parsed: Any = decode_json_attr(raw, "recall.text_query")
    if not isinstance(parsed, dict):
        raise SampleParseError("invalid:recall.text_query")
    return parsed


def parse_text_columns(attrs: dict[str, Any]) -> tuple[str, ...]:
    """Parse the JSON-encoded text-columns array attribute.

    Args:
        attrs: The flat attribute dictionary.

    Returns:
        The text columns the query searched.

    Raises:
        SampleParseError: If the attribute is missing, is not a non-empty JSON array, or holds a non-string entry.
    """
    raw: Any = attrs.get("recall.text_columns")
    if raw is None:
        raise SampleParseError("missing:recall.text_columns")
    parsed: Any = decode_json_attr(raw, "recall.text_columns")
    if not isinstance(parsed, list) or not parsed:
        raise SampleParseError("invalid:recall.text_columns")
    for item in parsed:
        if not isinstance(item, str) or not item:
            raise SampleParseError("invalid:recall.text_columns")
    return tuple(parsed)


def parse_fusion(attrs: dict[str, Any]) -> dict[str, Any]:
    """Parse the JSON-encoded hybrid fusion specification attribute.

    Args:
        attrs: The flat attribute dictionary.

    Returns:
        The fusion specification as a dictionary.

    Raises:
        SampleParseError: If the attribute is missing or is not a JSON object.
    """
    raw: Any = attrs.get("recall.fusion")
    if raw is None:
        raise SampleParseError("missing:recall.fusion")
    parsed: Any = decode_json_attr(raw, "recall.fusion")
    if not isinstance(parsed, dict):
        raise SampleParseError("invalid:recall.fusion")
    return parsed


def parse_recall_sample(attrs: dict[str, Any]) -> RecallSample:
    """Parse one flat recall attribute dictionary into a :class:`RecallSample`.

    Args:
        attrs: The flat attribute dictionary with ``recall.*`` keys.

    Returns:
        The parsed sample.

    Raises:
        SampleParseError: If any attribute is missing or malformed, carrying a bounded reason key.
    """
    distance_type: Any = attrs.get("recall.distance_type")
    if distance_type is not None and distance_type not in DISTANCE_TYPES:
        raise SampleParseError("invalid:recall.distance_type")
    query_type: Any = attrs.get("recall.query_type")
    if query_type is None:
        query_type = "vector"
    if query_type not in QUERY_TYPES:
        raise SampleParseError("invalid:recall.query_type")
    k: int = attr_int(attrs, "recall.k")
    if k < 1:
        raise SampleParseError("invalid:recall.k")
    query_vector: tuple[float, ...] = ()
    if query_type in ("vector", "hybrid"):
        query_vector = parse_query_vector(attrs)
    text_query: dict[str, Any] | None = None
    text_columns: tuple[str, ...] = ()
    if query_type in ("text", "hybrid"):
        text_query = parse_text_query(attrs)
        text_columns = parse_text_columns(attrs)
    fusion: dict[str, Any] | None = None
    if query_type == "hybrid":
        fusion = parse_fusion(attrs)
    return RecallSample(
        sample_id=attr_string(attrs, "recall.sample_id"),
        captured_at_unix_ms=attr_int(attrs, "recall.captured_at_unix_ms"),
        org_id=attr_path_component(attrs, "recall.org_id"),
        tenant_id=attr_path_component(attrs, "recall.tenant_id"),
        namespace=attr_path_component(attrs, "recall.namespace"),
        dataset_version=attr_int(attrs, "recall.dataset_version"),
        k=k,
        query_type=query_type,
        query_vector=query_vector,
        result_ids=parse_result_ids(attrs),
        result_distances=parse_float_tuple_attr(attrs, "recall.result_distances"),
        result_scores=parse_float_tuple_attr(attrs, "recall.result_scores"),
        text_query=text_query,
        text_columns=text_columns,
        fusion=fusion,
        nprobes_min=attr_optional_int(attrs, "recall.nprobes_min"),
        nprobes_max=attr_optional_int(attrs, "recall.nprobes_max"),
        refine_factor=attr_optional_int(attrs, "recall.refine_factor"),
        distance_type=distance_type,
        filter_ast=parse_filter_ast(attrs),
    )


def parse_samples(records: Iterable[dict[str, Any]]) -> tuple[list[RecallSample], dict[str, int]]:
    """Parse raw attribute records, counting malformed ones by reason.

    Args:
        records: The flat attribute dictionaries from a span source.

    Returns:
        The parsed samples and a reason-keyed count of skipped records.
    """
    samples: list[RecallSample] = []
    skips: Counter[str] = Counter()
    for record in records:
        try:
            samples.append(parse_recall_sample(record))
        except SampleParseError as exc:
            skips[exc.reason] += 1
    return samples, dict(skips)


def render_filter_column(name: Any, columns: frozenset[str]) -> str:
    """Validate and render one filter column identifier.

    Args:
        name: The column name from the AST.
        columns: The dataset schema's column names.

    Returns:
        The validated identifier, unchanged.

    Raises:
        FilterTranslationError: If the name fails the identifier allowlist or is not in the schema.
    """
    if not isinstance(name, str) or not FILTER_COLUMN_PATTERN.match(name):
        raise FilterTranslationError(f"filter column fails identifier allowlist: {name!r}")
    if name not in columns:
        raise FilterTranslationError(f"filter column not in dataset schema: {name!r}")
    return name


def render_filter_literal(value: Any) -> str:
    """Render one typed filter literal.

    The literal must be an externally tagged single-key object, one of ``{"int": i}``, ``{"float": f}``,
    ``{"string": s}``, or ``{"bool": b}``. Strings are single-quoted with embedded quotes doubled, and floats must be
    finite.

    Args:
        value: The typed literal object from the AST.

    Returns:
        The rendered literal.

    Raises:
        FilterTranslationError: If the object shape, tag, or payload type is invalid.
    """
    if not isinstance(value, dict) or len(value) != 1:
        raise FilterTranslationError(f"filter literal must be a single-key typed object: {value!r}")
    tag, raw = next(iter(value.items()))
    if tag == "int":
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise FilterTranslationError(f"int literal payload must be an integer: {raw!r}")
        return str(raw)
    if tag == "float":
        if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(float(raw)):
            raise FilterTranslationError(f"float literal payload must be a finite number: {raw!r}")
        return repr(float(raw))
    if tag == "string":
        if not isinstance(raw, str):
            raise FilterTranslationError(f"string literal payload must be a string: {raw!r}")
        escaped: str = raw.replace("'", "''")
        return f"'{escaped}'"
    if tag == "bool":
        if not isinstance(raw, bool):
            raise FilterTranslationError(f"bool literal payload must be a boolean: {raw!r}")
        return "TRUE" if raw else "FALSE"
    raise FilterTranslationError(f"unknown filter literal tag: {tag!r}")


def filter_ast_to_sql(node: Any, columns: frozenset[str]) -> str:
    """Translate a captured typed filter AST into a Lance scanner filter string.

    This is internal generation from validated typed data only, which is why it honors the repository's no-raw-SQL
    rule: clients never supply strings, every identifier passes the allowlist plus a schema-membership check, and
    every literal is rendered through :func:`render_filter_literal`.

    Args:
        node: The externally tagged AST node.
        columns: The dataset schema's column names.

    Returns:
        The generated filter string.

    Raises:
        FilterTranslationError: If the node shape, tag, operator, column, or any literal is invalid.
    """
    if not isinstance(node, dict) or len(node) != 1:
        raise FilterTranslationError(f"filter node must be a single-key tagged object: {node!r}")
    tag, body = next(iter(node.items()))
    if tag == "compare":
        op: Any = body.get("op")
        if op not in COMPARE_OPS:
            raise FilterTranslationError(f"unknown compare op: {op!r}")
        column: str = render_filter_column(body.get("column"), columns)
        return f"({column} {COMPARE_OPS[op]} {render_filter_literal(body.get('value'))})"
    if tag == "in_list":
        column = render_filter_column(body.get("column"), columns)
        values: Any = body.get("values")
        if not isinstance(values, list) or not values:
            raise FilterTranslationError("in_list requires a non-empty values list")
        rendered: str = ", ".join(render_filter_literal(value) for value in values)
        keyword: str = "NOT IN" if body.get("negated", False) else "IN"
        return f"({column} {keyword} ({rendered}))"
    if tag == "is_null":
        return f"({render_filter_column(body.get('column'), columns)} IS NULL)"
    if tag == "is_not_null":
        return f"({render_filter_column(body.get('column'), columns)} IS NOT NULL)"
    if tag == "between":
        column = render_filter_column(body.get("column"), columns)
        low: str = render_filter_literal(body.get("low"))
        high: str = render_filter_literal(body.get("high"))
        return f"({column} BETWEEN {low} AND {high})"
    if tag in ("and", "or"):
        if not isinstance(body, list) or not body:
            raise FilterTranslationError(f"{tag} requires a non-empty child list")
        joiner: str = " AND " if tag == "and" else " OR "
        return f"({joiner.join(filter_ast_to_sql(child, columns) for child in body)})"
    if tag == "not":
        return f"(NOT {filter_ast_to_sql(body, columns)})"
    raise FilterTranslationError(f"unknown filter node tag: {tag!r}")


def sample_dataset_uri(base_uri: str, sample: RecallSample) -> str:
    """Build the dataset URI for one sample's routing components.

    The components were validated against the path allowlist at parse time, so the URI is confined to the
    routing-key prefix.

    Args:
        base_uri: Root location under which per-tenant datasets live.
        sample: The parsed sample.

    Returns:
        The ``{base_uri}/{org_id}/{tenant_id}/{namespace}.lance`` URI.
    """
    base: str = base_uri.rstrip("/")
    return f"{base}/{sample.org_id}/{sample.tenant_id}/{sample.namespace}.lance"


def resolve_dataset(
    uri: str, version: int, storage_options: dict[str, Any] | None
) -> tuple[lance.LanceDataset | None, bool]:
    """Open a dataset checked out at the recorded version, falling back to latest on drift.

    Args:
        uri: The dataset URI.
        version: The committed version that served the sampled queries.
        storage_options: Object-store options forwarded to pylance.

    Returns:
        ``(dataset, version_drift)`` where the dataset is None when the URI cannot be opened at all, and
        ``version_drift`` is True when the recorded version was unavailable and the latest version was opened instead.
    """
    try:
        return lance.dataset(uri, version=version, storage_options=storage_options), False
    except (ValueError, OSError, RuntimeError):
        try:
            return lance.dataset(uri, storage_options=storage_options), True
        except (ValueError, OSError, RuntimeError):
            return None, False


def index_default_distance_type(dataset: lance.LanceDataset, vector_column: str) -> str:
    """Read the default distance metric from the dataset's vector index metadata.

    Samples that omit ``recall.distance_type`` were served with the index metric, so the brute-force replay must use
    the same metric. When no vector index covers the column or the statistics omit the metric, ``l2`` is returned as
    the Lance default.

    Args:
        dataset: The opened dataset.
        vector_column: The vector column the queries searched.

    Returns:
        The lowercase distance type, one of :data:`DISTANCE_TYPES` or ``l2``.
    """
    try:
        descriptions: list[Any] = dataset.describe_indices()
    except (ValueError, OSError, RuntimeError):
        return "l2"
    for description in descriptions:
        if vector_column not in getattr(description, "field_names", []):
            continue
        try:
            stats: dict[str, Any] = dataset.stats.index_stats(description.name)
        except (ValueError, OSError, RuntimeError):
            continue
        entries: list[Any] = stats.get("indices") or []
        candidates: list[Any] = [stats, *entries]
        for entry in candidates:
            if isinstance(entry, dict):
                metric: Any = entry.get("metric_type")
                if isinstance(metric, str) and metric.lower() in DISTANCE_TYPES:
                    return metric.lower()
    return "l2"


def compute_distances(candidates: np.ndarray, query: np.ndarray, distance_type: str) -> np.ndarray:
    """Compute exact distances between candidate vectors and a query.

    ``l2`` returns the squared Euclidean distance, which preserves the L2 ranking exactly and is what recall needs.
    ``cosine`` returns ``1 - cosine_similarity`` with zero-norm rows pinned to distance 1.0. ``dot`` returns the
    negated dot product matching Lance's dot distance ordering. ``hamming`` counts differing components.

    Args:
        candidates: A ``(rows, dim)`` float64 matrix of candidate vectors.
        query: A ``(dim,)`` float64 query vector.
        distance_type: One of :data:`DISTANCE_TYPES`.

    Returns:
        A ``(rows,)`` distance vector where smaller is closer.

    Raises:
        ValueError: If the distance type is unknown.
    """
    if distance_type == "l2":
        deltas: np.ndarray = candidates - query
        return np.einsum("ij,ij->i", deltas, deltas)
    if distance_type == "cosine":
        norms: np.ndarray = np.linalg.norm(candidates, axis=1) * float(np.linalg.norm(query))
        dots: np.ndarray = candidates @ query
        safe: np.ndarray = np.where(norms > 0.0, norms, 1.0)
        return np.where(norms > 0.0, 1.0 - dots / safe, 1.0)
    if distance_type == "dot":
        return -(candidates @ query)
    if distance_type == "hamming":
        return np.count_nonzero(candidates != query, axis=1).astype(np.float64)
    raise ValueError(f"unknown distance type: {distance_type!r}")


def fixed_size_list_to_numpy(column: pa.Array) -> np.ndarray:
    """Convert a fixed-size-list Arrow array into a 2-D float64 numpy matrix.

    Args:
        column: The fixed-size-list array, with nulls already filtered out.

    Returns:
        A ``(rows, dim)`` float64 matrix.
    """
    flat: pa.Array = column.flatten()
    values: np.ndarray = np.asarray(flat.to_numpy(zero_copy_only=False), dtype=np.float64)
    return values.reshape(len(column), column.type.list_size)


def brute_force_top_k_scored(
    dataset: lance.LanceDataset,
    query: np.ndarray,
    k: int,
    distance_type: str,
    id_column: str,
    vector_column: str,
    filter_sql: str | None,
    batch_size: int,
) -> tuple[list[Any], list[float], int]:
    """Compute the exact top-k ids and their distances for a query by scanning the dataset.

    Streams ``(id, vector)`` batches, computes exact distances per batch in numpy, and merges each batch's partial
    top-k into a running top-k so memory stays bounded by ``batch_size + k``.

    Args:
        dataset: The opened (possibly version-pinned) dataset.
        query: The float64 query vector.
        k: The requested result count.
        distance_type: One of :data:`DISTANCE_TYPES`.
        id_column: Name of the unique id column.
        vector_column: Name of the fixed-size-list vector column.
        filter_sql: The internally generated filter string, or None for an unfiltered scan.
        batch_size: Scanner batch size.

    Returns:
        ``(true_top_k_ids, true_top_k_distances, candidate_count)`` where the ids are in ascending-distance order, the
        distances are aligned with them, and the count is the number of rows that passed the filter and carried a
        non-null vector.

    Raises:
        ValueError: If a batch's vector dimension does not match the query dimension.
    """
    scanner: lance.LanceScanner = dataset.scanner(
        columns=[id_column, vector_column], filter=filter_sql, batch_size=batch_size
    )
    best_ids: list[Any] = []
    best_dists: np.ndarray = np.empty(0, dtype=np.float64)
    candidate_count: int = 0
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        table: pa.Table = pa.Table.from_batches([batch])
        vectors: pa.ChunkedArray = table.column(vector_column)
        if vectors.null_count:
            table = table.filter(pc.is_valid(vectors))
            vectors = table.column(vector_column)
        if table.num_rows == 0:
            continue
        candidates: np.ndarray = fixed_size_list_to_numpy(vectors.combine_chunks())
        if candidates.shape[1] != query.shape[0]:
            raise ValueError(
                f"query dimension {query.shape[0]} does not match dataset vector dimension {candidates.shape[1]}"
            )
        distances: np.ndarray = compute_distances(candidates, query, distance_type)
        candidate_count += table.num_rows
        merged_dists: np.ndarray = np.concatenate([best_dists, distances])
        merged_ids: list[Any] = best_ids + table.column(id_column).to_pylist()
        order: np.ndarray = np.argsort(merged_dists, kind="stable")[:k]
        best_dists = merged_dists[order]
        best_ids = [merged_ids[index] for index in order]
    return best_ids, best_dists.tolist(), candidate_count


def brute_force_top_k(
    dataset: lance.LanceDataset,
    query: np.ndarray,
    k: int,
    distance_type: str,
    id_column: str,
    vector_column: str,
    filter_sql: str | None,
    batch_size: int,
) -> tuple[list[Any], int]:
    """Compute the exact top-k ids for a query by scanning the dataset.

    Args:
        dataset: The opened (possibly version-pinned) dataset.
        query: The float64 query vector.
        k: The requested result count.
        distance_type: One of :data:`DISTANCE_TYPES`.
        id_column: Name of the unique id column.
        vector_column: Name of the fixed-size-list vector column.
        filter_sql: The internally generated filter string, or None for an unfiltered scan.
        batch_size: Scanner batch size.

    Returns:
        ``(true_top_k_ids, candidate_count)`` where the ids are in ascending-distance order and the count is the
        number of rows that passed the filter and carried a non-null vector.

    Raises:
        ValueError: If a batch's vector dimension does not match the query dimension.
    """
    ids, scores, count = brute_force_top_k_scored(
        dataset, query, k, distance_type, id_column, vector_column, filter_sql, batch_size
    )
    del scores
    return ids, count


def ranking_quality(
    true_ids_ordered: list[Any], served_ids: list[Any], k: int, candidate_count: int
) -> tuple[float, float, float]:
    """Grade a served ranking against the exact ground-truth order with recall@k, nDCG@k, and MRR.

    Graded relevance is the position in the exact top-k: the item at true rank ``j`` (1-based) is assigned grade
    ``n - j + 1`` where ``n = len(true_ids_ordered)``, so the exact top result carries the largest grade and an item
    outside the exact top-k carries grade ``0``. nDCG@k is the served ranking's discounted cumulative gain over those
    grades divided by the ideal discounted cumulative gain of the exact order, with the standard ``1 / log2(rank + 1)``
    position discount. MRR is the reciprocal of the served rank at which the single exact top result (the first element
    of the ground-truth order) appears, or ``0`` when it is absent from the served top-k.

    Args:
        true_ids_ordered: The exact ground-truth ids in best-first order, length ``min(k, candidate_count)``.
        served_ids: The served result ids in rank order.
        k: The requested result count.
        candidate_count: The number of eligible candidates the ground truth was drawn from.

    Returns:
        ``(recall, ndcg, mrr)`` over the served top-k.
    """
    denominator: int = min(k, candidate_count)
    served_top: list[Any] = list(served_ids)[:k]
    n: int = len(true_ids_ordered)
    grade_map: dict[Any, int] = {tid: n - idx for idx, tid in enumerate(true_ids_ordered)}
    hits: int = len(set(grade_map) & set(served_top))
    recall: float = hits / denominator if denominator > 0 else 0.0
    dcg: float = sum(grade_map.get(sid, 0) / math.log2(pos + 2) for pos, sid in enumerate(served_top))
    idcg: float = sum((n - j) / math.log2(j + 2) for j in range(n))
    ndcg: float = dcg / idcg if idcg > 0 else 0.0
    mrr: float = 0.0
    if true_ids_ordered:
        top_true: Any = true_ids_ordered[0]
        if top_true in served_top:
            mrr = 1.0 / (served_top.index(top_true) + 1)
    return recall, ndcg, mrr


def tokenize_text(text: Any) -> list[str]:
    """Tokenize one text value into lowercase word tokens.

    The reference tokenizer is a Unicode word splitter over the lowercased string. It approximates the default Lance
    full-text tokenizer. A divergence between this tokenizer and the index tokenizer shows up as a sub-1.0 text recall,
    which the report attributes to the staleness and correctness signal documented at the module level.

    Args:
        text: The cell value, which may be None or non-string.

    Returns:
        The token list, empty when the value is None or not a string.
    """
    if not isinstance(text, str):
        return []
    return TOKEN_PATTERN.findall(text.lower())


def bm25_column_scores(
    token_lists: list[list[str]], query_terms: list[str], operator: str
) -> tuple[np.ndarray, np.ndarray]:
    """Compute exact Okapi BM25 scores and a match mask for one column.

    Uses the Lucene BM25 parameterization with ``k1`` of :data:`BM25_K1`, ``b`` of :data:`BM25_B`, and the
    always-positive inverse document frequency ``ln(1 + (N - df + 0.5) / (df + 0.5))``. A document matches under the
    ``or`` operator when it contains at least one query term and under the ``and`` operator when it contains all of
    them.

    Args:
        token_lists: One token list per document, aligned with the candidate order.
        query_terms: The tokenized query terms.
        operator: ``or`` or ``and``.

    Returns:
        ``(scores, matched)`` where ``scores`` holds the BM25 score per document and ``matched`` is the boolean match
        mask per document.
    """
    count: int = len(token_lists)
    scores: np.ndarray = np.zeros(count, dtype=np.float64)
    if count == 0 or not query_terms:
        return scores, np.zeros(count, dtype=bool)
    lengths: np.ndarray = np.asarray([len(tokens) for tokens in token_lists], dtype=np.float64)
    avgdl: float = float(lengths.mean()) if lengths.sum() > 0 else 1.0
    if avgdl == 0.0:
        avgdl = 1.0
    counters: list[Counter[str]] = [Counter(tokens) for tokens in token_lists]
    unique_terms: list[str] = sorted(set(query_terms))
    present: dict[str, np.ndarray] = {}
    for term in unique_terms:
        tf: np.ndarray = np.asarray([counter.get(term, 0) for counter in counters], dtype=np.float64)
        present[term] = tf
        df: int = int(np.count_nonzero(tf))
        idf: float = math.log(1.0 + (count - df + 0.5) / (df + 0.5))
        denom: np.ndarray = tf + BM25_K1 * (1.0 - BM25_B + BM25_B * lengths / avgdl)
        scores += np.where(tf > 0, idf * (tf * (BM25_K1 + 1.0)) / denom, 0.0)
    term_present: np.ndarray = np.vstack([present[term] > 0 for term in unique_terms])
    matched: np.ndarray = term_present.all(axis=0) if operator == "and" else term_present.any(axis=0)
    return scores, matched


def text_query_field_queries(
    node: Any, text_columns: tuple[str, ...], columns: frozenset[str]
) -> list[tuple[str, list[str], str, float]]:
    """Extract per-column scoring clauses from a captured text-query node tree.

    Supports the ``match`` and ``multi_match`` node shapes, which cover the sampled full-text and hybrid traffic. Every
    referenced column is validated against the identifier allowlist and the dataset schema, honoring the same strict
    no-raw-string replay rule as the filter AST. Other node shapes are rejected so the sample is skipped rather than
    scored against an unsupported reference.

    Args:
        node: The externally tagged text-query node.
        text_columns: The query's default columns, used by clauses that do not name a column.
        columns: The dataset schema's column names.

    Returns:
        A list of ``(column, terms, operator, boost)`` clauses.

    Raises:
        TextQueryTranslationError: If the node shape is unsupported or any column fails validation.
    """
    if not isinstance(node, dict) or len(node) != 1:
        raise TextQueryTranslationError(f"text query node must be a single-key tagged object: {node!r}")
    tag, body = next(iter(node.items()))
    if not isinstance(body, dict):
        raise TextQueryTranslationError(f"text query body must be an object: {body!r}")
    if tag == "match":
        terms: list[str] = tokenize_text(body.get("terms"))
        operator: str = body.get("operator", "or")
        if operator not in TEXT_OPERATORS:
            raise TextQueryTranslationError(f"unknown text operator: {operator!r}")
        boost: float = float(body.get("boost", 1.0))
        column: Any = body.get("column")
        targets: tuple[str, ...] = (column,) if column is not None else text_columns
        if not targets:
            raise TextQueryTranslationError("match clause has no column and no default text columns")
        return [(validate_text_column(target, columns), terms, operator, boost) for target in targets]
    if tag == "multi_match":
        terms = tokenize_text(body.get("terms"))
        operator = body.get("operator", "or")
        if operator not in TEXT_OPERATORS:
            raise TextQueryTranslationError(f"unknown text operator: {operator!r}")
        target_columns: Any = body.get("columns")
        if not isinstance(target_columns, list) or not target_columns:
            raise TextQueryTranslationError("multi_match requires a non-empty columns list")
        boosts: Any = body.get("boosts") or [1.0] * len(target_columns)
        if not isinstance(boosts, list) or len(boosts) != len(target_columns):
            raise TextQueryTranslationError("multi_match boosts must match the columns length")
        return [
            (validate_text_column(target, columns), terms, operator, float(weight))
            for target, weight in zip(target_columns, boosts, strict=True)
        ]
    raise TextQueryTranslationError(f"unsupported text query node tag: {tag!r}")


def validate_text_column(name: Any, columns: frozenset[str]) -> str:
    """Validate one text column identifier against the allowlist and the dataset schema.

    Args:
        name: The column name from the text-query AST.
        columns: The dataset schema's column names.

    Returns:
        The validated identifier, unchanged.

    Raises:
        TextQueryTranslationError: If the name fails the identifier allowlist or is not in the schema.
    """
    if not isinstance(name, str) or not FILTER_COLUMN_PATTERN.match(name):
        raise TextQueryTranslationError(f"text column fails identifier allowlist: {name!r}")
    if name not in columns:
        raise TextQueryTranslationError(f"text column not in dataset schema: {name!r}")
    return name


def bm25_top_k(
    dataset: lance.LanceDataset,
    field_queries: list[tuple[str, list[str], str, float]],
    k: int,
    id_column: str,
    filter_sql: str | None,
    batch_size: int,
) -> tuple[list[Any], list[float], int]:
    """Compute the exact BM25 top-k ids and scores over the named text columns.

    Materializes the candidate text columns at the pinned version, computes per-column BM25 with
    :func:`bm25_column_scores`, sums the boosted column scores, and ranks the documents that matched at least one
    clause. Ties break on the id column so the reference order is deterministic.

    Args:
        dataset: The opened (possibly version-pinned) dataset.
        field_queries: The ``(column, terms, operator, boost)`` clauses to score.
        k: The requested result count.
        id_column: Name of the unique id column.
        filter_sql: The internally generated filter string, or None for an unfiltered scan.
        batch_size: Scanner batch size.

    Returns:
        ``(true_top_k_ids, true_top_k_scores, candidate_count)`` where the ids are in descending-score order, the
        scores are aligned with them, and the count is the number of documents that matched at least one clause.
    """
    needed_columns: list[str] = sorted({clause[0] for clause in field_queries})
    scanner: lance.LanceScanner = dataset.scanner(
        columns=[id_column, *needed_columns], filter=filter_sql, batch_size=batch_size
    )
    ids: list[Any] = []
    column_tokens: dict[str, list[list[str]]] = {column: [] for column in needed_columns}
    for batch in scanner.to_batches():
        if batch.num_rows == 0:
            continue
        table: pa.Table = pa.Table.from_batches([batch])
        ids.extend(table.column(id_column).to_pylist())
        for column in needed_columns:
            column_tokens[column].extend(tokenize_text(value) for value in table.column(column).to_pylist())
    total: int = len(ids)
    if total == 0:
        return [], [], 0
    scores: np.ndarray = np.zeros(total, dtype=np.float64)
    matched_any: np.ndarray = np.zeros(total, dtype=bool)
    for column, terms, operator, boost in field_queries:
        column_scores, matched = bm25_column_scores(column_tokens[column], terms, operator)
        scores += boost * np.where(matched, column_scores, 0.0)
        matched_any |= matched
    matched_indices: list[int] = [index for index in range(total) if matched_any[index]]
    matched_indices.sort(key=lambda index: (-scores[index], ids[index]))
    top: list[int] = matched_indices[:k]
    return [ids[index] for index in top], [float(scores[index]) for index in top], len(matched_indices)


def normalize_leg(ids: list[Any], scores: list[float], lower_is_better: bool) -> dict[Any, float]:
    """Min-max normalize one fusion leg's scores into ``[0, 1]`` with best mapped to 1.0.

    Args:
        ids: The leg's ids in rank order.
        scores: The leg's raw scores aligned with the ids.
        lower_is_better: True for distance legs where a smaller score is better, False for BM25 legs.

    Returns:
        A mapping from id to normalized score, where the best id maps to 1.0 and ties or single-element legs map all
        ids to 1.0.
    """
    if not ids:
        return {}
    values: np.ndarray = np.asarray(scores, dtype=np.float64)
    if lower_is_better:
        values = -values
    low: float = float(values.min())
    high: float = float(values.max())
    if high == low:
        return {rid: 1.0 for rid in ids}
    normalized: np.ndarray = (values - low) / (high - low)
    return {rid: float(normalized[index]) for index, rid in enumerate(ids)}


def fuse_legs(
    fusion_ast: dict[str, Any],
    vector_ids: list[Any],
    vector_scores: list[float],
    text_ids: list[Any],
    text_scores: list[float],
    k: int,
) -> list[Any]:
    """Replay a captured hybrid fusion specification over the exact per-leg references.

    Reciprocal-rank fusion mirrors the Rust ``rrf_fuse`` math exactly: each id accrues ``1 / (rrf_k + rank + 1)`` with
    1-based ranks summed across the legs that contain it. Weighted fusion forms ``vector_weight * vector_norm +
    (1 - vector_weight) * text_norm`` over the min-max normalized leg scores. Ties break on the id so the fused order
    is deterministic where the Rust hash-map order is not.

    Args:
        fusion_ast: The single-key fusion specification, ``{"rrf": {"k": ...}}`` or ``{"weighted": {...}}``.
        vector_ids: The exact vector leg ids in best-first order.
        vector_scores: The exact vector leg distances aligned with ``vector_ids``.
        text_ids: The exact BM25 leg ids in best-first order.
        text_scores: The exact BM25 leg scores aligned with ``text_ids``.
        k: The number of fused results to return.

    Returns:
        The fused top-k ids in best-first order.

    Raises:
        FusionReplayError: If the specification shape or its parameters are invalid.
    """
    if not isinstance(fusion_ast, dict) or len(fusion_ast) != 1:
        raise FusionReplayError(f"fusion must be a single-key tagged object: {fusion_ast!r}")
    tag, body = next(iter(fusion_ast.items()))
    if not isinstance(body, dict):
        raise FusionReplayError(f"fusion body must be an object: {body!r}")
    if tag == "rrf":
        rrf_k: Any = body.get("k", DEFAULT_RRF_K)
        if isinstance(rrf_k, bool) or not isinstance(rrf_k, (int, float)) or rrf_k <= 0:
            raise FusionReplayError(f"rrf k must be a positive number: {rrf_k!r}")
        fused: dict[Any, float] = {}
        for leg in (vector_ids, text_ids):
            for rank, rid in enumerate(leg):
                fused[rid] = fused.get(rid, 0.0) + 1.0 / (float(rrf_k) + rank + 1.0)
        return sorted(fused, key=lambda rid: (-fused[rid], rid))[:k]
    if tag == "weighted":
        weight: Any = body.get("vector_weight")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not 0.0 <= float(weight) <= 1.0:
            raise FusionReplayError(f"weighted vector_weight must be in [0, 1]: {weight!r}")
        vector_norm: dict[Any, float] = normalize_leg(vector_ids, vector_scores, lower_is_better=True)
        text_norm: dict[Any, float] = normalize_leg(text_ids, text_scores, lower_is_better=False)
        weight_value: float = float(weight)
        union: set[Any] = set(vector_norm) | set(text_norm)
        scored: dict[Any, float] = {
            rid: weight_value * vector_norm.get(rid, 0.0) + (1.0 - weight_value) * text_norm.get(rid, 0.0)
            for rid in union
        }
        return sorted(scored, key=lambda rid: (-scored[rid], rid))[:k]
    raise FusionReplayError(f"unknown fusion tag: {tag!r}")


def skipped_score(sample: RecallSample, reason: str, version_drift: bool = False) -> SampleScore:
    """Build the score record for a skipped sample.

    Args:
        sample: The sample that was skipped.
        reason: The bounded-cardinality skip reason.
        version_drift: Whether the dataset was opened at a drifted version before the skip.

    Returns:
        A score with ``recall=None`` and the reason recorded.
    """
    return SampleScore(
        sample_id=sample.sample_id,
        org_id=sample.org_id,
        k=sample.k,
        query_type=sample.query_type,
        nprobes_min=sample.nprobes_min,
        nprobes_max=sample.nprobes_max,
        refine_factor=sample.refine_factor,
        recall=None,
        version_drift=version_drift,
        skip_reason=reason,
    )


def scored_sample(sample: RecallSample, recall: float, ndcg: float, mrr: float, version_drift: bool) -> SampleScore:
    """Build the score record for a successfully scored sample.

    Args:
        sample: The scored sample.
        recall: The measured recall@k.
        ndcg: The measured nDCG@k.
        mrr: The measured reciprocal rank of the exact top result.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The populated score record.
    """
    return SampleScore(
        sample_id=sample.sample_id,
        org_id=sample.org_id,
        k=sample.k,
        query_type=sample.query_type,
        nprobes_min=sample.nprobes_min,
        nprobes_max=sample.nprobes_max,
        refine_factor=sample.refine_factor,
        recall=recall,
        ndcg=ndcg,
        mrr=mrr,
        version_drift=version_drift,
        skip_reason=None,
    )


def resolve_filter_sql(sample: RecallSample, schema_columns: frozenset[str]) -> tuple[str | None, str | None]:
    """Translate the sample's filter AST into a scanner filter string.

    Args:
        sample: The sample whose filter is translated.
        schema_columns: The dataset schema's column names, for filter validation.

    Returns:
        ``(filter_sql, skip_reason)`` where exactly one is None: a translated filter string with no skip reason, a
        None filter with no skip reason for unfiltered samples, or a None filter with the ``filter_translation`` skip
        reason when translation failed.
    """
    if sample.filter_ast is None:
        return None, None
    try:
        return filter_ast_to_sql(sample.filter_ast, schema_columns), None
    except FilterTranslationError:
        return None, "filter_translation"


def score_vector_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    default_distance: str,
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one vector sample against an already-opened dataset.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The vector sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    distance_type: str = sample.distance_type or default_distance
    query: np.ndarray = np.asarray(sample.query_vector, dtype=np.float64)
    try:
        true_ids, candidate_count = brute_force_top_k(
            dataset,
            query,
            sample.k,
            distance_type,
            config.id_column,
            config.vector_column,
            filter_sql,
            config.batch_size,
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error", version_drift)
    if candidate_count == 0:
        return skipped_score(sample, "empty_candidate_set", version_drift)
    recall, ndcg, mrr = ranking_quality(true_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr, version_drift)


def score_text_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    schema_columns: frozenset[str],
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one text sample against the exact BM25 reference at the pinned version.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The text sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        schema_columns: The dataset schema's column names, for text-column validation.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    try:
        field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
            sample.text_query, sample.text_columns, schema_columns
        )
    except TextQueryTranslationError:
        return skipped_score(sample, "text_query_translation", version_drift)
    try:
        true_ids, true_scores, candidate_count = bm25_top_k(
            dataset, field_queries, sample.k, config.id_column, filter_sql, config.batch_size
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error", version_drift)
    del true_scores
    if candidate_count == 0:
        return skipped_score(sample, "empty_candidate_set", version_drift)
    recall, ndcg, mrr = ranking_quality(true_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr, version_drift)


def score_hybrid_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    filter_sql: str | None,
    schema_columns: frozenset[str],
    default_distance: str,
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one hybrid sample by fusing exact vector and exact BM25 references at the pinned version.

    The vector and text legs are each computed to the fused ``k`` (the common case where the leg ``k`` inherits the
    fused ``k``), then merged with the recorded fusion strategy before grading the served ids.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The hybrid sample to score.
        filter_sql: The translated scanner filter, or None for an unfiltered scan.
        schema_columns: The dataset schema's column names, for text-column validation.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with a skip reason when scoring was not possible.
    """
    try:
        field_queries: list[tuple[str, list[str], str, float]] = text_query_field_queries(
            sample.text_query, sample.text_columns, schema_columns
        )
    except TextQueryTranslationError:
        return skipped_score(sample, "text_query_translation", version_drift)
    distance_type: str = sample.distance_type or default_distance
    query: np.ndarray = np.asarray(sample.query_vector, dtype=np.float64)
    try:
        vector_ids, vector_scores, vector_count = brute_force_top_k_scored(
            dataset,
            query,
            sample.k,
            distance_type,
            config.id_column,
            config.vector_column,
            filter_sql,
            config.batch_size,
        )
        text_ids, text_scores, text_count = bm25_top_k(
            dataset, field_queries, sample.k, config.id_column, filter_sql, config.batch_size
        )
    except (ValueError, OSError, RuntimeError):
        return skipped_score(sample, "scan_error", version_drift)
    if vector_count == 0 and text_count == 0:
        return skipped_score(sample, "empty_candidate_set", version_drift)
    try:
        fused_ids: list[Any] = fuse_legs(
            sample.fusion or {}, vector_ids, vector_scores, text_ids, text_scores, sample.k
        )
    except FusionReplayError:
        return skipped_score(sample, "fusion_replay", version_drift)
    candidate_count: int = max(vector_count, text_count)
    recall, ndcg, mrr = ranking_quality(fused_ids, list(sample.result_ids or ()), sample.k, candidate_count)
    return scored_sample(sample, recall, ndcg, mrr, version_drift)


def score_sample(
    dataset: lance.LanceDataset,
    sample: RecallSample,
    schema_columns: frozenset[str],
    default_distance: str,
    config: RecallJobConfig,
    version_drift: bool,
) -> SampleScore:
    """Score one sample against an already-opened dataset, dispatching on the query type.

    Args:
        dataset: The dataset checked out at the sample's recorded version, or at latest on drift.
        sample: The sample to score.
        schema_columns: The dataset schema's column names, for filter and text-column validation.
        default_distance: The index-metric default used when the sample omits a distance type.
        config: The job configuration.
        version_drift: Whether the dataset was opened at a drifted version.

    Returns:
        The sample's score, with ``recall=None`` and a reason when the sample had to be skipped.
    """
    if sample.result_ids is None:
        return skipped_score(sample, "null_result_ids", version_drift)
    filter_sql, filter_skip = resolve_filter_sql(sample, schema_columns)
    if filter_skip is not None:
        return skipped_score(sample, filter_skip, version_drift)
    if sample.query_type == "text":
        return score_text_sample(dataset, sample, filter_sql, schema_columns, config, version_drift)
    if sample.query_type == "hybrid":
        return score_hybrid_sample(dataset, sample, filter_sql, schema_columns, default_distance, config, version_drift)
    return score_vector_sample(dataset, sample, filter_sql, default_distance, config, version_drift)


def score_version_group(
    uri: str, version: int, samples: list[RecallSample], config: RecallJobConfig
) -> list[SampleScore]:
    """Score every sample of one ``(uri, version)`` group on an executor.

    Opens the dataset once at the recorded version (falling back to latest with a drift flag when the version was
    cleaned up), resolves the index-metric default distance once, and then scores each sample.

    Args:
        uri: The dataset URI shared by the group.
        version: The recorded dataset version shared by the group.
        samples: The samples to score.
        config: The job configuration.

    Returns:
        One score per sample.
    """
    telemetry: Telemetry = Telemetry.create(config.telemetry)
    with telemetry.timed("recall.group_ms"):
        dataset, version_drift = resolve_dataset(uri, version, config.storage_options)
        if dataset is None:
            telemetry.incr("recall.dataset_missing")
            return [skipped_score(sample, "dataset_missing") for sample in samples]
        schema_columns: frozenset[str] = frozenset(dataset.schema.names)
        if config.id_column not in schema_columns or config.vector_column not in schema_columns:
            telemetry.incr("recall.missing_columns")
            return [skipped_score(sample, "missing_columns", version_drift) for sample in samples]
        if version_drift:
            telemetry.incr("recall.version_drift")
        default_distance: str = index_default_distance_type(dataset, config.vector_column)
        return [
            score_sample(dataset, sample, schema_columns, default_distance, config, version_drift) for sample in samples
        ]


def optional_label(value: int | None, fallback: str) -> str:
    """Render an optional integer bucket key for labels and tags.

    Args:
        value: The optional value.
        fallback: The label used when the value is None.

    Returns:
        The rendered label.
    """
    return fallback if value is None else str(value)


def rpc_bucket_label(nprobes_min: int | None, nprobes_max: int | None, refine_factor: int | None) -> str:
    """Build the table label for one RPC-parameter bucket.

    Args:
        nprobes_min: Lower nprobes bound, or None for the index default.
        nprobes_max: Upper nprobes bound, or None for the index default.
        refine_factor: Refine factor, or None when unset.

    Returns:
        The bucket label, for example ``rpc nprobes=8..32 refine=2``.
    """
    low: str = optional_label(nprobes_min, "default")
    high: str = optional_label(nprobes_max, "default")
    refine: str = optional_label(refine_factor, "unset")
    return f"rpc nprobes={low}..{high} refine={refine}"


def mean_or_none(values: list[float]) -> float | None:
    """Return the mean of the values, or None when the list is empty.

    Args:
        values: The values to average.

    Returns:
        The mean, or None.
    """
    return float(np.asarray(values, dtype=np.float64).mean()) if values else None


def summarize_bucket(
    bucket: str,
    scores: list[SampleScore],
    nprobes_min: int | None = None,
    nprobes_max: int | None = None,
    refine_factor: int | None = None,
    query_type: str | None = None,
    is_rpc_bucket: bool = False,
    is_query_type_bucket: bool = False,
) -> AggregateRow:
    """Aggregate one bucket of scores into a report row.

    Args:
        bucket: The bucket label.
        scores: The scores in the bucket, including skipped ones.
        nprobes_min: RPC bucket key carried for metric tagging.
        nprobes_max: RPC bucket key carried for metric tagging.
        refine_factor: RPC bucket key carried for metric tagging.
        query_type: Query-type bucket key carried for metric tagging.
        is_rpc_bucket: Whether this row is an RPC-parameter bucket eligible for metric emission.
        is_query_type_bucket: Whether this row is a query-type bucket eligible for metric emission.

    Returns:
        The aggregate row with the mean recall, nDCG, and MRR plus recall p50 and p95 over the scored samples only.
    """
    recalls: list[float] = [score.recall for score in scores if score.recall is not None]
    ndcgs: list[float] = [score.ndcg for score in scores if score.ndcg is not None]
    mrrs: list[float] = [score.mrr for score in scores if score.mrr is not None]
    values: np.ndarray = np.asarray(recalls, dtype=np.float64)
    return AggregateRow(
        bucket=bucket,
        samples=len(recalls),
        mean_recall=mean_or_none(recalls),
        mean_ndcg=mean_or_none(ndcgs),
        mean_mrr=mean_or_none(mrrs),
        p50=float(np.percentile(values, 50)) if recalls else None,
        p95=float(np.percentile(values, 95)) if recalls else None,
        drift_count=sum(1 for score in scores if score.version_drift),
        skip_count=sum(1 for score in scores if score.skip_reason is not None),
        nprobes_min=nprobes_min,
        nprobes_max=nprobes_max,
        refine_factor=refine_factor,
        query_type=query_type,
        is_rpc_bucket=is_rpc_bucket,
        is_query_type_bucket=is_query_type_bucket,
    )


def aggregate_scores(scores: list[SampleScore]) -> list[AggregateRow]:
    """Aggregate scores into the report rows: overall, per RPC bucket, per query type, then per org.

    Args:
        scores: Every per-sample scoring outcome.

    Returns:
        The aggregate rows in table order.
    """
    rows: list[AggregateRow] = [summarize_bucket("overall", scores)]
    rpc_groups: dict[tuple[int | None, int | None, int | None], list[SampleScore]] = {}
    query_type_groups: dict[str, list[SampleScore]] = {}
    org_groups: dict[str, list[SampleScore]] = {}
    for score in scores:
        rpc_key: tuple[int | None, int | None, int | None] = (score.nprobes_min, score.nprobes_max, score.refine_factor)
        rpc_groups.setdefault(rpc_key, []).append(score)
        query_type_groups.setdefault(score.query_type, []).append(score)
        org_groups.setdefault(score.org_id, []).append(score)
    for rpc_key in sorted(rpc_groups, key=lambda key: rpc_bucket_label(*key)):
        rows.append(
            summarize_bucket(
                rpc_bucket_label(*rpc_key),
                rpc_groups[rpc_key],
                nprobes_min=rpc_key[0],
                nprobes_max=rpc_key[1],
                refine_factor=rpc_key[2],
                is_rpc_bucket=True,
            )
        )
    for query_type in sorted(query_type_groups):
        rows.append(
            summarize_bucket(
                f"query_type {query_type}",
                query_type_groups[query_type],
                query_type=query_type,
                is_query_type_bucket=True,
            )
        )
    for org_id in sorted(org_groups):
        rows.append(summarize_bucket(f"org {org_id}", org_groups[org_id]))
    return rows


def format_metric(value: float | None) -> str:
    """Format one recall statistic for the table.

    Args:
        value: The statistic, or None when the bucket has no scored samples.

    Returns:
        The fixed-precision rendering, or ``-`` for None.
    """
    return "-" if value is None else f"{value:.4f}"


def format_report(report: RecallReport) -> str:
    """Render the recall report as a fixed-width text table.

    Args:
        report: The report to render.

    Returns:
        The multi-line table, followed by parse-skip and score-skip reason summaries when any were counted.
    """
    width: int = max([len("bucket"), *(len(row.bucket) for row in report.rows)])
    header: str = (
        f"{'bucket':<{width}}  {'samples':>7}  {'recall':>8}  {'ndcg':>8}  {'mrr':>8}  "
        f"{'p50':>8}  {'p95':>8}  {'drift':>5}  {'skipped':>7}"
    )
    lines: list[str] = [header]
    for row in report.rows:
        lines.append(
            f"{row.bucket:<{width}}  {row.samples:>7}  {format_metric(row.mean_recall):>8}  "
            f"{format_metric(row.mean_ndcg):>8}  {format_metric(row.mean_mrr):>8}  "
            f"{format_metric(row.p50):>8}  {format_metric(row.p95):>8}  {row.drift_count:>5}  {row.skip_count:>7}"
        )
    if report.parse_skips:
        rendered: str = ", ".join(f"{reason}={count}" for reason, count in sorted(report.parse_skips.items()))
        lines.append(f"parse skips: {rendered}")
    score_skips: Counter[str] = Counter(score.skip_reason for score in report.scores if score.skip_reason is not None)
    if score_skips:
        rendered = ", ".join(f"{reason}={count}" for reason, count in sorted(score_skips.items()))
        lines.append(f"score skips: {rendered}")
    return "\n".join(lines)


def emit_bucket_metrics(telemetry: Telemetry, row: AggregateRow, tags: list[str]) -> None:
    """Emit the recall, nDCG, and MRR gauges for one bucket under shared tags.

    Args:
        telemetry: The driver telemetry facade.
        row: The aggregate row to emit.
        tags: The shared metric tags for the bucket.
    """
    if row.mean_recall is not None:
        telemetry.gauge("recall.measured", row.mean_recall, tags=tags)
    if row.mean_ndcg is not None:
        telemetry.gauge("recall.ndcg", row.mean_ndcg, tags=tags)
    if row.mean_mrr is not None:
        telemetry.gauge("recall.mrr", row.mean_mrr, tags=tags)


def emit_recall_metrics(telemetry: Telemetry, rows: list[AggregateRow]) -> None:
    """Emit the recall, nDCG, and MRR gauges per RPC-parameter bucket and per query-type bucket.

    RPC buckets are tagged with the RPC parameters and query-type buckets with the query type. Both tag sets are
    bounded-cardinality. Org-level numbers stay in the stdout table so the metric tag cardinality does not explode
    with the organization count.

    Args:
        telemetry: The driver telemetry facade.
        rows: The aggregate rows of the report.
    """
    for row in rows:
        if row.is_rpc_bucket:
            emit_bucket_metrics(
                telemetry,
                row,
                [
                    f"nprobes_min:{optional_label(row.nprobes_min, 'default')}",
                    f"nprobes_max:{optional_label(row.nprobes_max, 'default')}",
                    f"refine_factor:{optional_label(row.refine_factor, 'unset')}",
                ],
            )
        elif row.is_query_type_bucket and row.query_type is not None:
            emit_bucket_metrics(telemetry, row, [f"query_type:{row.query_type}"])


class RecallAuditJob:
    """Replays sampled vector, text, and hybrid queries against pinned dataset versions and reports retrieval quality.

    Each sample is scored with recall@k, nDCG@k, and MRR against an exact reference computed at the recorded dataset
    version: brute-force nearest neighbors for vector legs and exact Okapi BM25 for text legs, fused with the recorded
    strategy for hybrid samples.
    """

    def __init__(self, config: RecallJobConfig) -> None:
        """Initialize the job.

        Args:
            config: The job configuration.
        """
        self.config: RecallJobConfig = config

    def run(self, spark: SparkSession, source: SpanSource, from_ms: int, to_ms: int) -> RecallReport:
        """Fetch, parse, score, aggregate, and report one window of recall samples.

        The driver fetches and parses the spans and groups samples by ``(dataset_uri, dataset_version)``. Executors do
        the heavy work: each Spark task opens one group's dataset at the recorded version and brute-force scores its
        samples. The driver aggregates, logs the table, and emits the per-RPC-bucket gauges.

        Args:
            spark: Active Spark session.
            source: The span source to fetch from.
            from_ms: Window start in epoch milliseconds, inclusive.
            to_ms: Window end in epoch milliseconds, inclusive.

        Returns:
            The full report, for callers and tests.
        """
        config: RecallJobConfig = self.config
        telemetry: Telemetry = Telemetry.create(config.telemetry)
        with telemetry.span("lance.recall.run") as run_span:
            records: list[dict[str, Any]] = list(source.fetch(from_ms, to_ms, config.max_samples))
            samples, parse_skips = parse_samples(records)
            groups: dict[tuple[str, int], list[RecallSample]] = {}
            for sample in samples:
                key: tuple[str, int] = (sample_dataset_uri(config.base_uri, sample), sample.dataset_version)
                groups.setdefault(key, []).append(sample)
            items: list[tuple[str, int, list[RecallSample]]] = [
                (uri, version, group) for (uri, version), group in groups.items()
            ]

            def score_group(item: tuple[str, int, list[RecallSample]]) -> list[SampleScore]:
                """Score one version group on an executor.

                Args:
                    item: The ``(uri, version, samples)`` group.

                Returns:
                    One score per sample in the group.
                """
                return score_version_group(item[0], item[1], item[2], config)

            scores: list[SampleScore] = []
            if items:
                with telemetry.timed("recall.score_ms"):
                    collected: list[list[SampleScore]] = (
                        spark.sparkContext.parallelize(items, len(items)).map(score_group).collect()
                    )
                scores = [score for group in collected for score in group]
            report: RecallReport = RecallReport(rows=aggregate_scores(scores), scores=scores, parse_skips=parse_skips)
            logger.info("recall audit results:\n%s", format_report(report))
            emit_recall_metrics(telemetry, report.rows)
            run_span.set_tag("samples", len(samples))
            telemetry.gauge("recall.samples_fetched", len(records))
            telemetry.gauge("recall.samples_scored", sum(1 for score in scores if score.recall is not None))
            telemetry.gauge("recall.samples_skipped", sum(1 for score in scores if score.skip_reason is not None))
            return report
