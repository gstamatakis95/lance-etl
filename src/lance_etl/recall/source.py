"""Recall span fetching and parsing.

Span fetching goes through the :class:`SpanSource` protocol. :class:`DatadogSpanSource` talks to the Datadog Spans
search API v2 with stdlib ``urllib.request`` so no new heavy dependency is introduced. :class:`InMemorySpanSource`
feeds the same pipeline from a list of attribute dictionaries and exists for tests.

Every flat ``recall.*`` attribute dictionary is parsed into a :class:`RecallSample` by :func:`parse_recall_sample`,
with malformed records counted by a bounded-cardinality reason key rather than raising out of the batch.
"""

from __future__ import annotations

import json
import os
import urllib.request
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Protocol

from lance_etl.recall.config import DISTANCE_TYPES, PATH_COMPONENT_PATTERN, QUERY_TYPES, SPANS_SEARCH_PATH


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
        query_type: The captured query type, one of :data:`~lance_etl.recall.config.QUERY_TYPES`. Legacy captures
            default to ``vector``.
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

    The character allowlist alone permits the literal components ``.`` and ``..``, which are valid path segments
    that walk up the directory tree instead of naming a routing key, so both are rejected explicitly in addition to
    the regex match.

    Args:
        attrs: The flat attribute dictionary.
        key: The attribute key.

    Returns:
        The validated component.

    Raises:
        SampleParseError: If the attribute is missing, fails path-component validation, or is a directory-traversal
            component (``.`` or ``..``).
    """
    value: str = attr_string(attrs, key)
    if not PATH_COMPONENT_PATTERN.match(value) or value in {".", ".."}:
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
