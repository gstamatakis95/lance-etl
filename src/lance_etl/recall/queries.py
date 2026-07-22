"""Span-to-query translation: filter AST to SQL, text tokenization, text-query trees, and hybrid fusion replay.

The repository forbids accepting raw SQL strings in the gRPC filter API because client-supplied strings cannot be
trusted. :func:`filter_ast_to_sql` honors that rule even though it hands the Lance scanner a SQL string, because the
string never crosses a trust boundary. It is generated here, internally, from the typed filter AST that the Rust
service captured from its own typed ``Filter`` proto. Clients never supply strings at any point. Every column
identifier is validated against both the ``[A-Za-z_][A-Za-z0-9_]*`` allowlist and the dataset schema, and every
literal is rendered through the typed value renderer, so the generated string is a pure function of validated typed
data. :func:`text_query_field_queries` and :func:`fuse_legs` apply the same validated-typed-data discipline to the
captured text-query node tree and hybrid fusion specification. :func:`tokenize_text` is the single reference
tokenizer shared by query-term tokenization here and document tokenization in
:mod:`~lance_etl.recall.scoring`, so both sides of a BM25 match use the same vocabulary.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

from lance_etl.recall.config import COMPARE_OPS, DEFAULT_RRF_K, FILTER_COLUMN_PATTERN, TEXT_OPERATORS, TOKEN_PATTERN
from lance_etl.recall.source import RecallSample


class FilterTranslationError(ValueError):
    """A captured filter AST failed strict validation during SQL generation."""


class TextQueryTranslationError(ValueError):
    """A captured text-query AST could not be translated into an exact BM25 reference plan."""


class FusionReplayError(ValueError):
    """A captured hybrid fusion specification could not be replayed."""


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
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise FilterTranslationError(f"float literal payload must be a finite number: {raw!r}")
        try:
            rendered_float: float = float(raw)
        except (OverflowError, ValueError) as error:
            raise FilterTranslationError(f"float literal payload must be a finite number: {raw!r}") from error
        if not math.isfinite(rendered_float):
            raise FilterTranslationError(f"float literal payload must be a finite number: {raw!r}")
        return repr(rendered_float)
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


def filter_body_object(tag: str, body: Any) -> dict[str, Any]:
    """Validate and return an object-shaped filter node body.

    Args:
        tag: Filter node tag used in the validation error.
        body: Captured node body.

    Returns:
        The validated dictionary body.

    Raises:
        FilterTranslationError: If the body is not an object.
    """
    if not isinstance(body, dict):
        raise FilterTranslationError(f"{tag} body must be an object: {body!r}")
    return body


def render_in_list_filter(body: Any, columns: frozenset[str]) -> str:
    """Validate and render one membership filter node body.

    Args:
        body: Captured ``in_list`` body.
        columns: Dataset schema column names.

    Returns:
        The rendered membership expression.

    Raises:
        FilterTranslationError: If the body, values, or negation flag is malformed.
    """
    values_body: dict[str, Any] = filter_body_object("in_list", body)
    column: str = render_filter_column(values_body.get("column"), columns)
    values: Any = values_body.get("values")
    if not isinstance(values, list) or not values:
        raise FilterTranslationError("in_list requires a non-empty values list")
    negated: Any = values_body.get("negated", False)
    if not isinstance(negated, bool):
        raise FilterTranslationError("in_list negated must be a boolean")
    rendered: str = ", ".join(render_filter_literal(value) for value in values)
    keyword: str = "NOT IN" if negated else "IN"
    return f"({column} {keyword} ({rendered}))"


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
        body = filter_body_object(tag, body)
        op: Any = body.get("op")
        if op not in COMPARE_OPS:
            raise FilterTranslationError(f"unknown compare op: {op!r}")
        column: str = render_filter_column(body.get("column"), columns)
        return f"({column} {COMPARE_OPS[op]} {render_filter_literal(body.get('value'))})"
    if tag == "in_list":
        return render_in_list_filter(body, columns)
    if tag == "is_null":
        body = filter_body_object(tag, body)
        return f"({render_filter_column(body.get('column'), columns)} IS NULL)"
    if tag == "is_not_null":
        body = filter_body_object(tag, body)
        return f"({render_filter_column(body.get('column'), columns)} IS NOT NULL)"
    if tag == "between":
        body = filter_body_object(tag, body)
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


def tokenize_text(text: Any) -> list[str]:
    """Tokenize one text value into lowercase word tokens.

    The reference tokenizer is a Unicode word splitter over the lowercased string. It approximates the default Lance
    full-text tokenizer. A divergence between this tokenizer and the index tokenizer shows up as a sub-1.0 text recall,
    which the report attributes to the staleness and correctness signal documented at the recall package level.

    Args:
        text: The cell value, which may be None or non-string.

    Returns:
        The token list, empty when the value is None or not a string.
    """
    if not isinstance(text, str):
        return []
    return TOKEN_PATTERN.findall(text.lower())


def parse_text_boost(value: Any, context: str) -> float:
    """Parse one finite numeric text-query boost with a normalized error type.

    Args:
        value: Captured boost value.
        context: Human-readable clause context for the validation error.

    Returns:
        The finite boost as a float.

    Raises:
        TextQueryTranslationError: If the value is not a finite JSON number.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TextQueryTranslationError(f"{context} boost must be a finite number: {value!r}")
    try:
        boost: float = float(value)
    except (OverflowError, ValueError) as error:
        raise TextQueryTranslationError(f"{context} boost must be a finite number: {value!r}") from error
    if not math.isfinite(boost):
        raise TextQueryTranslationError(f"{context} boost must be a finite number: {value!r}")
    return boost


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
        boost: float = parse_text_boost(body.get("boost", 1.0), "match")
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
        raw_boosts: Any = body.get("boosts")
        boosts: Any = [1.0] * len(target_columns) if raw_boosts is None or raw_boosts == [] else raw_boosts
        if not isinstance(boosts, list) or len(boosts) != len(target_columns):
            raise TextQueryTranslationError("multi_match boosts must match the columns length")
        return [
            (validate_text_column(target, columns), terms, operator, parse_text_boost(weight, "multi_match"))
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
    record_ids: list[Any],
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
        record_ids: The exact vector leg ids in best-first order.
        vector_scores: The exact vector leg distances aligned with ``record_ids``.
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
        for leg in (record_ids, text_ids):
            for rank, rid in enumerate(leg):
                fused[rid] = fused.get(rid, 0.0) + 1.0 / (float(rrf_k) + rank + 1.0)
        return sorted(fused, key=lambda rid: (-fused[rid], rid))[:k]
    if tag == "weighted":
        weight: Any = body.get("vector_weight")
        if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not 0.0 <= float(weight) <= 1.0:
            raise FusionReplayError(f"weighted vector_weight must be in [0, 1]: {weight!r}")
        vector_norm: dict[Any, float] = normalize_leg(record_ids, vector_scores, lower_is_better=True)
        text_norm: dict[Any, float] = normalize_leg(text_ids, text_scores, lower_is_better=False)
        weight_value: float = float(weight)
        union: set[Any] = set(vector_norm) | set(text_norm)
        scored: dict[Any, float] = {
            rid: weight_value * vector_norm.get(rid, 0.0) + (1.0 - weight_value) * text_norm.get(rid, 0.0)
            for rid in union
        }
        return sorted(scored, key=lambda rid: (-scored[rid], rid))[:k]
    raise FusionReplayError(f"unknown fusion tag: {tag!r}")
