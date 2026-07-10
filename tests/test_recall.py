"""Tests for recall sample parsing, filter AST translation, span sources, and the recall CLI surface."""

from __future__ import annotations

import json
from typing import Any

import pytest

import lance_etl.tools.cli as tools_cli
from lance_etl.recall import (
    DatadogSpanSource,
    FilterTranslationError,
    InMemorySpanSource,
    RecallSample,
    build_spans_request_body,
    filter_ast_to_sql,
    flatten_recall_attributes,
    parse_recall_sample,
    parse_samples,
)

COLUMNS: frozenset[str] = frozenset({"category", "value", "score", "flag", "vector_id"})


def make_attrs(**overrides: Any) -> dict[str, Any]:
    """Build a valid flat recall attribute dictionary with optional overrides.

    Args:
        overrides: Attribute overrides keyed by the suffix after ``recall.``. A None value removes the attribute.

    Returns:
        The flat attribute dictionary.
    """
    attrs: dict[str, Any] = {
        "recall.sample": "true",
        "recall.sample_id": "0b6e8a3e-aaaa-bbbb-cccc-1234567890ab",
        "recall.captured_at_unix_ms": "1700000000000",
        "recall.org_id": "acme",
        "recall.tenant_id": "tenant1",
        "recall.namespace": "ns1",
        "recall.dataset_version": "7",
        "recall.k": "10",
        "recall.query_vector": json.dumps([0.1] * 8),
        "recall.result_ids": json.dumps([1, 2, 3]),
        "recall.result_distances": json.dumps([0.1, 0.2, 0.3]),
    }
    for key, value in overrides.items():
        full: str = f"recall.{key}"
        if value is None:
            attrs.pop(full, None)
        else:
            attrs[full] = value
    return attrs


class TestSampleParsing:
    """Recall spans parse into samples and malformed records are skipped with counted reasons."""

    def test_valid_sample_parses(self) -> None:
        """A complete capture parses into a sample with the recorded fields."""
        sample: RecallSample = parse_recall_sample(make_attrs())
        assert sample.sample_id == "0b6e8a3e-aaaa-bbbb-cccc-1234567890ab"
        assert sample.captured_at_unix_ms == 1700000000000
        assert (sample.org_id, sample.tenant_id, sample.namespace) == ("acme", "tenant1", "ns1")
        assert sample.dataset_version == 7
        assert sample.k == 10
        assert sample.query_vector == tuple([0.1] * 8)
        assert sample.result_ids == (1, 2, 3)
        assert sample.result_distances == (0.1, 0.2, 0.3)

    def test_optional_fields_default_to_none(self) -> None:
        """Absent nprobes, refine factor, distance type, and filter parse as None."""
        sample: RecallSample = parse_recall_sample(make_attrs())
        assert sample.nprobes_min is None
        assert sample.nprobes_max is None
        assert sample.refine_factor is None
        assert sample.distance_type is None
        assert sample.filter_ast is None

    def test_optional_fields_parse_when_present(self) -> None:
        """Present optional fields parse to their typed values."""
        ast: dict[str, Any] = {"is_null": {"column": "category"}}
        sample: RecallSample = parse_recall_sample(
            make_attrs(
                nprobes_min="8", nprobes_max="32", refine_factor="2", distance_type="cosine", filter=json.dumps(ast)
            )
        )
        assert sample.nprobes_min == 8
        assert sample.nprobes_max == 32
        assert sample.refine_factor == 2
        assert sample.distance_type == "cosine"
        assert sample.filter_ast == ast

    def test_null_result_ids_parse_as_none(self) -> None:
        """A JSON null result-id capture parses as None instead of being a parse skip."""
        sample: RecallSample = parse_recall_sample(make_attrs(result_ids="null"))
        assert sample.result_ids is None
        samples, skips = parse_samples([make_attrs(result_ids="null")])
        assert len(samples) == 1
        assert skips == {}

    @pytest.mark.parametrize(
        ("overrides", "reason"),
        [
            ({"sample_id": None}, "missing:recall.sample_id"),
            ({"k": None}, "missing:recall.k"),
            ({"k": "0"}, "invalid:recall.k"),
            ({"k": "ten"}, "invalid:recall.k"),
            ({"dataset_version": "x"}, "invalid:recall.dataset_version"),
            ({"captured_at_unix_ms": None}, "missing:recall.captured_at_unix_ms"),
            ({"org_id": "../etc"}, "invalid:recall.org_id"),
            ({"org_id": "."}, "invalid:recall.org_id"),
            ({"org_id": ".."}, "invalid:recall.org_id"),
            ({"tenant_id": "a/b"}, "invalid:recall.tenant_id"),
            ({"namespace": ""}, "missing:recall.namespace"),
            ({"query_vector": "not json"}, "invalid:recall.query_vector"),
            ({"query_vector": "[]"}, "invalid:recall.query_vector"),
            ({"query_vector": '["a"]'}, "invalid:recall.query_vector"),
            ({"query_vector": None}, "missing:recall.query_vector"),
            ({"distance_type": "manhattan"}, "invalid:recall.distance_type"),
            ({"result_ids": '{"a":1}'}, "invalid:recall.result_ids"),
            ({"result_distances": '["x"]'}, "invalid:recall.result_distances"),
            ({"filter": "[1,2]"}, "invalid:recall.filter"),
            ({"nprobes_min": "low"}, "invalid:recall.nprobes_min"),
        ],
    )
    def test_malformed_records_skip_with_reason(self, overrides: dict[str, Any], reason: str) -> None:
        """Each malformed capture is skipped and counted under its bounded reason key."""
        samples, skips = parse_samples([make_attrs(**overrides)])
        assert samples == []
        assert skips == {reason: 1}

    def test_mixed_batch_counts_each_reason(self) -> None:
        """A batch of good and bad records yields the good samples plus per-reason counts."""
        records: list[dict[str, Any]] = [
            make_attrs(),
            make_attrs(sample_id=None),
            make_attrs(sample_id=None),
            make_attrs(query_vector="oops"),
        ]
        samples, skips = parse_samples(records)
        assert len(samples) == 1
        assert skips == {"missing:recall.sample_id": 2, "invalid:recall.query_vector": 1}

    @pytest.mark.parametrize("distance", ["l2", "cosine", "dot", "hamming"])
    def test_all_distance_types_accepted(self, distance: str) -> None:
        """Every documented distance type passes validation."""
        assert parse_recall_sample(make_attrs(distance_type=distance)).distance_type == distance


class TestFilterTranslation:
    """The typed filter AST translates to a validated scanner filter string."""

    @pytest.mark.parametrize(
        ("op", "symbol"),
        [("eq", "="), ("ne", "<>"), ("lt", "<"), ("le", "<="), ("gt", ">"), ("ge", ">=")],
    )
    def test_compare_ops(self, op: str, symbol: str) -> None:
        """Every compare operator maps to its SQL symbol."""
        node: dict[str, Any] = {"compare": {"column": "value", "op": op, "value": {"int": 3}}}
        assert filter_ast_to_sql(node, COLUMNS) == f"(value {symbol} 3)"

    def test_string_literal_is_escaped(self) -> None:
        """Embedded single quotes are doubled so the literal cannot break out."""
        node: dict[str, Any] = {"compare": {"column": "category", "op": "eq", "value": {"string": "O'Brien"}}}
        assert filter_ast_to_sql(node, COLUMNS) == "(category = 'O''Brien')"

    def test_float_and_bool_literals(self) -> None:
        """Float and bool payloads render as typed SQL literals."""
        flt: dict[str, Any] = {"compare": {"column": "score", "op": "gt", "value": {"float": 0.5}}}
        assert filter_ast_to_sql(flt, COLUMNS) == "(score > 0.5)"
        true_node: dict[str, Any] = {"compare": {"column": "flag", "op": "eq", "value": {"bool": True}}}
        assert filter_ast_to_sql(true_node, COLUMNS) == "(flag = TRUE)"
        false_node: dict[str, Any] = {"compare": {"column": "flag", "op": "eq", "value": {"bool": False}}}
        assert filter_ast_to_sql(false_node, COLUMNS) == "(flag = FALSE)"

    def test_in_list(self) -> None:
        """in_list renders an IN membership test over typed literals."""
        node: dict[str, Any] = {
            "in_list": {"column": "category", "values": [{"string": "a"}, {"string": "b"}], "negated": False}
        }
        assert filter_ast_to_sql(node, COLUMNS) == "(category IN ('a', 'b'))"

    def test_in_list_negated(self) -> None:
        """A negated in_list renders NOT IN."""
        node: dict[str, Any] = {"in_list": {"column": "value", "values": [{"int": 1}, {"int": 2}], "negated": True}}
        assert filter_ast_to_sql(node, COLUMNS) == "(value NOT IN (1, 2))"

    def test_is_null_and_is_not_null(self) -> None:
        """Null tests render IS NULL and IS NOT NULL."""
        assert filter_ast_to_sql({"is_null": {"column": "category"}}, COLUMNS) == "(category IS NULL)"
        assert filter_ast_to_sql({"is_not_null": {"column": "category"}}, COLUMNS) == "(category IS NOT NULL)"

    def test_between(self) -> None:
        """Between renders a BETWEEN range with typed bounds."""
        node: dict[str, Any] = {"between": {"column": "value", "low": {"int": 1}, "high": {"int": 9}}}
        assert filter_ast_to_sql(node, COLUMNS) == "(value BETWEEN 1 AND 9)"

    def test_and_or_not_nesting(self) -> None:
        """Boolean combinators nest with explicit parentheses."""
        node: dict[str, Any] = {
            "and": [
                {"compare": {"column": "value", "op": "ge", "value": {"int": 1}}},
                {
                    "or": [
                        {"is_null": {"column": "category"}},
                        {"not": {"compare": {"column": "flag", "op": "eq", "value": {"bool": True}}}},
                    ]
                },
            ]
        }
        expected: str = "((value >= 1) AND ((category IS NULL) OR (NOT (flag = TRUE))))"
        assert filter_ast_to_sql(node, COLUMNS) == expected

    @pytest.mark.parametrize(
        "column",
        ["vector_id; DROP TABLE t", "1abc", "a-b", "a b", "", "col'umn", None, 42],
    )
    def test_malicious_or_invalid_columns_rejected(self, column: Any) -> None:
        """Identifiers outside the allowlist are rejected before schema lookup."""
        node: dict[str, Any] = {"compare": {"column": column, "op": "eq", "value": {"int": 1}}}
        with pytest.raises(FilterTranslationError):
            filter_ast_to_sql(node, COLUMNS)

    def test_unknown_column_rejected_by_schema_membership(self) -> None:
        """A well-formed identifier absent from the schema is rejected."""
        node: dict[str, Any] = {"compare": {"column": "not_a_column", "op": "eq", "value": {"int": 1}}}
        with pytest.raises(FilterTranslationError):
            filter_ast_to_sql(node, COLUMNS)

    @pytest.mark.parametrize(
        "node",
        [
            {"compare": {"column": "value", "op": "like", "value": {"int": 1}}},
            {"regex": {"column": "value"}},
            {"and": []},
            {"or": "value"},
            {"in_list": {"column": "value", "values": [], "negated": False}},
            {"compare": {"column": "value", "op": "eq", "value": {"int": 1}}, "extra": {}},
            "not a node",
            {"compare": {"column": "value", "op": "eq", "value": {"decimal": 1}}},
            {"compare": {"column": "value", "op": "eq", "value": {"int": True}}},
            {"compare": {"column": "value", "op": "eq", "value": {"float": float("inf")}}},
            {"compare": {"column": "value", "op": "eq", "value": {"string": 5}}},
            {"compare": {"column": "value", "op": "eq", "value": {"bool": "yes"}}},
        ],
    )
    def test_invalid_nodes_rejected(self, node: Any) -> None:
        """Unknown tags, operators, literal tags, and degenerate shapes are all rejected."""
        with pytest.raises(FilterTranslationError):
            filter_ast_to_sql(node, COLUMNS)


class TestSpanSources:
    """Both span sources honor the window, the cap, and the capture shapes."""

    def test_in_memory_window_and_cap(self) -> None:
        """The in-memory source filters by capture time and respects the cap."""
        inside: dict[str, Any] = make_attrs(captured_at_unix_ms="1500")
        outside: dict[str, Any] = make_attrs(captured_at_unix_ms="2500")
        source: InMemorySpanSource = InMemorySpanSource(records=[inside, outside, inside, inside])
        fetched: list[dict[str, Any]] = list(source.fetch(1000, 2000, 2))
        assert len(fetched) == 2
        assert all(record["recall.captured_at_unix_ms"] == "1500" for record in fetched)

    def test_in_memory_yields_unparseable_capture_times(self) -> None:
        """A record with an unreadable capture time is still yielded for the parser to count."""
        broken: dict[str, Any] = make_attrs(captured_at_unix_ms="soon")
        source: InMemorySpanSource = InMemorySpanSource(records=[broken])
        assert list(source.fetch(0, 1, 10)) == [broken]

    def test_request_body_shape(self) -> None:
        """The Datadog search body filters on the sample tag, the window, and the page settings."""
        body: dict[str, Any] = build_spans_request_body(1000, 2000, 50, None)
        attributes: dict[str, Any] = body["data"]["attributes"]
        assert body["data"]["type"] == "search_request"
        assert attributes["filter"] == {"query": "@recall.sample:true", "from": "1000", "to": "2000"}
        assert attributes["page"] == {"limit": 50}
        assert attributes["sort"] == "timestamp"
        with_cursor: dict[str, Any] = build_spans_request_body(1000, 2000, 50, "abc")
        assert with_cursor["data"]["attributes"]["page"] == {"limit": 50, "cursor": "abc"}

    def test_flatten_nested_custom_attributes(self) -> None:
        """Nested custom recall objects flatten to dotted keys."""
        span: dict[str, Any] = {
            "attributes": {"custom": {"recall": {"sample_id": "abc", "k": "5"}, "recall.org_id": "acme"}}
        }
        flat: dict[str, Any] = flatten_recall_attributes(span)
        assert flat == {"recall.sample_id": "abc", "recall.k": "5", "recall.org_id": "acme"}

    def test_flatten_flat_attribute_keys(self) -> None:
        """Already-flat dotted keys at the attributes level pass through."""
        span: dict[str, Any] = {"attributes": {"recall.sample_id": "abc"}}
        assert flatten_recall_attributes(span) == {"recall.sample_id": "abc"}

    def test_flatten_empty_span(self) -> None:
        """A span without recall attributes flattens to an empty dictionary."""
        assert flatten_recall_attributes({}) == {}
        assert flatten_recall_attributes({"attributes": {"custom": {"other": 1}}}) == {}

    def test_datadog_source_url_and_missing_keys(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The endpoint follows the site and fetch refuses to run without credentials."""
        source: DatadogSpanSource = DatadogSpanSource(site="datadoghq.eu")
        assert source.search_url() == "https://api.datadoghq.eu/api/v2/spans/events/search"
        monkeypatch.delenv("DD_API_KEY", raising=False)
        monkeypatch.delenv("DD_APP_KEY", raising=False)
        with pytest.raises(ValueError, match="DD_API_KEY"):
            list(source.fetch(0, 1, 10))


class TestRecallCli:
    """The recall subcommand parses and exposes its flags."""

    def test_recall_help_exits_zero(self) -> None:
        """``lance-etl-tools recall --help`` exits with status 0."""
        with pytest.raises(SystemExit) as exc_info:
            tools_cli.main(["recall", "--help"])
        assert exc_info.value.code == 0

    def test_recall_args_parse_with_defaults(self) -> None:
        """Required flags parse and optional flags carry the documented defaults."""
        args = tools_cli.build_parser().parse_args(
            ["recall", "--from", "1700000000000", "--to", "1700000400000", "--base-uri", "s3://bucket/root"]
        )
        assert args.command == "recall"
        assert args.from_ts == "1700000000000"
        assert args.to_ts == "1700000400000"
        assert args.base_uri == "s3://bucket/root"
        assert args.dd_site == "datadoghq.com"
        assert args.max_samples == 10_000
        assert args.vector_column == "vector"

    def test_recall_requires_window_and_base_uri(self) -> None:
        """Missing required flags fail argument parsing."""
        with pytest.raises(SystemExit) as exc_info:
            tools_cli.build_parser().parse_args(["recall", "--base-uri", "s3://bucket/root"])
        assert exc_info.value.code == 2
