"""Unit tests for the pure ``bench.fuzz`` evidence helpers (no Spark or PostgreSQL required)."""

from __future__ import annotations

from typing import Any

from bench.fuzz import resurrection_evidence
from bench.fuzz_workload import OracleRow


def oracle_row(scenario: str, payload_version: int = 2, is_deleted: bool = False) -> OracleRow:
    """Build a minimal oracle row for a resurrection-evidence unit test.

    Args:
        scenario: Evidence label of the winning op.
        payload_version: Expected regenerated content version.
        is_deleted: Whether the row is a tombstone.

    Returns:
        An oracle row with an arbitrary timestamp and snapshot ordinal.
    """
    return OracleRow(
        ts_us=0, is_deleted=is_deleted, payload_version=payload_version, snapshot_ordinal=0, scenario=scenario
    )


class TestResurrectionEvidence:
    """The revive-only evidence extraction cross-references the actual published rows."""

    def test_ignores_keys_whose_terminal_op_is_not_a_revive(self) -> None:
        """Normal, duplicate, and redelete terminal states never appear in the evidence."""
        oracle_org: dict[str, OracleRow] = {
            "k1": oracle_row("normal"),
            "k2": oracle_row("duplicate"),
            "k3": oracle_row("redelete", is_deleted=True),
        }
        assert resurrection_evidence(oracle_org, {}) == []

    def test_reports_a_revived_key_found_live_in_actual(self) -> None:
        """A revived key present and live in ``actual`` reports ``published_live: True``."""
        oracle_org: dict[str, OracleRow] = {"k1": oracle_row("revive", payload_version=3)}
        actual: dict[str, dict[str, Any]] = {"k1": {"is_deleted": False}}
        evidence: list[dict[str, Any]] = resurrection_evidence(oracle_org, actual)
        assert evidence == [{"record_id": "k1", "expected_payload_version": 3, "published_live": True}]

    def test_reports_a_revived_key_missing_from_actual_as_not_live(self) -> None:
        """A revived key absent from ``actual`` reports ``published_live: False``, not an error."""
        oracle_org: dict[str, OracleRow] = {"k1": oracle_row("revive")}
        evidence: list[dict[str, Any]] = resurrection_evidence(oracle_org, {})
        assert evidence == [{"record_id": "k1", "expected_payload_version": 2, "published_live": False}]

    def test_reports_a_revived_key_still_tombstoned_in_actual_as_not_live(self) -> None:
        """A revived key present but still marked deleted in ``actual`` is not published live.

        This is exactly the shape a real product bug would take (the replay sink failing to flip
        ``is_deleted`` back on resurrection): the evidence surfaces it as ``published_live: False``
        even though the general mismatch comparison elsewhere would already fail the run.
        """
        oracle_org: dict[str, OracleRow] = {"k1": oracle_row("revive")}
        actual: dict[str, dict[str, Any]] = {"k1": {"is_deleted": True}}
        evidence: list[dict[str, Any]] = resurrection_evidence(oracle_org, actual)
        assert evidence == [{"record_id": "k1", "expected_payload_version": 2, "published_live": False}]

    def test_results_are_sorted_by_record_id(self) -> None:
        """Multiple revived keys are reported in deterministic sorted order."""
        oracle_org: dict[str, OracleRow] = {
            "k9": oracle_row("revive", payload_version=5),
            "k1": oracle_row("revive", payload_version=2),
        }
        actual: dict[str, dict[str, Any]] = {"k9": {"is_deleted": False}, "k1": {"is_deleted": False}}
        evidence: list[dict[str, Any]] = resurrection_evidence(oracle_org, actual)
        assert [entry["record_id"] for entry in evidence] == ["k1", "k9"]
