"""Unit tests for the fuzz workload generator's resurrection and double-delete coverage (PR-04).

These tests are pure Python (no Spark, gRPC, or PostgreSQL) and exercise the same
:mod:`bench.fuzz_workload` module the real ``python -m bench fuzz`` evaluator drives, at a scale
small enough to run in milliseconds.
"""

from __future__ import annotations

import random
from collections import Counter
from dataclasses import dataclass, field

from bench.fuzz_workload import (
    REDELETE_PROBABILITY,
    REVIVE_PROBABILITY,
    FuzzOp,
    FuzzSettings,
    FuzzWorkload,
    OracleRow,
    WorkloadBuilder,
    generate_workload,
    oracle_rows,
)

NOW_US: int = 1_700_000_000_000_000
"""Fixed generation instant reused across every test in this module."""


def small_settings(
    ops: int = 1,
    snapshots: int = 2,
    keyspace: int = 1,
    mix: tuple[int, int, int] = (1, 1, 1),
) -> FuzzSettings:
    """Build minimal validated fuzz settings for a builder-level unit test.

    Args:
        ops: Total randomized op budget.
        snapshots: Snapshot count.
        keyspace: Distinct record-id pool.
        mix: Insert, update, and delete relative weights.

    Returns:
        Validated fuzz settings with retention and conflict mode both disabled.
    """
    return FuzzSettings(
        seed=1,
        ops=ops,
        snapshots=snapshots,
        keyspace=keyspace,
        mix=mix,
        dup_probability=0.0,
        retention_mode="off",
        retention_seconds=None,
        conflict=False,
        dim=8,
        tenants=1,
        num_clusters=1,
    )


def empty_builder(settings: FuzzSettings) -> WorkloadBuilder:
    """Build a workload builder with no ops generated yet.

    Args:
        settings: Validated fuzz settings.

    Returns:
        A fresh builder over ``settings.snapshots`` empty snapshot buckets.
    """
    return WorkloadBuilder(
        settings=settings,
        now_us=NOW_US,
        rng=random.Random(0),
        per_snapshot=[[] for _ in range(settings.snapshots)],
        reserved=[set() for _ in range(settings.snapshots)],
    )


@dataclass
class ScriptedRng:
    """Deterministic stand-in for ``random.Random`` exposing only what the builder calls.

    Attributes:
        random_values: Successive ``random()`` return values; the last one repeats once exhausted.
        calls: Number of ``random()`` calls made so far.
    """

    random_values: list[float]
    calls: int = field(default=0)

    def random(self) -> float:
        """Return the next scripted draw.

        Returns:
            The scripted value for this call.
        """
        value: float = self.random_values[min(self.calls, len(self.random_values) - 1)]
        self.calls += 1
        return value

    def choice(self, candidates: list[str]) -> str:
        """Deterministically pick the first (lexicographically smallest) candidate.

        Args:
            candidates: Sorted eligible record ids.

        Returns:
            The first candidate.
        """
        return candidates[0]

    def randint(self, low: int, high: int) -> int:
        """Deterministically return the lower inclusive bound.

        Args:
            low: Inclusive lower bound.
            high: Inclusive upper bound (unused; the scripted draw is always the lower bound).

        Returns:
            ``low``.
        """
        del high
        return low

    def randrange(self, stop: int) -> int:
        """Deterministically return zero, the first eligible index.

        Args:
            stop: Exclusive upper bound (unused; the scripted draw is always zero).

        Returns:
            ``0``.
        """
        del stop
        return 0


class TestMaybeRevive:
    """The revive branch resurrects a dead key with a bumped content version."""

    def test_revives_a_dead_key_when_the_dice_hits(self) -> None:
        """A guaranteed-hit draw moves the key from dead to alive and bumps its version."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.dead.add("k000001")
        builder.key_org["k000001"] = "org0"
        builder.payload_version["k000001"] = 3
        builder.rng = ScriptedRng([0.0])
        op: FuzzOp | None = builder.maybe_revive(snapshot=1, used=set())
        assert op is not None
        assert op.scenario == "revive"
        assert op.record_id == "k000001"
        assert op.org_id == "org0"
        assert op.payload_version == 4
        assert not op.is_delete()
        assert "k000001" in builder.alive
        assert "k000001" not in builder.dead

    def test_returns_none_without_a_dice_hit(self) -> None:
        """A guaranteed-miss draw never revives, even with an eligible dead key."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.dead.add("k000001")
        builder.key_org["k000001"] = "org0"
        builder.payload_version["k000001"] = 1
        builder.rng = ScriptedRng([1.0])
        assert builder.maybe_revive(snapshot=1, used=set()) is None
        assert "k000001" in builder.dead

    def test_returns_none_without_any_dead_candidate(self) -> None:
        """A guaranteed hit still yields nothing when no key is currently dead."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.rng = ScriptedRng([0.0])
        assert builder.maybe_revive(snapshot=0, used=set()) is None

    def test_never_revives_an_absent_only_key(self) -> None:
        """A key that was only ever an absent-delete target is never in ``dead`` and never revives."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.key_org["absent000000"] = "org0"
        builder.rng = ScriptedRng([0.0])
        assert builder.maybe_revive(snapshot=0, used=set()) is None


class TestMaybeRedelete:
    """The redelete branch re-tombstones an already-dead key."""

    def test_redeletes_a_dead_key_when_the_dice_hits(self) -> None:
        """A guaranteed-hit draw emits a tombstone for a dead key and keeps it dead."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.dead.add("k000002")
        builder.key_org["k000002"] = "org0"
        builder.rng = ScriptedRng([0.0])
        op: FuzzOp | None = builder.maybe_redelete(snapshot=1, used=set())
        assert op is not None
        assert op.scenario == "redelete"
        assert op.record_id == "k000002"
        assert op.is_delete()
        assert "k000002" in builder.dead
        assert "k000002" not in builder.alive

    def test_returns_none_without_a_dice_hit(self) -> None:
        """A guaranteed-miss draw never redeletes, even with an eligible dead key."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.dead.add("k000002")
        builder.key_org["k000002"] = "org0"
        builder.rng = ScriptedRng([1.0])
        assert builder.maybe_redelete(snapshot=1, used=set()) is None

    def test_returns_none_without_any_dead_candidate(self) -> None:
        """A guaranteed hit still yields nothing when no key is currently dead."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.rng = ScriptedRng([0.0])
        assert builder.maybe_redelete(snapshot=0, used=set()) is None


class TestDoDeletePopulatesDead:
    """A normal live-key delete must make the key eligible for a later revive or redelete."""

    def test_normal_delete_moves_the_key_into_dead(self) -> None:
        """Deleting a live key with both the redelete and absent-delete draws missing.

        Exercises the ordinary path through ``do_delete``: the redelete gate misses (first
        ``random()`` call), the absent-delete gate misses (second call), and the live-key branch
        runs, discarding the key from ``alive`` and adding it to ``dead``.
        """
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.alive.add("k000003")
        builder.key_org["k000003"] = "org0"
        builder.payload_version["k000003"] = 1
        builder.rng = ScriptedRng([1.0, 1.0])
        op: FuzzOp | None = builder.do_delete(snapshot=0, used=set())
        assert op is not None
        assert op.scenario == "normal"
        assert op.record_id == "k000003"
        assert "k000003" not in builder.alive
        assert "k000003" in builder.dead

    def test_absent_delete_never_populates_dead(self) -> None:
        """An absent-key delete targets a key that was never alive, so ``dead`` stays untouched."""
        settings: FuzzSettings = small_settings()
        builder: WorkloadBuilder = empty_builder(settings)
        builder.rng = ScriptedRng([1.0, 0.0])
        op: FuzzOp | None = builder.do_delete(snapshot=0, used=set())
        assert op is not None
        assert op.scenario == "absent_delete"
        assert not builder.dead


class TestGenerateWorkloadProducesBothScenarios:
    """The real seeded generator, not just the builder unit, must emit both new scenarios."""

    def test_revive_and_redelete_appear_under_default_shaped_settings(self) -> None:
        """A moderately sized, delete-heavy run reliably accumulates and reuses dead keys.

        Two fixed seeds are checked so the coverage is not an artifact of one lucky seed, matching
        the real ``bench fuzz --seed`` invocations this generator backs.
        """
        for seed in (42, 7):
            settings: FuzzSettings = FuzzSettings(
                seed=seed,
                ops=400,
                snapshots=6,
                keyspace=150,
                mix=(45, 20, 35),
                dup_probability=0.1,
                retention_mode="off",
                retention_seconds=None,
                conflict=False,
                dim=8,
                tenants=2,
                num_clusters=4,
            )
            workload: FuzzWorkload = generate_workload(settings, NOW_US)
            counts: Counter[str] = Counter(op.scenario for op in workload.ops)
            assert counts["revive"] > 0, f"seed {seed} produced no revive ops: {counts}"
            assert counts["redelete"] > 0, f"seed {seed} produced no redelete ops: {counts}"


class TestOracleReflectsResurrectionAndReDeletion:
    """The oracle needs no code change for the new scenarios.

    ``oracle_rows`` keys strictly on terminal per-record state, so it already replays revive and
    redelete ops in generation order exactly like any other op.
    """

    def test_oracle_marks_a_revived_key_live_with_the_bumped_payload_version(self) -> None:
        """Insert, delete, then revive: the terminal oracle row must be live at the bumped version."""
        settings: FuzzSettings = small_settings(snapshots=3)
        ops: tuple[FuzzOp, ...] = (
            FuzzOp(0, "org0", "k000000", "insert", NOW_US, 1, "normal"),
            FuzzOp(1, "org0", "k000000", "delete", NOW_US, 1, "normal"),
            FuzzOp(2, "org0", "k000000", "upsert", NOW_US, 2, "revive"),
        )
        workload = FuzzWorkload(ops=ops, now_us=NOW_US, snapshots=3)
        state: dict[str, dict[str, OracleRow]] = oracle_rows(workload, settings, NOW_US, verified_through_snapshot=2)
        row: OracleRow = state["org0"]["k000000"]
        assert row.is_deleted is False
        assert row.payload_version == 2

    def test_oracle_marks_a_redeleted_key_as_deleted(self) -> None:
        """Insert, delete, revive, then redelete: the terminal oracle row must be a tombstone."""
        settings: FuzzSettings = small_settings(snapshots=4)
        ops: tuple[FuzzOp, ...] = (
            FuzzOp(0, "org0", "k000000", "insert", NOW_US, 1, "normal"),
            FuzzOp(1, "org0", "k000000", "delete", NOW_US, 1, "normal"),
            FuzzOp(2, "org0", "k000000", "upsert", NOW_US, 2, "revive"),
            FuzzOp(3, "org0", "k000000", "delete", NOW_US, 1, "redelete"),
        )
        workload = FuzzWorkload(ops=ops, now_us=NOW_US, snapshots=4)
        state: dict[str, dict[str, OracleRow]] = oracle_rows(workload, settings, NOW_US, verified_through_snapshot=3)
        row: OracleRow = state["org0"]["k000000"]
        assert row.is_deleted is True

    def test_revive_probability_constants_are_nontrivial(self) -> None:
        """The tunable probabilities are positive and bounded so both scenarios are reachable."""
        assert 0.0 < REVIVE_PROBABILITY <= 1.0
        assert 0.0 < REDELETE_PROBABILITY <= 1.0
