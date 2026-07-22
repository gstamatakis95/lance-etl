"""Unit tests for the bench reconciler helpers that need neither Spark nor PostgreSQL."""

from __future__ import annotations

from dataclasses import dataclass, field, replace

import pytest

import bench.reconcile as reconcile_module
from bench.config import BenchConfig
from bench.reconcile import batch_windows_by_ordinal, bench_spec_index_names, drain_reconciler
from lance_etl.reconciler.planning import EnqueueSummary
from lance_etl.reconciler.results import DispatchSummary, ReconcileSummary
from lance_etl.reconciler.service import RetentionDecision, RunOnceSummary, SloStatus
from lance_etl.state import ControlPlaneStatus


def bench_config(limit: int, batches: int) -> BenchConfig:
    """Build a minimal e2e configuration for the bigann adapter.

    Args:
        limit: Corpus row budget.
        batches: Requested append batch count.

    Returns:
        A configuration targeting the bigann adapter at the requested scale.
    """
    return replace(BenchConfig(command="e2e", dataset="bigann"), limit=limit, batches=batches)


def test_batch_windows_cover_the_corpus_without_gaps() -> None:
    """Consecutive windows tile the whole ordinal range with strictly positive width."""
    windows: list[tuple[int, int]] = batch_windows_by_ordinal(bench_config(1_200, 4))
    assert len(windows) == 4
    assert windows[0][0] == 0
    assert windows[-1][1] == 1_200
    for first, last in windows:
        assert last > first
    for index in range(len(windows) - 1):
        assert windows[index][1] == windows[index + 1][0]


def test_batches_above_limit_raises_readable_error() -> None:
    """More batches than rows would drive a repartition(0) and is rejected up front."""
    with pytest.raises(ValueError, match="exceeds the row budget"):
        batch_windows_by_ordinal(bench_config(4, 8))


def test_non_positive_batches_raise() -> None:
    """A non-positive batch count is rejected before any window arithmetic runs."""
    with pytest.raises(ValueError, match="must be positive"):
        batch_windows_by_ordinal(bench_config(1_200, 0))


def test_bench_spec_index_names_include_every_production_family() -> None:
    """The bench spec declares all six production indexes including the INVERTED full-text index."""
    names: frozenset[str] = bench_spec_index_names(bench_config(1_200, 2))
    assert names == frozenset(
        {
            "vector_idx",
            "text_fts_idx",
            "cluster_idx",
            "ts_idx",
            "ts_zonemap_idx",
            "is_deleted_bitmap_idx",
        }
    )


def quiescent_control_plane_status(retry_wait_work: int = 0, due_work: int = 0) -> ControlPlaneStatus:
    """Build a minimal control-plane status carrying only the fields ``drain_reconciler`` reads.

    Args:
        retry_wait_work: Count of ``RETRY_WAIT`` work rows.
        due_work: Count of claimable work rows.

    Returns:
        A status with every other field zeroed or ``None``.
    """
    return ControlPlaneStatus(
        pending_work=0,
        running_work=0,
        retry_wait_work=retry_wait_work,
        blocked_work=0,
        due_work=due_work,
        blocked_source_snapshots=0,
        oldest_open_work_at=None,
        retention_source_snapshot_seq=None,
        retention_snapshot_id=None,
        retention_parent_snapshot_id=None,
        retention_state=None,
        retention_created_at=None,
    )


def zero_claim_run_once_summary() -> RunOnceSummary:
    """Build a ``RunOnceSummary`` for a cycle that enqueued and claimed nothing.

    Returns:
        A summary with every dispatch counter at zero.
    """
    return RunOnceSummary(
        planning=EnqueueSummary(
            pinned_head_snapshot_id=None,
            planned_snapshots=0,
            enqueued_snapshots=0,
            source_snapshot_sequences=(),
            truncated=False,
        ),
        dispatch=DispatchSummary(claimed=0, succeeded=0, advanced=0, retried=0, blocked=0, stale=0),
        reconciliation=ReconcileSummary(inspected=0, reconciled=0, deferred=0),
        retention=RetentionDecision(
            retention_held=False, source_snapshot_seq=None, retain_snapshot_id=None, state=None
        ),
        slo=SloStatus(
            healthy=True,
            reasons=(),
            due_work=0,
            blocked_work=0,
            blocked_source_snapshots=0,
            oldest_open_age_seconds=0.0,
            retention_age_seconds=0.0,
        ),
    )


@dataclass
class StubStatusRepository:
    """Fake repository serving a scripted sequence of control-plane statuses.

    Attributes:
        statuses: Statuses returned in order; the last one repeats once exhausted.
        calls: Number of times ``control_plane_status`` has been invoked.
    """

    statuses: list[ControlPlaneStatus]
    calls: int = 0

    def control_plane_status(self) -> ControlPlaneStatus:
        """Return the next scripted status.

        Returns:
            The scripted status for this call.
        """
        status: ControlPlaneStatus = self.statuses[min(self.calls, len(self.statuses) - 1)]
        self.calls += 1
        return status


@dataclass
class StubDrainApplication:
    """Fake reconciler application serving a scripted sequence of zero-claim cycles.

    Attributes:
        repository: The stub status repository consulted once a cycle is quiescent-looking.
        summary: Cycle summary returned by every ``run_once`` invocation.
        cycles: Number of times ``run_once`` has been invoked.
    """

    repository: StubStatusRepository
    summary: RunOnceSummary = field(default_factory=zero_claim_run_once_summary)
    cycles: int = field(default=0)

    def run_once(self) -> RunOnceSummary:
        """Return a zero-claim, zero-enqueue cycle summary.

        Returns:
            The configured cycle summary.
        """
        self.cycles += 1
        return self.summary


def noop_sleep(seconds: float) -> None:
    """Discard one drain retry-poll delay so the test suite never sleeps in real time.

    Args:
        seconds: The delay the drain would otherwise have waited.
    """
    del seconds


def test_drain_reconciler_keeps_cycling_while_retry_wait_work_is_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """A zero-claim cycle with pending ``RETRY_WAIT`` work must not be declared quiescent.

    The first two status reads report a pending retry (and, on the second read, a due row), so the
    drain must keep cycling instead of returning early with undercounted totals. Only once both
    counters read zero does the drain return.
    """
    monkeypatch.setattr(reconcile_module.time, "sleep", noop_sleep)
    application = StubDrainApplication(
        repository=StubStatusRepository(
            statuses=[
                quiescent_control_plane_status(retry_wait_work=1, due_work=0),
                quiescent_control_plane_status(retry_wait_work=0, due_work=1),
                quiescent_control_plane_status(retry_wait_work=0, due_work=0),
            ]
        )
    )
    totals: dict[str, int] = drain_reconciler(application)
    assert totals["cycles"] == 3
    assert application.repository.calls == 3


def test_drain_reconciler_raises_when_retries_never_resolve(monkeypatch: pytest.MonkeyPatch) -> None:
    """A queue that never drains its retry-wait work still raises within the bounded cycle count."""
    monkeypatch.setattr(reconcile_module.time, "sleep", noop_sleep)
    monkeypatch.setattr(reconcile_module, "MAX_DRAIN_CYCLES", 3)
    application = StubDrainApplication(
        repository=StubStatusRepository(statuses=[quiescent_control_plane_status(retry_wait_work=1, due_work=0)])
    )
    with pytest.raises(RuntimeError, match="did not reach quiescence"):
        drain_reconciler(application)
    assert application.repository.calls == 3


def test_drain_reconciler_raises_for_durably_blocked_work_before_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Existing durable blockage fails fast even when the dispatch blocked no new work."""
    sleep_calls: list[float] = []
    monkeypatch.setattr(reconcile_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    summary: RunOnceSummary = zero_claim_run_once_summary()
    summary = replace(
        summary,
        slo=replace(
            summary.slo,
            healthy=False,
            reasons=("blocked_work",),
            due_work=1,
            blocked_work=1,
        ),
    )
    application = StubDrainApplication(
        repository=StubStatusRepository(statuses=[quiescent_control_plane_status(due_work=1)]),
        summary=summary,
    )
    with pytest.raises(RuntimeError, match="blocked benchmark work"):
        drain_reconciler(application)
    assert application.cycles == 1
    assert application.repository.calls == 0
    assert sleep_calls == []


def test_drain_reconciler_returns_immediately_when_already_quiescent(monkeypatch: pytest.MonkeyPatch) -> None:
    """A zero-claim cycle with no pending retry or due work returns without sleeping."""
    sleep_calls: list[float] = []
    monkeypatch.setattr(reconcile_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    application = StubDrainApplication(repository=StubStatusRepository(statuses=[quiescent_control_plane_status()]))
    totals: dict[str, int] = drain_reconciler(application)
    assert totals["cycles"] == 1
    assert application.repository.calls == 1
    assert sleep_calls == []
