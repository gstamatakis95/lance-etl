"""Pure, Spark-free randomized CRUD fuzz workload model, generator, payload, and oracle.

This module owns everything about a fuzz run that is regenerable from ``(seed, knobs, now_us)``
alone. It carries no Spark, gRPC, or PostgreSQL dependency so it can be exercised directly and so
the driver only ever holds op specifications, never payloads. Payload bytes are produced by
:func:`fuzz_payload` inside Spark executors during ingest and regenerated bit-for-bit by the
verifier from the same identity tuple.

The op vocabulary is drawn from the full source spelling set so the reconciler's
``normalize_operation`` is exercised: ``insert`` / ``update`` / ``upsert`` / ``i`` / ``u`` all
normalize to an upsert, and ``delete`` / ``d`` normalize to a tombstone. Last-write-wins is driven
strictly by the Iceberg source sequence, never by the event ``ts``, so generated timestamps are
deliberately uncorrelated with delivery order. Retention band populations are planted with reserved
key prefixes and never mutated again so record expiry never interacts with resurrection.

Two scenarios beyond plain insert/update/delete exercise the replay sink's tombstone-transition
paths: ``revive`` resurrects a previously tombstoned key with a bumped content version (an upsert
over an existing tombstone at a higher source sequence must flip ``is_deleted`` back to false and
replace the payload), and ``redelete`` lands a second tombstone over an already-dead key (a delete
over an existing tombstone). Both draw only from keys a normal, non-absent delete actually
tombstoned, and both use the same recent timestamp band as every other non-planted op so retention
mode never expires them mid-run.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field

import numpy as np

from bench.config import BenchConfig
from lance_etl.etl.mutation import DELETE_OPERATIONS, UPSERT_OPERATIONS, normalize_operation

ABSENT_DELETE_PROBABILITY: float = 0.10
"""Chance a generated delete targets a never-born key, landing an absent-key tombstone."""

REVIVE_PROBABILITY: float = 0.30
"""Chance an insert attempt resurrects a previously tombstoned key instead of minting a fresh one."""

REDELETE_PROBABILITY: float = 0.25
"""Chance a delete attempt re-tombstones an already-dead key instead of a live one."""

MAX_REPORTED_MISMATCHES: int = 25
"""Upper bound on per-org content mismatches embedded verbatim in the evidence document."""

FRESH_BAND_SECONDS: int = 300
"""Width of the recent timestamp band assigned to normal ops, well inside any retention window."""

REPLAY_HORIZON_SECONDS: int = 2_592_000
"""Source replay horizon (30 days) that the tombstone retention clock never expires within."""

ANCIENT_TOMBSTONE_SECONDS: int = 40 * 86_400
"""Age of the ancient-tombstone band (40 days), safely past the 30-day replay horizon."""

XL_LIVE_MULTIPLIER: int = 10
"""Expired-live band age as a multiple of the retention window (10x, ~9h past at 3600s)."""

ST_TOMBSTONE_MULTIPLIER: int = 5
"""Stale-tombstone band age as a multiple of the retention window (5x), past retention yet young."""

RETENTION_PREFIXES: tuple[str, str, str] = ("xl-", "st-", "at-")
"""Reserved record-id prefixes for the expired-live, stale-tombstone, and ancient-tombstone bands."""


@dataclass(frozen=True, slots=True)
class FuzzSettings:
    """Validated fuzz tunables resolved once from the benchmark configuration.

    Attributes:
        seed: Master seed making the entire op sequence and every payload deterministic.
        ops: Total randomized ops distributed across the snapshots.
        snapshots: Iceberg append snapshots, the first of which seeds every org.
        keyspace: Distinct randomized record-id pool shared across orgs.
        mix: Insert, update, and delete relative weights.
        dup_probability: Chance an accepted upsert is redelivered verbatim in a later snapshot.
        retention_mode: Either ``off`` or ``short`` (enables retention and the three ts bands).
        retention_seconds: Retention window in short mode, otherwise ``None``.
        conflict: When true the final snapshot injects a same-snapshot distinct-mutation pair.
        dim: Synthetic vector dimension, divisible by eight.
        tenants: Round-robin organization count.
        num_clusters: Deterministic cluster-bucket cardinality for the metadata payload.
    """

    seed: int
    ops: int
    snapshots: int
    keyspace: int
    mix: tuple[int, int, int]
    dup_probability: float
    retention_mode: str
    retention_seconds: int | None
    conflict: bool
    dim: int
    tenants: int
    num_clusters: int

    @classmethod
    def from_config(cls, config: BenchConfig) -> FuzzSettings:
        """Resolve and deeply validate fuzz settings from a benchmark configuration.

        Args:
            config: Benchmark configuration carrying the ``fuzz_*`` knobs.

        Returns:
            A validated settings instance.

        Raises:
            ValueError: If any knob is out of range or the retention window is too small to keep
                fresh live rows comfortably inside it.
        """
        if config.fuzz_dim < 8 or config.fuzz_dim % 8 != 0:
            raise ValueError(f"--fuzz-dim must be positive and divisible by 8, got {config.fuzz_dim}")
        mix: tuple[int, int, int] = parse_mix(config.fuzz_mix)
        if config.fuzz_snapshots < 2:
            raise ValueError(f"--fuzz-snapshots must be at least 2, got {config.fuzz_snapshots}")
        if config.fuzz_ops < config.fuzz_snapshots:
            raise ValueError(
                f"--fuzz-ops ({config.fuzz_ops}) must be at least --fuzz-snapshots "
                f"({config.fuzz_snapshots}) so every snapshot carries at least one op"
            )
        if config.tenants < 1:
            raise ValueError(f"--tenants must be at least 1, got {config.tenants}")
        if config.fuzz_keyspace < config.tenants:
            raise ValueError(
                f"--fuzz-keyspace ({config.fuzz_keyspace}) must be at least --tenants "
                f"({config.tenants}) so every org receives a baseline key"
            )
        if config.fuzz_retention_mode not in ("off", "short"):
            raise ValueError(f"--fuzz-retention-mode must be off or short, got {config.fuzz_retention_mode!r}")
        if not 0.0 <= config.fuzz_dup_probability <= 1.0:
            raise ValueError(f"--fuzz-dup-probability must be in [0, 1], got {config.fuzz_dup_probability}")
        retention_seconds: int | None = None
        if config.fuzz_retention_mode == "short":
            retention_seconds = config.fuzz_retention_seconds
            if retention_seconds <= FRESH_BAND_SECONDS * 2:
                raise ValueError(
                    f"--fuzz-retention-seconds ({retention_seconds}) must comfortably exceed the "
                    f"{FRESH_BAND_SECONDS}s fresh band so live rows are not expired mid-run; use at least "
                    f"{FRESH_BAND_SECONDS * 2 + 1}"
                )
        return cls(
            seed=config.seed,
            ops=config.fuzz_ops,
            snapshots=config.fuzz_snapshots,
            keyspace=config.fuzz_keyspace,
            mix=mix,
            dup_probability=config.fuzz_dup_probability,
            retention_mode=config.fuzz_retention_mode,
            retention_seconds=retention_seconds,
            conflict=config.fuzz_conflict,
            dim=config.fuzz_dim,
            tenants=config.tenants,
            num_clusters=config.num_clusters,
        )

    def org_ids(self) -> tuple[str, ...]:
        """Return the org identifiers in tenant order.

        Returns:
            One ``orgN`` identifier per tenant.
        """
        return tuple(f"org{index}" for index in range(self.tenants))


@dataclass(frozen=True, slots=True)
class FuzzOp:
    """One generated source mutation targeting a single record in a single snapshot.

    Attributes:
        snapshot: Zero-based snapshot ordinal the op is delivered in.
        org_id: Owning organization identifier.
        record_id: Logical merge key.
        op: Source operation spelling drawn from the full vocabulary.
        ts_us: Event timestamp in epoch microseconds, uncorrelated with delivery order.
        payload_version: Content generation seed component; unchanged across a verbatim redelivery.
        scenario: Evidence label for the op (``normal`` / ``duplicate`` / ``absent_delete`` /
            ``xl`` / ``st`` / ``at`` / ``conflict_update`` / ``conflict_delete``).
    """

    snapshot: int
    org_id: str
    record_id: str
    op: str
    ts_us: int
    payload_version: int
    scenario: str

    def is_delete(self) -> bool:
        """Return whether this op normalizes to a tombstone.

        Returns:
            True when the op spelling normalizes to a delete.
        """
        return normalize_operation(self.op) == "delete"


@dataclass(frozen=True, slots=True)
class FuzzWorkload:
    """The complete driver-side op program for one fuzz run, without any payloads.

    Attributes:
        ops: Every op in snapshot-major delivery order.
        now_us: Generation wall-clock instant in epoch microseconds anchoring the ts bands.
        snapshots: Snapshot count the program spans.
    """

    ops: tuple[FuzzOp, ...]
    now_us: int
    snapshots: int

    def ops_for_snapshot(self, snapshot: int) -> tuple[FuzzOp, ...]:
        """Return the ops delivered in one snapshot, in insertion order.

        Args:
            snapshot: Zero-based snapshot ordinal.

        Returns:
            The ops for that snapshot.
        """
        return tuple(op for op in self.ops if op.snapshot == snapshot)


@dataclass(frozen=True, slots=True)
class OracleRow:
    """Expected terminal state of one record after replaying the workload.

    Attributes:
        ts_us: Expected stored event timestamp in microseconds.
        is_deleted: Whether the record is a tombstone.
        payload_version: Content version regenerated for live-row content comparison.
        snapshot_ordinal: Snapshot whose Iceberg sequence the stored source sequence must equal.
        scenario: Evidence label of the winning (terminal) op for this key, letting a caller
            identify, for example, a key whose terminal state was produced by a ``revive``.
    """

    ts_us: int
    is_deleted: bool
    payload_version: int
    snapshot_ordinal: int
    scenario: str


def parse_mix(raw: str) -> tuple[int, int, int]:
    """Parse an ``insert:update:delete`` mix specification into three positive weights.

    Args:
        raw: The colon-separated weight string.

    Returns:
        The three parsed weights.

    Raises:
        ValueError: If the string is malformed or any weight is not a positive integer.
    """
    parts: list[str] = raw.split(":")
    if len(parts) != 3:
        raise ValueError(f"--fuzz-mix must be i:u:d, got {raw!r}")
    try:
        weights: tuple[int, int, int] = (int(parts[0]), int(parts[1]), int(parts[2]))
    except ValueError as exc:
        raise ValueError(f"--fuzz-mix weights must be integers, got {raw!r}") from exc
    if any(weight <= 0 for weight in weights):
        raise ValueError(f"--fuzz-mix weights must be positive, got {raw!r}")
    return weights


def retention_band_keys(org: str) -> dict[str, str]:
    """Return the reserved retention-band record ids for one org.

    Args:
        org: Organization identifier.

    Returns:
        A mapping of band name (``xl`` / ``st`` / ``at``) to its reserved record id.
    """
    return {"xl": f"xl-{org}", "st": f"st-{org}", "at": f"at-{org}"}


@dataclass(slots=True)
class WorkloadBuilder:
    """Mutable state threaded through deterministic workload generation.

    Attributes:
        settings: Validated fuzz settings.
        now_us: Generation wall-clock instant in epoch microseconds.
        rng: Seeded pseudo-random generator driving every choice.
        per_snapshot: Accumulated ops keyed by snapshot ordinal.
        reserved: Keys no random op may touch in a given snapshot.
        alive: Currently live record ids.
        dead: Previously live record ids tombstoned by a normal (non-absent) delete, eligible for
            a later resurrection or re-delete. Absent-key deletes never populate this set: a key
            that was never alive cannot be resurrected.
        key_org: Owning org for every allocated record id.
        payload_version: Latest content version for every record id.
        next_ordinal: Next unborn key ordinal.
        insert_count: Inserts issued so far, driving org round-robin.
        absent_count: Absent-key deletes issued so far.
    """

    settings: FuzzSettings
    now_us: int
    rng: random.Random
    per_snapshot: list[list[FuzzOp]]
    reserved: list[set[str]]
    alive: set[str] = field(default_factory=set)
    dead: set[str] = field(default_factory=set)
    key_org: dict[str, str] = field(default_factory=dict)
    payload_version: dict[str, int] = field(default_factory=dict)
    next_ordinal: int = 0
    insert_count: int = 0
    absent_count: int = 0

    def fresh_ts(self) -> int:
        """Return a timestamp in the recent band, uncorrelated with delivery order.

        Returns:
            An epoch-microsecond timestamp inside the fresh band.
        """
        return self.now_us - self.rng.randint(0, FRESH_BAND_SECONDS) * 1_000_000

    def upsert_spelling(self) -> str:
        """Return a deterministically chosen upsert operation spelling.

        Returns:
            One spelling from the upsert vocabulary.
        """
        return self.rng.choice(sorted(UPSERT_OPERATIONS))

    def delete_spelling(self) -> str:
        """Return a deterministically chosen delete operation spelling.

        Returns:
            One spelling from the delete vocabulary.
        """
        return self.rng.choice(sorted(DELETE_OPERATIONS))

    def emit(self, op: FuzzOp) -> None:
        """Record one op in its snapshot bucket.

        Args:
            op: The op to append.
        """
        self.per_snapshot[op.snapshot].append(op)

    def seed_orgs(self) -> None:
        """Insert one baseline key per org into the first snapshot.

        Guarantees every org materializes a dataset and gives conflict mode a live org0 key.
        """
        for org in self.settings.org_ids():
            record_id: str = self.allocate_key(org)
            self.alive.add(record_id)
            self.payload_version[record_id] = 1
            self.emit(FuzzOp(0, org, record_id, self.upsert_spelling(), self.fresh_ts(), 1, "normal"))

    def allocate_key(self, org: str) -> str:
        """Allocate the next unborn key to an org.

        Args:
            org: Owning organization.

        Returns:
            The freshly allocated record id.
        """
        record_id: str = f"k{self.next_ordinal:06d}"
        self.next_ordinal += 1
        self.insert_count += 1
        self.key_org[record_id] = org
        return record_id

    def plant_retention_bands(self) -> None:
        """Plant the expired-live, stale-tombstone, and ancient-tombstone populations per org.

        The bands use reserved key prefixes and are never mutated again, so their expiry never
        interacts with last-write-wins resurrection. Ages are chosen far from every retention
        boundary so wall-clock drift between generation and publication cannot flip an assertion.
        """
        retention: int = int(self.settings.retention_seconds or 0)
        for org in self.settings.org_ids():
            keys: dict[str, str] = retention_band_keys(org)
            xl_ts: int = self.now_us - retention * XL_LIVE_MULTIPLIER * 1_000_000
            self.payload_version[keys["xl"]] = 1
            self.emit(FuzzOp(0, org, keys["xl"], self.upsert_spelling(), xl_ts, 1, "xl"))
            self.payload_version[keys["st"]] = 1
            self.emit(FuzzOp(0, org, keys["st"], self.upsert_spelling(), self.fresh_ts(), 1, "st"))
            st_ts: int = self.now_us - retention * ST_TOMBSTONE_MULTIPLIER * 1_000_000
            self.emit(FuzzOp(1, org, keys["st"], self.delete_spelling(), st_ts, 1, "st"))
            at_ts: int = self.now_us - ANCIENT_TOMBSTONE_SECONDS * 1_000_000
            self.emit(FuzzOp(0, org, keys["at"], self.delete_spelling(), at_ts, 1, "at"))
            for snapshot in range(self.settings.snapshots):
                self.reserved[snapshot].update(keys.values())

    def reserve_conflict_key(self) -> str | None:
        """Reserve org0's baseline key against every snapshot after the first.

        Returns:
            The reserved conflict key, or ``None`` when conflict mode is disabled.
        """
        if not self.settings.conflict:
            return None
        candidates: list[str] = sorted(key for key in self.alive if self.key_org[key] == "org0")
        conflict_key: str = candidates[0]
        for snapshot in range(1, self.settings.snapshots):
            self.reserved[snapshot].add(conflict_key)
        return conflict_key

    def inject_conflict(self, conflict_key: str) -> None:
        """Inject a distinct update and delete for one org0 key into the final snapshot.

        Args:
            conflict_key: The reserved live org0 key that receives both mutations.
        """
        final: int = self.settings.snapshots - 1
        version: int = self.payload_version[conflict_key] + 1
        self.emit(
            FuzzOp(final, "org0", conflict_key, self.upsert_spelling(), self.fresh_ts(), version, "conflict_update")
        )
        self.emit(
            FuzzOp(final, "org0", conflict_key, self.delete_spelling(), self.fresh_ts(), version, "conflict_delete")
        )

    def choose_type(self) -> str:
        """Choose an op type by the configured mix weights.

        Returns:
            One of ``insert``, ``update``, or ``delete``.
        """
        insert_weight: int
        update_weight: int
        delete_weight: int
        insert_weight, update_weight, delete_weight = self.settings.mix
        draw: float = self.rng.random() * (insert_weight + update_weight + delete_weight)
        if draw < insert_weight:
            return "insert"
        if draw < insert_weight + update_weight:
            return "update"
        return "delete"

    def available(self, snapshot: int, used: set[str]) -> list[str]:
        """Return live keys eligible for mutation in a snapshot, sorted for determinism.

        Args:
            snapshot: The snapshot being generated.
            used: Keys already touched in this snapshot.

        Returns:
            Sorted eligible live record ids.
        """
        return sorted(self.alive - used - self.reserved[snapshot])

    def available_dead(self, snapshot: int, used: set[str]) -> list[str]:
        """Return tombstoned keys eligible for a revival or a re-delete in a snapshot.

        Args:
            snapshot: The snapshot being generated.
            used: Keys already touched in this snapshot.

        Returns:
            Sorted eligible dead record ids.
        """
        return sorted(self.dead - used - self.reserved[snapshot])

    def maybe_revive(self, snapshot: int, used: set[str]) -> FuzzOp | None:
        """Occasionally resurrect a previously tombstoned key with a bumped content version.

        Exercises the ``etl/replay_sink.py`` ``when_matched_update_all`` watermark path: an upsert
        for a tombstoned key at a higher source sequence must flip ``is_deleted`` back to false and
        replace the payload. Draws only from :attr:`WorkloadBuilder.dead`, never from absent-delete
        keys, which were never alive and so cannot be resurrected.

        Args:
            snapshot: Target snapshot.
            used: Keys already touched in this snapshot.

        Returns:
            The emitted ``revive`` op, or ``None`` when the dice missed or no dead key is eligible.
        """
        if self.rng.random() >= REVIVE_PROBABILITY:
            return None
        candidates: list[str] = self.available_dead(snapshot, used)
        if not candidates:
            return None
        record_id: str = self.rng.choice(candidates)
        self.dead.discard(record_id)
        self.alive.add(record_id)
        self.payload_version[record_id] += 1
        version: int = self.payload_version[record_id]
        op: FuzzOp = FuzzOp(
            snapshot, self.key_org[record_id], record_id, self.upsert_spelling(), self.fresh_ts(), version, "revive"
        )
        used.add(record_id)
        return op

    def do_insert(self, snapshot: int, used: set[str]) -> FuzzOp | None:
        """Emit a revive of a dead key, or an insert of a fresh key when not exhausted.

        A revive (see :meth:`maybe_revive`) is attempted first and, when drawn, is returned
        regardless of remaining keyspace budget since it reuses an already-allocated key rather
        than consuming a fresh one.

        Args:
            snapshot: Target snapshot.
            used: Keys already touched in this snapshot.

        Returns:
            The emitted op, or ``None`` when no revive was drawn and no unborn key remains.
        """
        revive_op: FuzzOp | None = self.maybe_revive(snapshot, used)
        if revive_op is not None:
            return revive_op
        if self.next_ordinal >= self.settings.keyspace:
            return None
        org: str = self.settings.org_ids()[self.insert_count % self.settings.tenants]
        record_id: str = self.allocate_key(org)
        self.alive.add(record_id)
        self.payload_version[record_id] = 1
        op: FuzzOp = FuzzOp(snapshot, org, record_id, self.upsert_spelling(), self.fresh_ts(), 1, "normal")
        used.add(record_id)
        return op

    def do_update(self, snapshot: int, used: set[str]) -> FuzzOp | None:
        """Emit an update of an eligible live key.

        Args:
            snapshot: Target snapshot.
            used: Keys already touched in this snapshot.

        Returns:
            The emitted op, or ``None`` when no eligible live key exists.
        """
        candidates: list[str] = self.available(snapshot, used)
        if not candidates:
            return None
        record_id: str = self.rng.choice(candidates)
        self.payload_version[record_id] += 1
        version: int = self.payload_version[record_id]
        op: FuzzOp = FuzzOp(
            snapshot, self.key_org[record_id], record_id, self.upsert_spelling(), self.fresh_ts(), version, "normal"
        )
        used.add(record_id)
        return op

    def maybe_redelete(self, snapshot: int, used: set[str]) -> FuzzOp | None:
        """Occasionally re-tombstone an already-dead key instead of a live one.

        Exercises a second tombstone landing over an existing tombstone (the same source-sequence
        watermark path a revive uses, just staying deleted). Draws only from
        :attr:`WorkloadBuilder.dead`, distinct from the absent-key delete branch which targets a
        key that was never alive.

        Args:
            snapshot: Target snapshot.
            used: Keys already touched in this snapshot.

        Returns:
            The emitted ``redelete`` op, or ``None`` when the dice missed or no dead key is
            eligible.
        """
        if self.rng.random() >= REDELETE_PROBABILITY:
            return None
        candidates: list[str] = self.available_dead(snapshot, used)
        if not candidates:
            return None
        record_id: str = self.rng.choice(candidates)
        op: FuzzOp = FuzzOp(
            snapshot, self.key_org[record_id], record_id, self.delete_spelling(), self.fresh_ts(), 1, "redelete"
        )
        used.add(record_id)
        return op

    def do_delete(self, snapshot: int, used: set[str]) -> FuzzOp | None:
        """Emit a re-delete of a dead key, a delete of a live key, or an absent-key tombstone.

        A re-delete (see :meth:`maybe_redelete`) is attempted first. Failing that, the existing
        absent-key and live-key delete branches run unchanged.

        Args:
            snapshot: Target snapshot.
            used: Keys already touched in this snapshot.

        Returns:
            The emitted op, or ``None`` when no re-delete was drawn, no live key exists, and an
            absent delete was not drawn either.
        """
        redelete_op: FuzzOp | None = self.maybe_redelete(snapshot, used)
        if redelete_op is not None:
            return redelete_op
        if self.rng.random() < ABSENT_DELETE_PROBABILITY:
            org: str = self.settings.org_ids()[self.rng.randrange(self.settings.tenants)]
            record_id: str = f"absent{self.absent_count:06d}"
            self.absent_count += 1
            self.key_org[record_id] = org
            op: FuzzOp = FuzzOp(snapshot, org, record_id, self.delete_spelling(), self.fresh_ts(), 1, "absent_delete")
            used.add(record_id)
            return op
        candidates: list[str] = self.available(snapshot, used)
        if not candidates:
            return None
        alive_key: str = self.rng.choice(candidates)
        self.alive.discard(alive_key)
        self.dead.add(alive_key)
        delete_op: FuzzOp = FuzzOp(
            snapshot, self.key_org[alive_key], alive_key, self.delete_spelling(), self.fresh_ts(), 1, "normal"
        )
        used.add(alive_key)
        return delete_op

    def maybe_duplicate(self, op: FuzzOp) -> None:
        """Schedule a verbatim redelivery of an accepted upsert into a later snapshot.

        The key is reserved through the duplicate snapshot so no intervening mutation changes its
        content, keeping the redelivery byte-identical while advancing its stored source sequence.

        Args:
            op: The accepted op eligible for redelivery.
        """
        if op.scenario != "normal" or op.is_delete():
            return
        if op.snapshot >= self.settings.snapshots - 1:
            return
        if self.rng.random() >= self.settings.dup_probability:
            return
        target: int = self.rng.randint(op.snapshot + 1, self.settings.snapshots - 1)
        for snapshot in range(op.snapshot + 1, target + 1):
            self.reserved[snapshot].add(op.record_id)
        self.emit(FuzzOp(target, op.org_id, op.record_id, op.op, op.ts_us, op.payload_version, "duplicate"))

    def fill_snapshot(self, snapshot: int, count: int) -> None:
        """Generate up to ``count`` random ops for one snapshot.

        Args:
            snapshot: Target snapshot ordinal.
            count: Number of random ops to attempt.
        """
        used: set[str] = {op.record_id for op in self.per_snapshot[snapshot]}
        for _ in range(count):
            op: FuzzOp | None = self.attempt(snapshot, used)
            if op is None:
                continue
            self.emit(op)
            self.maybe_duplicate(op)

    def attempt(self, snapshot: int, used: set[str]) -> FuzzOp | None:
        """Attempt one op of the chosen type with a deterministic feasibility fallback.

        Args:
            snapshot: Target snapshot.
            used: Keys already touched in this snapshot.

        Returns:
            An emitted op, or ``None`` when no op of any type is currently feasible.
        """
        order: dict[str, tuple[str, str, str]] = {
            "insert": ("insert", "update", "delete"),
            "update": ("update", "insert", "delete"),
            "delete": ("delete", "update", "insert"),
        }
        runners = {"insert": self.do_insert, "update": self.do_update, "delete": self.do_delete}
        for kind in order[self.choose_type()]:
            op: FuzzOp | None = runners[kind](snapshot, used)
            if op is not None:
                return op
        return None


def snapshot_op_counts(snapshots: int, ops: int) -> list[int]:
    """Split a random-op budget across snapshots as evenly as possible.

    Args:
        snapshots: Snapshot count.
        ops: Total random ops.

    Returns:
        Per-snapshot random-op counts summing to ``ops``.
    """
    base: int = ops // snapshots
    remainder: int = ops % snapshots
    return [base + (1 if index < remainder else 0) for index in range(snapshots)]


def generate_workload(settings: FuzzSettings, now_us: int) -> FuzzWorkload:
    """Generate the complete deterministic op program for one fuzz run.

    Args:
        settings: Validated fuzz settings.
        now_us: Generation wall-clock instant in epoch microseconds.

    Returns:
        The ordered op program spanning every snapshot.
    """
    builder: WorkloadBuilder = WorkloadBuilder(
        settings=settings,
        now_us=now_us,
        rng=random.Random(settings.seed),
        per_snapshot=[[] for _ in range(settings.snapshots)],
        reserved=[set() for _ in range(settings.snapshots)],
    )
    if settings.retention_mode == "short":
        builder.plant_retention_bands()
    builder.seed_orgs()
    conflict_key: str | None = builder.reserve_conflict_key()
    counts: list[int] = snapshot_op_counts(settings.snapshots, settings.ops)
    for snapshot in range(settings.snapshots):
        builder.fill_snapshot(snapshot, counts[snapshot])
    if conflict_key is not None:
        builder.inject_conflict(conflict_key)
    ordered: list[FuzzOp] = [op for snapshot in builder.per_snapshot for op in snapshot]
    return FuzzWorkload(ops=tuple(ordered), now_us=now_us, snapshots=settings.snapshots)


def fuzz_payload(
    seed: int,
    org_id: str,
    record_id: str,
    payload_version: int,
    dim: int,
    num_clusters: int,
) -> tuple[np.ndarray, str, str]:
    """Generate the deterministic vector, text, and cluster payload for one record version.

    The identity tuple is hashed into a numpy PCG64 seed so the driver-side verifier reproduces the
    exact float32 vector an executor produced during ingest. Text is non-empty so the inverted
    index stays honest.

    Args:
        seed: Master run seed.
        org_id: Owning organization identifier.
        record_id: Logical merge key.
        payload_version: Content version component.
        dim: Vector dimension.
        num_clusters: Cluster-bucket cardinality.

    Returns:
        The float32 vector, the document text, and the cluster bucket string.
    """
    identity: str = f"{seed}:{org_id}:{record_id}:{payload_version}:{dim}:{num_clusters}"
    digest: bytes = hashlib.sha256(identity.encode("utf-8")).digest()
    seed_int: int = int.from_bytes(digest[:8], "big")
    generator: np.random.Generator = np.random.Generator(np.random.PCG64(seed_int))
    vector: np.ndarray = generator.random(dim, dtype=np.float32)
    cluster_id: int = seed_int % num_clusters
    cluster: str = str(cluster_id)
    text: str = f"cluster {cluster_id} record {record_id} version {payload_version} alpha beta gamma"
    return vector, text, cluster


def replay_state(workload: FuzzWorkload, verified_through_snapshot: int) -> dict[str, dict[str, OracleRow]]:
    """Replay the op program in order into per-org terminal state up to a snapshot cap.

    Args:
        workload: The generated op program.
        verified_through_snapshot: Inclusive maximum snapshot ordinal replayed.

    Returns:
        Per-org record-id to terminal :class:`OracleRow` mapping before retention is applied.
    """
    state: dict[str, dict[str, OracleRow]] = {}
    for op in workload.ops:
        if op.snapshot > verified_through_snapshot:
            continue
        if op.scenario in ("conflict_update", "conflict_delete"):
            continue
        org_state: dict[str, OracleRow] = state.setdefault(op.org_id, {})
        org_state[op.record_id] = OracleRow(
            ts_us=op.ts_us,
            is_deleted=op.is_delete(),
            payload_version=op.payload_version,
            snapshot_ordinal=op.snapshot,
            scenario=op.scenario,
        )
    return state


def apply_retention(
    state: dict[str, dict[str, OracleRow]],
    settings: FuzzSettings,
    now_us: int,
) -> dict[str, dict[str, OracleRow]]:
    """Drop expired-live and ancient-tombstone records under the two-clock retention predicate.

    Live rows expire at ``ts < now - retention``. Tombstones expire only at
    ``ts < now - max(retention, replay_horizon)`` so their anti-resurrection watermark outlives
    every replayable window. In ``off`` mode the state is returned unchanged.

    Args:
        state: Per-org terminal state before retention.
        settings: Validated fuzz settings.
        now_us: Generation wall-clock instant in epoch microseconds.

    Returns:
        The state with expired records removed.
    """
    if settings.retention_mode != "short" or settings.retention_seconds is None:
        return state
    live_cutoff: int = now_us - settings.retention_seconds * 1_000_000
    tomb_cutoff: int = now_us - max(settings.retention_seconds, REPLAY_HORIZON_SECONDS) * 1_000_000
    kept: dict[str, dict[str, OracleRow]] = {}
    for org, rows in state.items():
        org_kept: dict[str, OracleRow] = {}
        for record_id, row in rows.items():
            expired: bool = (not row.is_deleted and row.ts_us < live_cutoff) or (
                row.is_deleted and row.ts_us < tomb_cutoff
            )
            if not expired:
                org_kept[record_id] = row
        kept[org] = org_kept
    return kept


def oracle_rows(
    workload: FuzzWorkload,
    settings: FuzzSettings,
    now_us: int,
    verified_through_snapshot: int,
) -> dict[str, dict[str, OracleRow]]:
    """Compute the expected per-org terminal published state up to a snapshot cap.

    Args:
        workload: The generated op program.
        settings: Validated fuzz settings.
        now_us: Generation wall-clock instant in epoch microseconds.
        verified_through_snapshot: Inclusive maximum snapshot ordinal replayed.

    Returns:
        Per-org record-id to terminal :class:`OracleRow` mapping after retention.
    """
    return apply_retention(replay_state(workload, verified_through_snapshot), settings, now_us)
