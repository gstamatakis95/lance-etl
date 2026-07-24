"""Tests for deterministic bounded scale qualification evidence."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import BenchConfig
from bench.qualification import (
    MAX_SHUFFLE_PARTITIONS,
    QualificationSizing,
    bucket_count,
    external_scale_gates,
    generate_qualification_cohort,
    qualification_measurements,
    run_qualification,
    shuffle_partition_count,
    validate_capacity_evidence,
)


def plan_config(**overrides: object) -> QualificationSizing:
    """Build qualification-only shuffle sizing inputs.

    Args:
        **overrides: Sizing inputs forwarded to :class:`QualificationSizing`.

    Returns:
        Qualification sizing inputs carrying the overrides.
    """
    return QualificationSizing(**overrides)


def test_cohort_is_reproducible_and_covers_required_adversarial_shapes() -> None:
    """The same seedless cohort fingerprint covers every mutation and width scenario."""
    first = qualification_measurements(2_000)
    second = qualification_measurements(2_000)
    assert first["cohort_sha256"] == second["cohort_sha256"]
    scenarios = first["scenario_counts"]
    assert scenarios["exact_duplicate"] > 0
    assert scenarios["late_hour"] + scenarios["late_wide_payload"] > 0
    assert scenarios["delete"] == scenarios["recreate"] > 0
    assert scenarios["wide_payload"] + scenarios["late_wide_payload"] > 0
    assert first["target_count"] > 100
    assert first["collapse"]["largest_target_input_rows"] > first["input_deliveries"] // 2
    assert first["routing"]["within_internal_cap"] is True
    assert first["collapse"]["passes_per_snapshot_target"] == 1


def test_duplicate_deliveries_are_byte_identical_and_delete_recreate_are_ordered() -> None:
    """Planted duplicate deliveries match exactly while lifecycle rows advance source order."""
    rows = list(generate_qualification_cohort(60))
    first = rows[0]
    duplicate = rows[1]
    deleted = rows[2]
    recreated = rows[3]
    assert duplicate.mutation == first.mutation
    assert duplicate.source_sequence == first.source_sequence
    assert deleted.scenario == "delete"
    assert recreated.scenario == "recreate"
    assert first.source_sequence < deleted.source_sequence < recreated.source_sequence
    assert first.mutation.record_id == deleted.mutation.record_id == recreated.mutation.record_id


def test_qualification_writes_capacity_measurements_and_explicit_external_gates(tmp_path: Path) -> None:
    """A local run records reproducibility while refusing to claim external 100M or 1B success."""
    config = BenchConfig(
        command="qualify",
        workspace=tmp_path / "workspace",
        corpus_root=tmp_path / "corpus",
        results_root=tmp_path / "results",
        run_id="qualification-test",
        qualification_rows=500,
    )
    artifact = run_qualification(config)
    assert (config.run_dir() / "qualify.json").exists()
    assert artifact["capacity"]["git_commit"]
    assert artifact["qualification"]["base_rows"] == 500
    assert [gate["scale"] for gate in artifact["external_scale_gates"]] == ["100M", "1B"]
    assert {gate["claim"] for gate in artifact["external_scale_gates"]} == {"NOT_RUN"}
    assert all("unmet_resources" in gate for gate in artifact["external_scale_gates"])


def test_large_local_cohort_requires_explicit_opt_in(tmp_path: Path) -> None:
    """Routine runs cannot accidentally allocate a hundred-million-row synthetic cohort."""
    config = BenchConfig(
        command="qualify",
        workspace=tmp_path / "workspace",
        results_root=tmp_path / "results",
        qualification_rows=1_000_001,
    )
    with pytest.raises(ValueError, match="allow-large-qualification"):
        run_qualification(config)


def test_capacity_validator_rejects_missing_exact_lance_version() -> None:
    """Qualification cannot proceed without the exact pylance release in its capacity evidence."""
    with pytest.raises(ValueError, match="software.pylance"):
        validate_capacity_evidence(
            {
                "git_commit": "a" * 40,
                "software": {"python": "3.14", "pylance": None},
                "hardware": {
                    "logical_cpu_count": 4,
                    "physical_memory_bytes": 1,
                    "disk_free_bytes_at_start": 1,
                },
                "cache_state": {
                    "workspace_existed_at_start": False,
                    "prepared_corpus_existed_at_start": False,
                    "lance_root_existed_at_start": False,
                },
                "workload": {"seed": 42},
            }
        )


def test_external_gates_name_resource_floors_commands_and_evidence() -> None:
    """External gates are machine-actionable instead of prose-only caveats."""
    for gate in external_scale_gates():
        assert gate["status"] == "EXTERNAL_GATE_REQUIRED"
        assert gate["minimum_resources"]["disk_free_bytes_at_start"] > 0
        assert gate["minimum_resources"]["physical_memory_bytes"] > 0
        expected_rows: str = "100000000" if gate["scale"] == "100M" else "1000000000"
        assert f"--limit {expected_rows}" in gate["command"]
        assert "python -m bench e2e" in gate["command"]
        assert "capacity.json" in gate["required_evidence"]


def test_external_gates_name_each_unmet_local_resource() -> None:
    """An undersized host gets exact available and required values instead of a scale claim."""
    capacity = {
        "hardware": {
            "disk_free_bytes_at_start": 1,
            "logical_cpu_count": 2,
            "physical_memory_bytes": 3,
        }
    }
    for gate in external_scale_gates(capacity):
        assert gate["status"] == "BLOCKED_EXTERNAL_CAPACITY"
        assert set(gate["unmet_resources"]) == {
            "disk_free_bytes_at_start",
            "logical_cpu_count",
            "physical_memory_bytes",
        }


class TestBucketCount:
    """bucket_count scales sub-buckets with rows and caps at max_buckets."""

    def test_bucket_count_small_org_is_one(self) -> None:
        """A dataset smaller than one bucket's rows gets a single sub-bucket."""
        assert bucket_count(rows=1_000, bucket_rows=2_000_000, max_buckets=32) == 1

    def test_bucket_count_scales_with_rows(self) -> None:
        """Ten million rows over a two-million bucket size yields five sub-buckets."""
        assert bucket_count(rows=10_000_000, bucket_rows=2_000_000, max_buckets=32) == 5

    def test_bucket_count_caps_at_max_buckets(self) -> None:
        """A dataset far larger than max_buckets buckets is capped at max_buckets."""
        assert bucket_count(rows=10_000_000_000, bucket_rows=2_000_000, max_buckets=32) == 32


class TestShufflePartitionCount:
    """shuffle_partition_count sizes the shuffle by the larger of the row and dataset floors."""

    def test_partition_count_row_floor_dominates(self) -> None:
        """Many rows over few trios lets the row floor set the width."""
        config: QualificationSizing = plan_config(bucket_rows=2_000_000, datasets_per_task=64)
        assert shuffle_partition_count(total_rows=100_000_000, trio_count=2, config=config) == 50

    def test_partition_count_dataset_floor_dominates(self) -> None:
        """30_000 trios with datasets_per_task=64 pins the width to ceil(30000/64)=469.

        This is the long-tail scaling law: a fleet of tens of thousands of tiny datasets must not
        serialize into a handful of tasks even when the total row count is small.
        """
        config: QualificationSizing = plan_config(bucket_rows=2_000_000, datasets_per_task=64)
        assert shuffle_partition_count(total_rows=100, trio_count=30_000, config=config) == 469

    def test_partition_count_clamps_to_max(self) -> None:
        """A row floor beyond MAX_SHUFFLE_PARTITIONS is clamped to the ceiling."""
        config: QualificationSizing = plan_config(bucket_rows=1)
        assert (
            shuffle_partition_count(total_rows=MAX_SHUFFLE_PARTITIONS + 1_000, trio_count=1, config=config)
            == MAX_SHUFFLE_PARTITIONS
        )

    def test_partition_count_num_partitions_override_wins(self) -> None:
        """An explicit num_partitions is returned verbatim regardless of rows or trios."""
        config: QualificationSizing = plan_config(num_partitions=7)
        assert shuffle_partition_count(total_rows=10_000_000_000, trio_count=99_999, config=config) == 7

    def test_partition_count_empty_increment_is_at_least_one(self) -> None:
        """An increment with no rows and no trios still gets one partition."""
        config: QualificationSizing = plan_config()
        assert shuffle_partition_count(total_rows=0, trio_count=0, config=config) == 1
