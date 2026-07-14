"""Tests for deterministic bounded scale qualification evidence."""

from __future__ import annotations

from pathlib import Path

import pytest

from bench.config import BenchConfig
from bench.qualification import (
    external_scale_gates,
    generate_qualification_cohort,
    qualification_measurements,
    run_qualification,
    validate_capacity_evidence,
)


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
    assert first.mutation.vector_id == deleted.mutation.vector_id == recreated.mutation.vector_id


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
