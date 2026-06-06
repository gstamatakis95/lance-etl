"""Unit tests for the commit-conflict retry matcher in telemetry."""

from __future__ import annotations

import pytest

from lance_etl.telemetry import commit_with_retries


def test_retries_oserror_commit_conflict() -> None:
    """Retries an OSError whose message marks a Lance commit conflict."""
    attempts: list[int] = []

    def action() -> str:
        """Fail twice with a commit conflict, then succeed."""
        attempts.append(1)
        if len(attempts) < 3:
            raise OSError("LanceError(IO): Commit conflict for version 42")
        return "committed"

    result: str = commit_with_retries(action, retries=5, backoff_seconds=0.0)
    assert result == "committed"
    assert len(attempts) == 3


def test_retries_runtimeerror_retryable_commit_conflict() -> None:
    """Retries a RuntimeError whose message marks a retryable commit conflict."""
    attempts: list[int] = []

    def action() -> int:
        """Fail once with a retryable conflict, then succeed."""
        attempts.append(1)
        if len(attempts) < 2:
            raise RuntimeError("Retryable commit conflict for version 7: please retry")
        return 99

    result: int = commit_with_retries(action, retries=5, backoff_seconds=0.0)
    assert result == 99
    assert len(attempts) == 2


def test_reraises_original_exception_on_exhaustion() -> None:
    """Re-raises the original conflict exception when the budget is exhausted."""
    original: OSError = OSError("LanceError(IO): Commit conflict for version 1")

    def action() -> None:
        """Always raise the same conflict instance."""
        raise original

    with pytest.raises(OSError) as exc_info:
        commit_with_retries(action, retries=2, backoff_seconds=0.0)
    assert exc_info.value is original


def test_exhaustion_attempt_count() -> None:
    """Attempts exactly retries + 1 times before giving up."""
    attempts: list[int] = []

    def action() -> None:
        """Always raise a conflict."""
        attempts.append(1)
        raise RuntimeError("Commit conflict")

    with pytest.raises(RuntimeError):
        commit_with_retries(action, retries=3, backoff_seconds=0.0)
    assert len(attempts) == 4


def test_does_not_retry_unrelated_oserror() -> None:
    """Does not retry an OSError without a conflict marker in its message."""
    attempts: list[int] = []

    def action() -> None:
        """Raise a permission error that must propagate immediately."""
        attempts.append(1)
        raise OSError("Permission denied: s3://bucket/dataset")

    with pytest.raises(OSError, match="Permission denied"):
        commit_with_retries(action, retries=5, backoff_seconds=0.0)
    assert len(attempts) == 1


def test_does_not_retry_unrelated_runtimeerror() -> None:
    """Does not retry a RuntimeError without a conflict marker."""
    attempts: list[int] = []

    def action() -> None:
        """Raise an unrelated runtime error."""
        attempts.append(1)
        raise RuntimeError("schema mismatch: field order differs")

    with pytest.raises(RuntimeError, match="schema mismatch"):
        commit_with_retries(action, retries=5, backoff_seconds=0.0)
    assert len(attempts) == 1


def test_does_not_catch_other_exception_types() -> None:
    """Lets non-OSError, non-RuntimeError exceptions propagate untouched."""

    def action() -> None:
        """Raise a ValueError that the matcher must not handle."""
        raise ValueError("Commit conflict")

    with pytest.raises(ValueError):
        commit_with_retries(action, retries=5, backoff_seconds=0.0)


def test_on_conflict_called_per_conflict() -> None:
    """Invokes the on_conflict callback once per conflicting attempt."""
    conflicts: list[int] = []
    attempts: list[int] = []

    def action() -> str:
        """Fail twice, then succeed."""
        attempts.append(1)
        if len(attempts) < 3:
            raise OSError("Commit conflict")
        return "ok"

    result: str = commit_with_retries(action, retries=5, backoff_seconds=0.0, on_conflict=lambda: conflicts.append(1))
    assert result == "ok"
    assert len(conflicts) == 2
