"""Local one-process reconciliation and restricted repair CLI."""

from __future__ import annotations

import argparse
import json
import logging
import math
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import asdict, is_dataclass
from typing import Any, Protocol

from lance_etl.reconciler.migrations import build_runtime_migrator
from lance_etl.reconciler.runtime import build_runtime_application, build_runtime_operator
from lance_etl.reconciler.service import ReconcilerApplication, ReconcilerOperator, RunOnceSummary, SloStatus
from lance_etl.source import SourceConfigurationError
from lance_etl.state import RoutingIdentity

logger: logging.Logger = logging.getLogger(__name__)

EXIT_UNHEALTHY_STATUS: int = 3
"""Process exit code for `status` when `SloStatus.healthy` is `False`.

The distinct nonzero code lets a shell-level health check separate unhealthy state from an
unexpected command failure.
"""


class MigrationRunner(Protocol):
    """Injected database migration operation for the local CLI."""

    def migrate(self) -> None:
        """Upgrade the configured control-plane database to the current schema."""
        ...


def positive_seconds(value: str) -> float:
    """Parse a strictly positive polling interval.

    Args:
        value: Command-line value.

    Returns:
        Positive interval in seconds.

    Raises:
        argparse.ArgumentTypeError: If the value is not a positive number.
    """
    try:
        seconds: float = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("poll seconds must be a number") from exc
    if not math.isfinite(seconds) or seconds <= 0:
        raise argparse.ArgumentTypeError("poll seconds must be positive")
    return seconds


def positive_integer(value: str) -> int:
    """Parse a strictly positive integer identity.

    Args:
        value: Command-line value.

    Returns:
        Positive integer.

    Raises:
        argparse.ArgumentTypeError: If the value is not a positive integer.
    """
    try:
        parsed: int = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    """Build the local reconciler CLI.

    Returns:
        Argument parser containing migration, run, status, and repair commands.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="Run the local PostgreSQL-backed lance-etl reconciler"
    )
    subparsers: Any = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("migrate")
    subparsers.add_parser("run-once")
    run: argparse.ArgumentParser = subparsers.add_parser("run")
    run.add_argument("--poll-seconds", type=positive_seconds)
    subparsers.add_parser("status")
    repair: argparse.ArgumentParser = subparsers.add_parser("repair")
    repair.add_argument("--action", choices=("retry-blocked", "retry-blocked-source", "rebuild"), required=True)
    repair.add_argument("--work-id", type=uuid.UUID)
    repair.add_argument("--source-snapshot-seq", type=positive_integer)
    repair.add_argument("--request-id", type=uuid.UUID)
    repair.add_argument("--tenant-id")
    repair.add_argument("--namespace")
    repair.add_argument("--org-id")
    repair.add_argument("--dry-run", action="store_true")
    return parser


def run_loop(
    application: ReconcilerApplication,
    poll_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> RunOnceSummary | None:
    """Run complete reconciliation cycles until interrupted.

    Args:
        application: Fully wired local application.
        poll_seconds: Delay between cycles.
        sleep: Interruptible wait operation.

    Returns:
        Last completed cycle result, or ``None`` if interrupted earlier.
    """
    last_result: RunOnceSummary | None = None
    try:
        while True:
            try:
                last_result = application.run_once()
            except SourceConfigurationError:
                logger.exception("reconciler_terminal_source_configuration")
                raise
            except Exception:
                logger.exception("reconciler_cycle_failed")
            else:
                payload: dict[str, Any] | RunOnceSummary = (
                    asdict(last_result) if is_dataclass(last_result) else last_result
                )
                print(json.dumps(payload, sort_keys=True, default=str))
            sleep(poll_seconds)
    except KeyboardInterrupt:
        logger.info("reconciler_stopped")
        return last_result


def execute_command(
    application: ReconcilerApplication | ReconcilerOperator | None,
    args: argparse.Namespace,
    migrator: MigrationRunner | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> Any:
    """Dispatch one parsed closed action.

    Args:
        application: Fully wired local reconciler for non-migration commands.
        args: Parsed closed command.
        migrator: Optional injected database migration operation.
        sleep: Interruptible wait operation for the continuous run command.

    Returns:
        Typed command result.

    Raises:
        ValueError: If the selected command lacks its required runtime.
    """
    if args.command == "migrate":
        if migrator is None:
            raise ValueError("migrate requires a configured migration runner")
        migrator.migrate()
        return {"migrated": True}
    if application is None:
        raise ValueError(f"{args.command} requires a configured reconciler application")
    if args.command == "run-once":
        if isinstance(application, ReconcilerOperator):
            raise ValueError("run-once requires the full reconciliation application")
        return application.run_once()
    if args.command == "run":
        if isinstance(application, ReconcilerOperator):
            raise ValueError("run requires the full reconciliation application")
        poll_seconds: float = (
            args.poll_seconds if args.poll_seconds is not None else application.settings.poll_interval.total_seconds()
        )
        return run_loop(application, poll_seconds, sleep)
    if args.command == "status":
        return application.emit_slo_status()
    return execute_repair(application, args)


def execute_repair(
    application: ReconcilerApplication | ReconcilerOperator,
    args: argparse.Namespace,
) -> bool | uuid.UUID | None:
    """Dispatch one validated restricted repair action.

    Args:
        application: Full reconciler or PostgreSQL-only operator.
        args: Parsed repair arguments.

    Returns:
        Repair transition result or enqueued work identity.

    Raises:
        ValueError: If required repair identity fields are absent.
    """
    if args.action == "retry-blocked":
        if args.work_id is None:
            raise ValueError("retry-blocked requires --work-id")
        return application.repair_blocked_work(args.work_id, args.dry_run)
    if args.action == "retry-blocked-source":
        if args.source_snapshot_seq is None:
            raise ValueError("retry-blocked-source requires --source-snapshot-seq")
        return application.repair_blocked_source_snapshot(args.source_snapshot_seq, args.dry_run)
    if not args.tenant_id or not args.namespace or not args.org_id or args.request_id is None:
        raise ValueError("rebuild requires --tenant-id, --namespace, --org-id, and --request-id")
    identity: RoutingIdentity = RoutingIdentity(args.tenant_id, args.namespace, args.org_id).validate()
    return application.repair_rebuild(identity, args.request_id, args.dry_run)


def main(
    argv: Sequence[str] | None = None,
    application: ReconcilerApplication | ReconcilerOperator | None = None,
    migrator: MigrationRunner | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Run one reconciler action and print a bounded JSON result.

    Args:
        argv: Optional argument sequence.
        application: Optional explicitly injected runtime.
        migrator: Optional explicitly injected migration operation.
        sleep: Interruptible wait operation for continuous execution.

    Returns:
        Process exit code: ``0`` for every command's normal outcome, except that ``status``
        returns :data:`EXIT_UNHEALTHY_STATUS` when the evaluated `SloStatus.healthy` is `False`, so
        a shell-level health check or cron wrapper cannot see success on an unhealthy control plane.
        ``run``/``run-once`` semantics are unchanged: retries and blocks are normal operation there.
    """
    args: argparse.Namespace = build_parser().parse_args(argv)
    resolved: ReconcilerApplication | ReconcilerOperator | None = application
    resolved_migrator: MigrationRunner | None = migrator
    owns_runtime: bool = False
    try:
        if args.command == "migrate" and resolved_migrator is None:
            resolved_migrator = build_runtime_migrator()
        if resolved is None and args.command in ("run", "run-once"):
            resolved = build_runtime_application()
            owns_runtime = True
        if resolved is None and args.command != "migrate":
            resolved = build_runtime_operator()
            owns_runtime = True
        result: Any = execute_command(resolved, args, resolved_migrator, sleep)
        payload: Any = asdict(result) if is_dataclass(result) else result
        print(json.dumps(payload, sort_keys=True, default=str))
        if args.command == "status" and isinstance(result, SloStatus) and not result.healthy:
            return EXIT_UNHEALTHY_STATUS
        return 0
    finally:
        if owns_runtime and resolved is not None:
            resolved.close()
