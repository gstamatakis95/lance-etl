"""Minimal parameter-free scheduled CLI and restricted repair surface."""

from __future__ import annotations

import argparse
import json
import logging
import uuid
from collections.abc import Sequence
from dataclasses import asdict, is_dataclass
from typing import Any

from lance_etl.reconciler.runtime import build_runtime_application
from lance_etl.reconciler.service import ReconcilerApplication
from lance_etl.state import RoutingIdentity

logger: logging.Logger = logging.getLogger(__name__)

SCHEDULED_PHASES: tuple[str, ...] = (
    "plan_and_enqueue_window",
    "run_due_target_work",
    "reconcile_results",
    "gate_source_retention",
    "emit_slo_status",
)
"""Only actions exposed to the scheduled Airflow DAG."""


def build_parser() -> argparse.ArgumentParser:
    """Build the closed reconciler CLI without routine tuning flags.

    Returns:
        Argument parser containing five scheduled phases and one restricted repair command.
    """
    parser = argparse.ArgumentParser(description="Run one durable lance-etl reconciler phase")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for phase in SCHEDULED_PHASES:
        subparsers.add_parser(phase)
    repair = subparsers.add_parser("repair")
    repair.add_argument("--action", choices=("retry-blocked", "rollback"), required=True)
    repair.add_argument("--work-id", type=uuid.UUID)
    repair.add_argument("--retained-work-id", type=uuid.UUID)
    repair.add_argument("--tenant-id")
    repair.add_argument("--namespace")
    repair.add_argument("--org-id")
    repair.add_argument("--dry-run", action="store_true")
    return parser


def execute_command(application: ReconcilerApplication, args: argparse.Namespace) -> Any:
    """Dispatch one parsed closed action.

    Args:
        application: Fully wired durable reconciler.
        args: Parsed closed command.

    Returns:
        Typed phase or repair result.
    """
    if args.command == "plan_and_enqueue_window":
        return application.plan_and_enqueue_window()
    if args.command == "run_due_target_work":
        return application.run_due_target_work()
    if args.command == "reconcile_results":
        return application.reconcile_results()
    if args.command == "gate_source_retention":
        return application.gate_source_retention()
    if args.command == "emit_slo_status":
        return application.emit_slo_status()
    if args.action == "retry-blocked":
        if args.work_id is None:
            raise ValueError("retry-blocked requires --work-id")
        return application.repair_blocked_work(args.work_id, args.dry_run)
    if not args.tenant_id or not args.namespace or not args.org_id or args.retained_work_id is None:
        raise ValueError("rollback requires --tenant-id, --namespace, --org-id, and --retained-work-id")
    identity = RoutingIdentity(args.tenant_id, args.namespace, args.org_id).validate()
    return application.repair_rollback(identity, args.retained_work_id, args.dry_run)


def main(argv: Sequence[str] | None = None, application: ReconcilerApplication | None = None) -> int:
    """Run one reconciler action and print a bounded JSON result.

    Args:
        argv: Optional argument sequence.
        application: Optional explicitly injected runtime.

    Returns:
        Process exit code.
    """
    args = build_parser().parse_args(argv)
    resolved = application or build_runtime_application()
    result = execute_command(resolved, args)
    payload = asdict(result) if is_dataclass(result) else result
    logger.info("reconciler_result %s", json.dumps(payload, sort_keys=True, default=str))
    return 0
