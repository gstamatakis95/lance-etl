"""Application driver entered only after the reconciler launcher replaces its process."""

from __future__ import annotations

from lance_etl.reconciler.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
