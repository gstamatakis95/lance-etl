"""Application driver entered only after the benchmark launcher replaces its process."""

from __future__ import annotations

from bench.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
