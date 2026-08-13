"""$ python -m pipeline_runtime domains/fraud/pipelines/transactions_scored.yml"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

from .errors import PipelineError
from .run import run


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pipeline-runtime", description=__doc__)
    ap.add_argument("descriptor", type=Path, help="path to a descriptor YAML")
    ap.add_argument(
        "--run-id",
        default=None,
        help="OpenLineage run id (a UUID). Generated when omitted; supplied by the "
        "orchestrator in a deployment, which is what lets several producers "
        "accumulate facets against one run.",
    )
    args = ap.parse_args(argv)

    try:
        run(args.descriptor, args.run_id or str(uuid.uuid4()))
    except PipelineError as exc:
        # The trace already printed the reason and a FAIL event already carries
        # it. Exiting non-zero is what the orchestrator reads.
        print(f"pipeline-runtime: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
