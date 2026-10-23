"""$ python -m pipeline_runtime domains/fraud/pipelines/transactions_scored.yml

$ python -m pipeline_runtime demo ./demo      # seed the example and run all of it
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

from .errors import PipelineError


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv

    # Running a pipeline needs the `run` extra; reading the model does not. Say
    # which one is missing rather than surfacing a bare ImportError for duckdb.
    # `demo` needs the same extra, and the message is the same one.
    try:
        from .run import run
    except ModuleNotFoundError as exc:
        what = "seeding and running the example" if argv[:1] == ["demo"] else "running a pipeline"
        print(
            f"pipeline-runtime: {what} needs {exc.name}. Install the "
            "execution dependencies: pip install 'cordata-pipeline-runtime[run]'",
            file=sys.stderr,
        )
        return 2

    if argv[:1] == ["demo"]:
        # A subcommand rather than a flag, and checked before the parser below,
        # because `pipeline-runtime <descriptor>` is the published spelling and
        # part 1 § 3 shows it. Turning that into a subcommand too would break
        # every line of it.
        return _demo(argv[1:])

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


def _demo(argv: list[str]) -> int:
    from .demo import demo

    ap = argparse.ArgumentParser(
        prog="pipeline-runtime demo",
        description="Seed the example and run its three pipelines, writing into DIR.",
    )
    ap.add_argument(
        "dir",
        nargs="?",
        default=Path(),
        type=Path,
        help="where to write the catalog, the warehouse and the events (default: here)",
    )
    args = ap.parse_args(argv)

    try:
        return demo(args.dir)
    except PipelineError as exc:
        print(f"pipeline-runtime: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
