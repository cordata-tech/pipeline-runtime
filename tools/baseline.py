"""Build a distributional baseline from a run the domain considers good.

A distributional expectation is only meaningful against a reference, and the
reference has to come from somewhere. This is that somewhere: read and transform
exactly as the executor would, then write the observed distribution of one
column to the domain's `baselines/` directory.

    python -m tools.baseline example/domains/fraud/pipelines/transactions_scored.yml \
        --column fraud_score --name baseline_fraud_score

The output is committed. Re-baselining is therefore a pull request with an
author and a reviewer on it — which is the point, because "the model drifted"
and "we decided the new distribution is fine" must not look the same in the
history.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_runtime import backends, catalog  # noqa: E402
from pipeline_runtime.run import load  # noqa: E402


def frame_for(descriptor_path: Path):
    pipeline = load(descriptor_path)
    backend = backends.load()
    schema = catalog.resolve(pipeline.source)
    frame = backend.READERS[pipeline.source.kind](pipeline.source, schema)
    for step in pipeline.steps:
        frame = backend.STEPS[step.kind](frame, step, descriptor_path.parents[1])
    return pipeline, frame


def partition_object(values: np.ndarray, bins: int) -> dict:
    """Great Expectations' continuous partition object: bin edges plus weights.

    Weights sum to 1, and every bin gets a floor so a zero-weight bin cannot
    make the KL divergence infinite the first time a single row lands there.
    """
    edges = np.linspace(float(values.min()), float(values.max()), bins + 1)
    counts, _ = np.histogram(values, bins=edges)
    weights = counts.astype(float) + 1e-9
    weights /= weights.sum()
    return {
        "bins": [round(float(e), 6) for e in edges],
        "weights": [round(float(w), 8) for w in weights],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("descriptor", type=Path)
    ap.add_argument("--column", required=True)
    ap.add_argument("--name", required=True, help="baseline file name, without .json")
    ap.add_argument("--bins", type=int, default=20)
    args = ap.parse_args(argv)

    pipeline, frame = frame_for(args.descriptor)
    if args.column not in frame.columns:
        print(f"{args.column} is not in the transformed frame: {list(frame.columns)}")
        return 1

    out = args.descriptor.parents[1] / "baselines" / f"{args.name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(partition_object(frame[args.column].to_numpy(dtype=float), args.bins), indent=2)
        + "\n"
    )
    print(f"{out}  {len(frame):,} rows, {args.bins} bins, from {pipeline.metadata.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
