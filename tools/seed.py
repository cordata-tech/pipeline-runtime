"""Build the local catalog and the source data the example pipelines read.

    python -m tools.seed              # the happy path
    python -m tools.seed --drift      # move fraud_raw.transactions to v8
    python -m tools.seed --clean      # delete the catalog and warehouse first

A wrapper over `pipeline_runtime.seed`, which is where the code lives so that
`pipeline-runtime demo` can seed from an installed copy with no checkout (#8).
This spelling stays because CI, the test suite and the README's drift scenario
all call it, and because a contributor working in a clone should not have to
install the package to seed it.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_runtime.seed import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
