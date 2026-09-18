"""Print the `cordata_provenance` facet from the last terminal event in a lineage log.

    python -m tools.show_provenance [PATH]

`out/lineage.ndjson` by default, or `CORDATA_LINEAGE_OUT` when it is set. The
log is append-only, so the last terminal event is the run that just finished;
its run id is printed beside it, which is what ties the facet to the `[abcd]`
tag on that run's trace.

Envelope keys are dropped — `_producer` and `_schemaURL` are on every facet and
say nothing about this run.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

TERMINAL = ("COMPLETE", "FAIL", "ABORT")


def last_terminal(path: Path) -> dict | None:
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    terminal = [e for e in events if e["eventType"] in TERMINAL]
    return terminal[-1] if terminal else None


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    path = Path(argv[0] if argv else os.environ.get("CORDATA_LINEAGE_OUT", "out/lineage.ndjson"))
    if not path.is_file():
        print(f"no lineage log at {path} — run a pipeline first", file=sys.stderr)
        return 1

    event = last_terminal(path)
    if event is None:
        print(f"{path} holds no terminal event", file=sys.stderr)
        return 1

    facet = {k: v for k, v in event["run"]["facets"]["cordata_provenance"].items() if k[0] != "_"}
    print(f"{event['eventType']}  run {event['run']['runId']}")
    print(json.dumps(facet, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
