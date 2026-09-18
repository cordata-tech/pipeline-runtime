"""Capture the runs that published writing quotes, into docs/evidence/.

    python -m tools.capture_evidence                      # all of them
    python -m tools.capture_evidence provenance-names-the-release.md

Naming files captures only those, which is the usual case: each file records
one run, so re-capturing the others would replace transcripts that are already
quoted, with new run ids and a later commit, for no reason.

Refuses to run on a dirty working tree, so the commit named in each file is the
code that produced its output. Each command runs from the repository root
exactly as written in the file, with stdout and stderr captured together in the
order they were printed (`PYTHONUNBUFFERED` keeps a pipe from reordering them),
and its exit status recorded after it.

The files are a record of one run and are not meant to be regenerated: a later
run prints a different run id. Re-capturing is for when the code they describe
has changed.
"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
from datetime import date
from pathlib import Path

from pipeline_runtime import __version__

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "docs/evidence"
PY = "./.venv/bin/python"

TRANSCRIPTS = {
    "schema-drift-names-the-release.md": (
        "Transcript: a `SchemaDrift` failure that names the release",
        "The consumer side. `fraud_raw.transactions` is published at v7 by card-ledger "
        "release v4.11.0, then moved to v8 by card-ledger release v4.12.0, which adds "
        "a column. The fraud pipeline still pins v7 and stops before reading a row.",
        [
            f"{PY} -m tools.seed --clean",
            f"{PY} -m tools.seed --drift",
            f"{PY} -m pipeline_runtime example/domains/fraud/pipelines/transactions_scored.yml",
        ],
    ),
    "provenance-names-the-release.md": (
        "Transcript: a successful run carrying the release behind its input",
        "The chain, on a run that worked. `fraud_raw.transactions` is at v7, published by "
        "card-ledger release v4.11.0, and the pipeline pins v7 — so nothing fails, and the "
        "event it emits still says which application release gave the input its shape. "
        "`descriptor_git_commit_signed` reaches the reviewed commit that authorised the "
        "pipeline; `source_published_by` and `source_published_release` reach the change "
        "upstream of it.",
        [
            f"{PY} -m tools.seed --clean",
            f"{PY} -m pipeline_runtime example/domains/fraud/pipelines/transactions_scored.yml",
            f"{PY} -m tools.show_provenance",
        ],
    ),
    "consumers-breaking-change.md": (
        "Transcript: the producer-side check failing on a breaking change",
        "The producer side. The catalog is at v7. card-ledger release v5.0.0 proposes "
        "renaming `merchant_id` to `merchant_ref`; the check finds the two descriptors "
        "that pin the table and exits 1. The second run is the contrast: release "
        "v4.12.0 only adds a column, and the check exits 0 while listing the pipelines "
        "whose pins have to move.",
        [
            f"{PY} -m tools.seed --clean",
            f"{PY} -m pipeline_runtime.consumers "
            "example/proposals/card-ledger-v5.0.0.yml example/domains",
            f"{PY} -m pipeline_runtime.consumers "
            "example/proposals/card-ledger-v4.12.0.yml example/domains",
        ],
    ),
}


def git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.strip()


def run(command: str) -> tuple[str, int]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("CORDATA_")}
    result = subprocess.run(
        command.split(),
        cwd=REPO,
        env={**env, "PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.stdout, result.returncode


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if unknown := [name for name in argv if name not in TRANSCRIPTS]:
        print(f"no such transcript: {unknown}; have {sorted(TRANSCRIPTS)}", file=sys.stderr)
        return 2
    wanted = {name: TRANSCRIPTS[name] for name in argv} if argv else TRANSCRIPTS

    if dirty := git("status", "--porcelain"):
        print(f"refusing to capture on a dirty working tree:\n{dirty}", file=sys.stderr)
        return 1

    commit = git("rev-parse", "--short=7", "HEAD")
    OUT.mkdir(parents=True, exist_ok=True)

    for name, (title, intro, commands) in wanted.items():
        lines = [
            f"# {title}",
            "",
            f"Captured by `tools/capture_evidence.py` on {date.today().isoformat()}, from "
            f"pipeline-runtime {__version__} at commit `{commit}` with a clean working "
            f"tree, on Python {platform.python_version()}. The output below is what each "
            "command printed, stdout and stderr together, unedited, followed by its exit "
            "status. Run the same commands from the repository root to reproduce it; the "
            "four-character run id differs on every run.",
            "",
            intro,
            "",
        ]
        for command in commands:
            output, status = run(command)
            lines += ["```console", f"$ {command}", output.rstrip(), "```", ""]
            lines += [f"Exit status {status}.", ""]
        (OUT / name).write_text("\n".join(lines))
        print(f"wrote {(OUT / name).relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
