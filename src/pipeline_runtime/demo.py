"""`pipeline-runtime demo` — seed the example and run it, from an installed copy.

The quickstart used to begin with `git clone`, because the data the example
reads was built by a script that lived only in the repository. Since 0.2.0 the
descriptors travel in the wheel, so the only thing still missing was the seed —
`seed.py` now sits in the package, and this walks the same path the README's
quickstart walks: seed a catalog and two source tables, then run all three
pipelines against them.

What it writes, all inside the target directory and nowhere else:

    catalog.duckdb      the local metastore, standing in for Glue
    warehouse/          where `s3://…` locations land, including the DMS batches
    out/lineage.ndjson  every OpenLineage event the three runs emitted

The directory is addressed through the same `CORDATA_*` variables a deployment
uses, rather than through a second mechanism — so a reader who later points
those at their own paths is using what the demo already used.
"""

from __future__ import annotations

import os
import sys
import uuid
from importlib.resources import files
from pathlib import Path
from typing import TextIO

# What a previous demo leaves behind, and therefore what a re-run may overwrite.
# Anything else in the directory belongs to somebody, and this stops.
OURS = frozenset({"catalog.duckdb", "catalog.duckdb.wal", "warehouse", "out"})

DOMAINS = ("fraud", "policy", "finance")


def example_root() -> Path:
    """The packaged `example/` as a filesystem path, or the clone's own.

    Reached through a file inside the package rather than the package directory,
    because `str()` of the directory can be a `MultiplexedPath` repr rather than
    a path — which is how the first version of this looked for descriptors under
    `MultiplexedPath('…/example')` and found none. `policy.ontology_path()` takes
    the same route for the same reason.

    `pyproject.toml` maps the package at the repository's `example/` rather than
    copying it, so a clone and an install read the same bytes. An editable
    install maps no such package, and the source tree keeps `example/` beside
    the package rather than inside it — so the fallback is found from this
    module's own location.

    It used to be the relative `Path("example")`, which resolved only when the
    working directory happened to be the repository root, while `demo` exists to
    be pointed at some other directory. So the command failed in a clone, which
    is the one place a contributor runs it, and the test fixture chdir'd to the
    repository to work around it (#14). Searching upward from the working
    directory would be the wrong repair: inside somebody else's checkout it
    would find their `example/`.
    """
    candidates = []
    try:
        inside = files("pipeline_runtime.example") / "ontology.json"
    except ModuleNotFoundError:
        pass
    else:
        candidates.append(Path(str(inside)).parent)
    candidates.append(Path(__file__).resolve().parents[2] / "example")

    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError(
        "no example/ directory: looked in "
        + ", ".join(str(c) for c in candidates)
        + " — an installed copy carries its own, and a clone has one beside the package"
    )


def descriptors() -> list[Path]:
    """The example descriptor of each domain, in the order the demo runs them."""
    root = example_root()
    found = []
    for domain in DOMAINS:
        pipelines = sorted((root / "domains" / domain / "pipelines").glob("*.yml"))
        if not pipelines:
            raise FileNotFoundError(
                f"no descriptor for the {domain} domain under {root} — an install "
                "missing part of its example, which the release workflow checks for"
            )
        found.extend(pipelines)
    return found


def unexpected(target: Path) -> list[str]:
    """Entries in `target` that a demo did not put there, sorted."""
    if not target.exists():
        return []
    return sorted(p.name for p in target.iterdir() if p.name not in OURS)


def demo(target: Path, out: TextIO | None = None) -> int:
    """Seed `target` and run the three pipelines against it.

    Returns a process exit status: 0 when all three ran, 1 when the directory
    is somebody else's.
    """
    from .run import run  # both need the `run` extra, so they are imported here
    from .seed import seed

    out = out or sys.stdout
    strangers = unexpected(target)
    if strangers:
        print(
            f"pipeline-runtime: {target} holds files this demo did not write "
            f"({', '.join(strangers[:5])}{', …' if len(strangers) > 5 else ''}). "
            "Point it at an empty directory, or name a new one: pipeline-runtime demo ./demo",
            file=sys.stderr,
        )
        return 1

    # Before anything is written: an install missing part of its example should
    # say so with the directory still empty, rather than after seeding it.
    found = descriptors()

    target.mkdir(parents=True, exist_ok=True)
    os.environ["CORDATA_CATALOG"] = str(target / "catalog.duckdb")
    os.environ["CORDATA_WAREHOUSE"] = str(target / "warehouse")
    os.environ["CORDATA_LINEAGE_OUT"] = str(target / "out" / "lineage.ndjson")

    # The transport appends, which is what makes several producers accumulate
    # against one run in a deployment and what makes a second demo in the same
    # directory report six COMPLETE events for three pipelines. A demo starts
    # from nothing, as the seed does.
    Path(os.environ["CORDATA_LINEAGE_OUT"]).unlink(missing_ok=True)

    written = seed(clean=True)
    print(
        f"seed        {written['transactions']:,} transactions at "
        f"v{written['transactions_version']}, {written['claims']:,} CDC claims rows",
        file=out,
    )

    for descriptor in found:
        run(descriptor, str(uuid.uuid4()))

    print(f"\ndescriptors {found[0].parents[2]}", file=out)
    print(f"events      {os.environ['CORDATA_LINEAGE_OUT']}", file=out)
    return 0
