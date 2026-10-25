"""LF-tag resolution — part 1 § 6.

The published version calls `lakeformation.list_lf_tags()`. Here the
governance-owned vocabulary is a JSON file. Same property either way: the
executor holds no opinion about what `sensitivity: high` means, and cannot
resolve a value governance has not defined.
"""

from __future__ import annotations

import functools
import json
import os
from importlib.resources import files
from pathlib import Path

from .errors import UnknownTagKey, UnknownTagValue


def ontology_path() -> Path:
    if "CORDATA_ONTOLOGY" in os.environ:
        return Path(os.environ["CORDATA_ONTOLOGY"])
    local = Path("example/ontology.json")
    if local.is_file():
        return local
    # An installed copy has no `example/` beside it, so it falls back to the one
    # the wheel carries.
    try:
        return Path(str(files("pipeline_runtime.example") / "ontology.json"))
    except ModuleNotFoundError:
        pass
    # An editable install maps no `pipeline_runtime.example`, and until 0.2.6
    # this returned the relative path above, so a run whose working directory
    # was not the repository root reported the ontology missing when it was
    # beside the package all along (#14). Imported here rather than at the top:
    # the resolution belongs to the example, and `policy` is read by every run.
    from .demo import example_root

    try:
        return example_root() / "ontology.json"
    except FileNotFoundError:
        return local


@functools.lru_cache(maxsize=1)
def _load(path: str, mtime: float) -> dict[str, list[str]]:
    # mtime is in the cache key so an edited ontology is picked up without a
    # restart, while a single run still reads the file once.
    del mtime
    raw = json.loads(Path(path).read_text())
    # Underscore keys are annotation, not vocabulary. Without this they would
    # show up in the `allowed:` list of every UnknownTagKey error.
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def ontology() -> dict[str, list[str]]:
    path = ontology_path()
    if not path.is_file():
        raise FileNotFoundError(
            f"no LF-tag ontology at {path}. In a deployment this is the "
            "governance account's vocabulary; here it is a file — set CORDATA_ONTOLOGY."
        )
    return _load(str(path), path.stat().st_mtime)


def resolve(declared: dict[str, str]) -> dict[str, str]:
    known = ontology()  # governance-owned vocabulary
    for key, value in declared.items():
        if key not in known:
            raise UnknownTagKey(key, allowed=sorted(known))
        if value not in known[key]:
            raise UnknownTagValue(key, value, allowed=known[key])
    return declared
