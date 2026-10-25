"""Which descriptors read a table, found from the pins they declare.

`consumers` compares each reader's pin with a proposed version, which needs the
catalog and so the `run` extra. Finding the readers needs neither: a descriptor
reads a table when its source names it. That half lives here, on the base
install, so that a catalog server answering "who reads this table?" gets the
same list the producer-side check works from rather than a second derivation of
it (catalog-mcp#2).

Readers that declare nothing — ad-hoc SQL, notebooks, BI tools — are not found,
here or anywhere else that works from descriptors.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import ValidationError

from . import descriptor_v2
from .descriptor import Descriptor, TableRef
from .errors import PipelineError, UnknownApiVersion

# apiVersion is a dispatch key, not decoration. v2 is registered beside v1
# rather than replacing it, so descriptors written against v1 keep parsing while
# domains migrate one at a time.
MODELS: dict[str, type[Descriptor] | type[descriptor_v2.Descriptor]] = {
    "cordata.tech/v1": Descriptor,
    "cordata.tech/v2": descriptor_v2.Descriptor,
}


def parse(path: Path) -> Descriptor | descriptor_v2.Descriptor:
    """The descriptor as written, in the model its apiVersion names."""
    raw = yaml.safe_load(path.read_text())
    if (model := MODELS.get(raw.get("apiVersion"))) is None:
        raise UnknownApiVersion(raw.get("apiVersion"), known=sorted(MODELS))
    return model.model_validate(raw)


def load(path: Path) -> Descriptor:
    """The descriptor in the one shape the executor runs, whichever version it was written in."""
    return as_v1(parse(path))


def as_v1(declared: Descriptor | descriptor_v2.Descriptor) -> Descriptor:
    return declared.as_v1() if isinstance(declared, descriptor_v2.Descriptor) else declared


def descriptors(paths: list[Path]) -> Iterator[Path]:
    for path in paths:
        if path.is_file():
            yield path
        else:
            yield from sorted(p for p in path.rglob("*") if p.suffix in (".yml", ".yaml"))


def names_table(raw: object, ref: TableRef) -> bool:
    """Whether a YAML document is a descriptor whose source is this table.

    Read before validation, so that a descriptor too broken to parse is still
    recognised as a consumer rather than silently dropped from the list.
    Anything that is not a Cordata descriptor — an expectation suite, say — is
    not a consumer.
    """
    if not isinstance(raw, dict) or not str(raw.get("apiVersion", "")).startswith("cordata.tech/"):
        return False
    source = raw.get("source")
    return (
        isinstance(source, dict)
        and str(source.get("database", "")).strip() == ref.database
        and str(source.get("table", "")).strip() == ref.table
    )


@dataclass(frozen=True)
class Reader:
    """A descriptor whose source names the table.

    `pipeline` is None when it names the table but does not parse, and `detail`
    then says why. It is still a reader: what it depends on is hidden, not absent.
    """

    path: Path
    pipeline: Descriptor | None = None
    detail: str = ""


def readers(ref: TableRef, paths: list[Path]) -> list[Reader]:
    """Every descriptor under `paths` whose source is `ref`, in path order."""
    found = []
    for path in descriptors(paths):
        try:
            raw = yaml.safe_load(path.read_text())
        except yaml.YAMLError:
            continue  # not a descriptor anyone could be running
        if not names_table(raw, ref):
            continue
        try:
            found.append(Reader(path, load(path)))
        except (ValidationError, PipelineError) as exc:
            found.append(Reader(path, detail=str(exc).splitlines()[0]))
    return found
