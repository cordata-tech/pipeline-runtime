"""Which pipelines a proposed schema version would break — the producer's side.

    python -m pipeline_runtime.consumers PROPOSAL PATH [PATH ...]

An application runs this in its own CI before it publishes a new version of a
table, and fails its build on a non-zero exit. The consumer list is not
registered anywhere: it is every descriptor under the given paths whose source
names the table, read from the same pins the pipelines run on. A pipeline added
tomorrow is on the list the moment its descriptor is, and nobody has to remember
to add it. Readers that declare nothing — ad-hoc SQL, notebooks, BI tools — are
not on it.

Each consumer is compared from the version it pins to the proposal:

- BREAKS: a column it reads is removed or retyped, or may now be null where the
  pinned version promised it would not be (`catalog.changes`).
- BUMP: nothing it reads changes, but it still stops at its next run with
  `SchemaDrift` until its pin moves. That is the runtime working as part 1 § 4
  intends, so it does not fail the producer's build: a consumer cannot pin a
  version before the version exists, and failing on it would block every change.
- UNREADABLE / UNKNOWN PIN: the descriptor names the table but does not parse,
  or pins a version the catalog does not have. Either hides what the consumer
  depends on, so both fail the build.

Exit status: 0 when nothing fails, 1 when a consumer breaks or cannot be
checked, 2 when the proposal itself is unusable.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import Field, ValidationError, field_validator

from . import catalog
from .catalog import Change, Column
from .descriptor import Descriptor, Strict, TableRef
from .errors import PipelineError
from .run import load


class ProposedColumn(Strict):
    name: str
    type: str
    nullable: bool
    primary_key: bool = False


class Proposal(Strict):
    """The version a producer intends to publish, in the shape `catalog.publish` takes."""

    table: str  # database.table
    producer: str
    release: str
    columns: list[ProposedColumn] = Field(min_length=1)

    @field_validator("table")
    @classmethod
    def qualified(cls, value: str) -> str:
        if value.count(".") != 1 or not all(value.split(".")):
            raise ValueError(f"table must be database.table, got {value!r}")
        return value

    @property
    def ref(self) -> TableRef:
        database, table = self.table.split(".")
        return TableRef(database=database, table=table)

    @property
    def schema(self) -> tuple[Column, ...]:
        return tuple(Column(c.name, c.type, c.nullable, c.primary_key) for c in self.columns)


@dataclass(frozen=True)
class Verdict:
    status: str  # BREAKS, BUMP, UNREADABLE, UNKNOWN PIN
    path: Path
    pipeline: Descriptor | None = None
    changes: tuple[Change, ...] = ()
    detail: str = ""

    @property
    def fails(self) -> bool:
        return self.status != "BUMP"


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


def judge(path: Path, proposal: Proposal, con) -> Verdict | None:
    try:
        raw = yaml.safe_load(path.read_text())
    except yaml.YAMLError:
        return None  # not a descriptor anyone could be running
    if not names_table(raw, proposal.ref):
        return None

    try:
        pipeline = load(path)
    except (ValidationError, PipelineError) as exc:
        first = str(exc).splitlines()[0]
        return Verdict("UNREADABLE", path, detail=first)

    pinned = pipeline.source.schema_version
    columns = catalog.columns_at(con, proposal.ref, pinned)
    if not columns:
        return Verdict("UNKNOWN PIN", path, pipeline, detail=f"the catalog has no v{pinned}")

    found = tuple(catalog.changes(columns, proposal.schema))
    status = "BREAKS" if any(c.breaking for c in found) else "BUMP"
    return Verdict(status, path, pipeline, found)


def report(
    proposal: Proposal,
    latest: int | None,
    paths: list[Path],
    verdicts: list[Verdict],
    out,
) -> None:
    def line(text: str = "") -> None:
        print(text.rstrip(), file=out)

    proposed = (latest or 0) + 1
    line(
        f"{proposal.producer} release {proposal.release} proposes {proposal.table} v{proposed}; "
        + (f"the catalog is at v{latest}" if latest else "the table is not in the catalog yet")
    )
    scanned = ", ".join(map(str, paths))
    count = f"{len(verdicts)} descriptor{'' if len(verdicts) == 1 else 's'}"
    line(f"{count if verdicts else 'no descriptor'} under {scanned} read {proposal.table}")
    if not verdicts:
        return

    for v in verdicts:
        line()
        if v.pipeline is None:
            line(f"{v.status:<12}{v.path}")
            line(f"{'':<12}{v.detail}")
            continue
        meta = v.pipeline.metadata
        line(
            f"{v.status:<12}{meta.name} ({meta.domain})  pins v{v.pipeline.source.schema_version}"
            f"  owner {meta.owner}"
        )
        line(f"{'':<12}{v.path}")
        if v.detail:
            line(f"{'':<12}{v.detail}")
        for change in v.changes:
            line(f"{'':<12}{change}{'   <- breaks' if change.breaking else ''}")
        if v.status == "BUMP":
            line(f"{'':<12}stops at its next run until the pin moves to v{proposed}")

    failing = [v for v in verdicts if v.fails]
    line()
    line(
        f"{len(failing)} of {len(verdicts)} consumer{'s' if len(verdicts) != 1 else ''} "
        f"{'fail' if len(failing) != 1 else 'fails'} this check"
    )


def main(argv: list[str] | None = None, out=None) -> int:
    out = out or sys.stdout
    ap = argparse.ArgumentParser(prog="pipeline-runtime-consumers", description=__doc__)
    ap.formatter_class = argparse.RawDescriptionHelpFormatter
    ap.add_argument("proposal", type=Path, help="YAML: table, producer, release, columns")
    ap.add_argument("paths", type=Path, nargs="+", help="descriptor files or directories to scan")
    args = ap.parse_args(argv)

    try:
        proposal = Proposal.model_validate(yaml.safe_load(args.proposal.read_text()))
    except (OSError, yaml.YAMLError, ValidationError) as exc:
        print(
            f"pipeline-runtime-consumers: unusable proposal {args.proposal}: {exc}", file=sys.stderr
        )
        return 2

    with catalog.connect(read_only=True) as con:
        latest = con.execute(
            'SELECT max(version) FROM _catalog.tables WHERE database = ? AND "table" = ?',
            [proposal.ref.database, proposal.ref.table],
        ).fetchone()[0]
        verdicts = [
            v for p in descriptors(args.paths) if (v := judge(p, proposal, con)) is not None
        ]

    report(proposal, latest, args.paths, verdicts, out)
    return 1 if any(v.fails for v in verdicts) else 0


if __name__ == "__main__":
    raise SystemExit(main())
