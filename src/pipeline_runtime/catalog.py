"""Schemas resolved from the catalog — part 1 § 4.

The published version calls `glue.get_table` and compares `VersionId`. This one
calls a local DuckDB metastore and compares the same thing. What matters is not
which metastore answers but that *something* answers at run time: nothing here
hardcodes a column list, and a descriptor that pins a version the catalog has
moved past fails before reading a single row.

The AWS shape is one import and one query away — the `resolve` signature, the
`SchemaDrift` it raises, and the column-level diff in the message are all
backend-independent, which is the point.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from .descriptor import Source, TableRef
from .errors import SchemaDrift


def catalog_path() -> Path:
    """The executor's own configuration, not any pipeline's.

    Every descriptor in a deployment resolves against the same catalog, so by
    part 1 § 2 rule 3 it has no business being a descriptor field. Read per call
    rather than captured at import, because a constant frozen at import time is
    configuration that can only be changed by restarting the process — and is
    the usual reason a module like this ends up untestable.
    """
    return Path(os.environ.get("CORDATA_CATALOG", "example/catalog.duckdb"))


# `tables` holds one row per column per version, mirroring Glue's linear
# table-definition history. `versions` holds one row per version saying who
# published it: the Glue equivalent is keys a publishing job sets in
# `TableInput.Parameters` on `UpdateTable`, which Glue archives with each
# `TableVersion`. `UpdateTable` replaces `Parameters` wholesale — measured
# against Glue, see `docs/evidence/glue-updatetable-parameters.md` — so nothing
# here carries a value forward from an earlier version either. See `publish`.
METASTORE_DDL = """
CREATE SCHEMA IF NOT EXISTS _catalog;
CREATE TABLE IF NOT EXISTS _catalog.tables (
    database      VARCHAR NOT NULL,
    "table"       VARCHAR NOT NULL,
    version       INTEGER NOT NULL,
    column_name   VARCHAR NOT NULL,
    column_type   VARCHAR NOT NULL,
    nullable      BOOLEAN NOT NULL,
    primary_key   BOOLEAN NOT NULL DEFAULT FALSE,
    ordinal       INTEGER NOT NULL,
    PRIMARY KEY (database, "table", version, column_name)
);
CREATE TABLE IF NOT EXISTS _catalog.versions (
    database  VARCHAR NOT NULL,
    "table"   VARCHAR NOT NULL,
    version   INTEGER NOT NULL,
    producer  VARCHAR NOT NULL,
    release   VARCHAR NOT NULL,
    PRIMARY KEY (database, "table", version)
);
CREATE TABLE IF NOT EXISTS _catalog.lf_tags (
    database  VARCHAR NOT NULL,
    "table"   VARCHAR NOT NULL,
    tag_key   VARCHAR NOT NULL,
    tag_value VARCHAR NOT NULL,
    PRIMARY KEY (database, "table", tag_key)
);
"""


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = catalog_path()
    if read_only and not path.exists():
        raise FileNotFoundError(f"no catalog at {path} — run `python -m tools.seed` first")
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path), read_only=read_only)
    if not read_only:
        con.execute(METASTORE_DDL)
    return con


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    primary_key: bool = False

    def __str__(self) -> str:
        return f"{self.name} ({self.type}, {'nullable' if self.nullable else 'not null'})"


@dataclass(frozen=True)
class Schema:
    """The resolved shape of a source, and the contract every reader ends at."""

    version: int
    columns: tuple[Column, ...]

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def primary_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.primary_key]

    def enforce(self, df: pd.DataFrame) -> pd.DataFrame:
        """Project, cast, fail on mismatch.

        Every reader ends here, which is what lets everything downstream — the
        steps, the suite, the writer — assume the frame matches the pin instead
        of defending against a source-shaped surprise.

        Extra columns are dropped rather than rejected: the *catalog* is the
        authority on drift and has already been consulted by the time a reader
        runs. A file carrying a stray column the catalog does not list is a
        landing-zone artefact, not a schema change.
        """
        missing = [c.name for c in self.columns if c.name not in df.columns]
        if missing:
            raise SchemaDrift(f"reader returned a frame missing {missing}")

        out = df[self.names].copy()
        for col in self.columns:
            out[col.name] = _cast(out[col.name], col.type)
            if not col.nullable and out[col.name].isna().any():
                raise SchemaDrift(f"{col.name} is declared not-null but the frame has nulls")
        return out

    @classmethod
    def from_catalog(cls, rows: list[tuple]) -> Schema:
        return cls(
            version=rows[0][0],
            columns=tuple(
                Column(name=n, type=t, nullable=bool(nul), primary_key=bool(pk))
                for _, n, t, nul, pk in rows
            ),
        )


_PANDAS_TYPES = {
    "VARCHAR": "string",
    "BIGINT": "Int64",
    "INTEGER": "Int64",
    "DOUBLE": "Float64",
    "BOOLEAN": "boolean",
    "DATE": "datetime64[ns]",
    "TIMESTAMP": "datetime64[ns]",
}


def _cast(series: pd.Series, duck_type: str) -> pd.Series:
    target = _PANDAS_TYPES.get(duck_type.upper())
    if target is None:
        raise SchemaDrift(f"catalog declares an unmapped column type: {duck_type}")
    try:
        return series.astype(target)
    except (TypeError, ValueError) as exc:
        raise SchemaDrift(f"cannot cast to {duck_type}: {exc}") from exc


def _rows(con: duckdb.DuckDBPyConnection, ref: TableRef, version: int) -> list[tuple]:
    return con.execute(
        """
        SELECT version, column_name, column_type, nullable, primary_key
          FROM _catalog.tables
         WHERE database = ? AND "table" = ? AND version = ?
         ORDER BY ordinal
        """,
        [ref.database, ref.table, version],
    ).fetchall()


def current_version(con: duckdb.DuckDBPyConnection, ref: TableRef) -> int:
    row = con.execute(
        'SELECT max(version) FROM _catalog.tables WHERE database = ? AND "table" = ?',
        [ref.database, ref.table],
    ).fetchone()
    if row is None or row[0] is None:
        raise SchemaDrift(f"{ref.fqn} is not registered in the catalog")
    return int(row[0])


def resolve(source: Source) -> Schema:
    """Fail before reading a row if the catalog has moved past the pin."""
    with connect(read_only=True) as con:
        version = current_version(con, source)

        if version != source.schema_version:
            raise SchemaDrift(
                f"{source.fqn} is at v{version}, descriptor pins "
                f"v{source.schema_version}. "
                f"{published_since(con, source, source.schema_version)}"
                f"Diff: {diff_columns(con, source, version)}. "
                f"Bump the pin to v{version} to accept.",
                found=version,
            )
        return Schema.from_catalog(_rows(con, source, version))


def columns_at(con: duckdb.DuckDBPyConnection, ref: TableRef, version: int) -> tuple[Column, ...]:
    return tuple(Column(n, t, bool(nul), bool(pk)) for _, n, t, nul, pk in _rows(con, ref, version))


@dataclass(frozen=True)
class Change:
    """One column-level difference between two versions of a table."""

    text: str
    # Whether a reader written against the older version stops working: a column
    # it projects is gone or retyped, or a column it may assume is filled can
    # now be null. An added column, or a nullable one tightened to not null,
    # leaves such a reader correct.
    breaking: bool

    def __str__(self) -> str:
        return self.text


def changes(older: Sequence[Column], newer: Sequence[Column]) -> list[Change]:
    before = {c.name: c for c in older}
    after = {c.name: c for c in newer}
    both = [n for n in after if n in before]

    out = [Change(f"+ {after[n]}", breaking=False) for n in after if n not in before]
    out += [Change(f"- {before[n]}", breaking=True) for n in before if n not in after]
    out += [
        Change(f"~ {n}: {before[n].type} -> {after[n].type}", breaking=True)
        for n in both
        if before[n].type != after[n].type
    ]
    out += [
        Change(
            f"~ {n}: {_nullability(before[n])} -> {_nullability(after[n])}",
            breaking=after[n].nullable,
        )
        for n in both
        if before[n].nullable != after[n].nullable
    ]
    return out


def _nullability(column: Column) -> str:
    return "nullable" if column.nullable else "not null"


def diff_columns(con: duckdb.DuckDBPyConnection, source: Source, version: int) -> str:
    """The column-level diff that makes the failure actionable rather than annoying.

    Part 2 § 4 puts this string on the lineage event, so it is read by a person
    deciding whether to bump the pin *and* by whoever asks six months later why
    the run stopped. Naming the columns is what turns "it broke" into "the
    upstream team added a merchant category code".
    """
    found = changes(
        columns_at(con, source, source.schema_version), columns_at(con, source, version)
    )
    return ", ".join(map(str, found)) or "no column changes — the version moved on its own"


# ---------------------------------------------------------------- who published a version


@dataclass(frozen=True)
class Publication:
    version: int
    producer: str | None  # None when the version carries no attribution
    release: str | None

    @property
    def by(self) -> str:
        if self.producer is None:
            return "an unrecorded producer"
        return f"{self.producer} release {self.release}"

    def __str__(self) -> str:
        return f"v{self.version} by {self.by}"


def publications(
    con: duckdb.DuckDBPyConnection, ref: TableRef, after: int, upto: int
) -> list[Publication]:
    """Every version in (after, upto] the catalog holds, with whoever published it.

    A version with no attribution is reported as such rather than skipped. In
    Glue that is the normal case for a version written by a crawler or by a job
    that set no parameters, and leaving it out would pin a change on the wrong
    release.
    """
    found = con.execute(
        """
        SELECT DISTINCT t.version, v.producer, v.release
          FROM _catalog.tables t
          LEFT JOIN _catalog.versions v USING (database, "table", version)
         WHERE t.database = ? AND t."table" = ? AND t.version > ? AND t.version <= ?
         ORDER BY t.version
        """,
        [ref.database, ref.table, after, upto],
    ).fetchall()
    return [Publication(v, producer, release) for v, producer, release in found]


def published_since(con: duckdb.DuckDBPyConnection, ref: TableRef, pinned: int) -> str:
    """The sentence naming the releases between a pin and the catalog, or nothing.

    Every version after the pin is named, not only the latest: the diff is
    cumulative, so any of those releases may be the one that removed the column.
    """
    found = publications(con, ref, after=pinned, upto=current_version(con, ref))
    match found:
        case []:
            return ""
        case [one]:
            return f"v{one.version} was published by {one.by}. "
        case _:
            return f"Published since v{pinned}: {', '.join(map(str, found))}. "


def publish(
    con: duckdb.DuckDBPyConnection,
    ref: TableRef,
    version: int,
    columns: Sequence[Column],
    *,
    producer: str,
    release: str,
) -> None:
    """Publish a new version of a source table, attributed to the release that made it.

    This is the producer's side of the contract: what an application's release
    pipeline does when it ships a schema change. In Glue it is `UpdateTable`
    with the producer and release set in `TableInput.Parameters`.

    Both are required on every call and nothing is copied from the version
    before, because Glue behaves the same way: `UpdateTable` replaces
    `Parameters` wholesale, dropping every key the job left out
    (`docs/evidence/glue-updatetable-parameters.md`). A publishing job has to
    write the full set each time, and a local catalog that quietly carried an
    earlier release forward would model something the real one does not do.

    Versions only move forward, as in Glue's version history. An existing
    version is never rewritten, because a pin on it is a claim about its shape.
    """
    if not producer.strip() or not release.strip():
        raise ValueError(f"{ref.fqn} v{version}: producer and release are both required")
    latest = con.execute(
        'SELECT max(version) FROM _catalog.tables WHERE database = ? AND "table" = ?',
        [ref.database, ref.table],
    ).fetchone()[0]
    if latest is not None and version <= latest:
        raise ValueError(f"{ref.fqn} is already at v{latest}; a published version is not rewritten")

    con.begin()
    try:
        con.executemany(
            "INSERT INTO _catalog.tables VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (ref.database, ref.table, version, c.name, c.type, c.nullable, c.primary_key, i)
                for i, c in enumerate(columns)
            ],
        )
        con.execute(
            "INSERT INTO _catalog.versions VALUES (?, ?, ?, ?, ?)",
            [ref.database, ref.table, version, producer, release],
        )
        con.commit()
    except BaseException:
        con.rollback()
        raise


def register(ref: TableRef, schema: Schema, tags: dict[str, str]) -> None:
    """Publish the output's shape and its tags, from the one declaration.

    Part 1 § 4: subscribers reading the catalog read the same contract the
    pipeline was executed against. That only holds if publishing the data and
    publishing its contract are the same act, so the writer calls this rather
    than leaving it to a separate crawler that may or may not have run.

    Unlike `publish`, this records no producer: the writer contract from part 1
    § 3 passes a target and tags, not the pipeline that wrote them. A pipeline's
    own output therefore reads as unrecorded to anything that pins it.
    """
    with connect() as con:
        con.execute(
            'DELETE FROM _catalog.tables WHERE database = ? AND "table" = ? AND version = ?',
            [ref.database, ref.table, schema.version],
        )
        con.executemany(
            "INSERT INTO _catalog.tables VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    ref.database,
                    ref.table,
                    schema.version,
                    c.name,
                    c.type,
                    c.nullable,
                    c.primary_key,
                    i,
                )
                for i, c in enumerate(schema.columns)
            ],
        )
        con.execute(
            'DELETE FROM _catalog.lf_tags WHERE database = ? AND "table" = ?',
            [ref.database, ref.table],
        )
        con.executemany(
            "INSERT INTO _catalog.lf_tags VALUES (?, ?, ?, ?)",
            [(ref.database, ref.table, k, v) for k, v in sorted(tags.items())],
        )
