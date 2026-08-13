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


# One version row per table, mirroring Glue's linear table-definition history.
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
                f"Diff: {diff_columns(con, source, version)}. "
                f"Bump the pin to v{version} to accept.",
                found=version,
            )
        return Schema.from_catalog(_rows(con, source, version))


def diff_columns(con: duckdb.DuckDBPyConnection, source: Source, version: int) -> str:
    """The column-level diff that makes the failure actionable rather than annoying.

    Part 2 § 4 puts this string on the lineage event, so it is read by a person
    deciding whether to bump the pin *and* by whoever asks six months later why
    the run stopped. Naming the columns is what turns "it broke" into "the
    upstream team added a merchant category code".
    """
    pinned = {
        c[1]: Column(c[1], c[2], bool(c[3])) for c in _rows(con, source, source.schema_version)
    }
    current = {c[1]: Column(c[1], c[2], bool(c[3])) for c in _rows(con, source, version)}

    parts = [f"+ {current[n]}" for n in current if n not in pinned]
    parts += [f"- {pinned[n]}" for n in pinned if n not in current]
    parts += [
        f"~ {n}: {pinned[n].type} -> {current[n].type}"
        for n in current
        if n in pinned and pinned[n].type != current[n].type
    ]
    return ", ".join(parts) or "no column changes — the version moved on its own"


def register(ref: TableRef, schema: Schema, tags: dict[str, str]) -> None:
    """Publish the output's shape and its tags, from the one declaration.

    Part 1 § 4: subscribers reading the catalog read the same contract the
    pipeline was executed against. That only holds if publishing the data and
    publishing its contract are the same act, so the writer calls this rather
    than leaving it to a separate crawler that may or may not have run.
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
