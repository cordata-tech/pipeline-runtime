"""The local backend: DuckDB and Parquet standing in for Glue, Spark and Iceberg.

Every function here honours one of the three contracts in `__init__`, and none
of them knows anything about fraud scoring or claims deduplication. That is the
executor's half of the ownership split — the domain's half is the YAML and the
SQL under `example/domains/`.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pandas as pd

from .. import catalog
from ..catalog import Schema
from ..descriptor import Source, Step, Target
from ..errors import PipelineError


def warehouse() -> Path:
    return Path(os.environ.get("CORDATA_WAREHOUSE", "example/warehouse"))


def local_path(location: str) -> Path:
    """Map the descriptor's `s3://bucket/prefix/` onto a directory.

    The descriptor keeps its published value — rewriting `location` per
    environment would put deployment detail in a domain-owned file, which is the
    coupling part 1 § 2 rule 4 exists to prevent. Object-store URI in, local
    directory out, entirely inside the backend.
    """
    parsed = urlparse(location)
    if parsed.scheme in ("", "file"):
        return Path(parsed.path or location)
    return warehouse() / parsed.netloc / parsed.path.strip("/")


# ---------------------------------------------------------------- readers


def read_glue_table(source: Source, schema: Schema) -> pd.DataFrame:
    with catalog.connect(read_only=True) as con:
        df = con.execute(f'SELECT * FROM "{source.database}"."{source.table}"').df()
    return schema.enforce(df)  # project, cast, fail on mismatch


def read_dms_landing(source: Source, schema: Schema) -> pd.DataFrame:
    # DMS CDC lands one file per batch with Op/before/after columns;
    # collapse to the latest row per key before anyone downstream sees it.
    prefix = dms_prefix(source)
    files = sorted(prefix.glob("*.parquet"))
    if not files:
        raise PipelineError(f"no DMS batches under {prefix} — run `python -m tools.seed`")

    raw = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = collapse_cdc(raw, keys=schema.primary_keys)
    return schema.enforce(df)


def dms_prefix(source: Source) -> Path:
    return warehouse() / "landing" / source.database / source.table


def collapse_cdc(raw: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Latest row per key, with deletes removed.

    A plain testable function rather than a block copy-pasted into every job
    that reads from DMS — which is the whole argument for the reader contract.

    `Op` is DMS's operation column: I/U/D, plus a load-phase row that carries no
    Op at all. Ordering is by `_dms_seq`, the sequence number DMS stamps on each
    change, because two updates to one key inside a batch are only distinguished
    by it — sorting on a timestamp loses to clock resolution.
    """
    if not keys:
        raise PipelineError("collapse_cdc needs a primary key; the catalog declares none")

    ordered = raw.sort_values("_dms_seq")
    latest = ordered.drop_duplicates(subset=keys, keep="last")
    alive = latest[latest["Op"].fillna("I") != "D"]
    return alive.drop(columns=[c for c in ("Op", "_dms_seq") if c in alive.columns])


READERS = {
    "glue_table": read_glue_table,
    "dms_landing": read_dms_landing,
}


# ---------------------------------------------------------------- writers


def _write(frame: pd.DataFrame, target: Target, tags: dict[str, str]) -> None:
    out = local_path(target.location)
    if out.exists():
        for stale in out.rglob("*.parquet"):
            stale.unlink()
    out.mkdir(parents=True, exist_ok=True)

    if target.partition_by:
        missing = [c for c in target.partition_by if c not in frame.columns]
        if missing:
            raise PipelineError(f"partition_by names columns the frame does not have: {missing}")
        _hive_partitioned(frame, target.partition_by).to_parquet(
            out, partition_cols=target.partition_by, index=False
        )
    else:
        frame.to_parquet(out / "part-0.parquet", index=False)

    _publish(frame, target, tags)


def _hive_partitioned(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Render date partition keys as `2026-07-04`, not `2026-07-04 00:00:00`.

    Hive-style partition values are strings in a directory name either way, and
    a midnight timestamp there gets percent-encoded into something no one can
    read or glob. DuckDB's DATE comes back through pandas as `datetime64`, so
    without this every `CAST(x AS DATE)` partition key inherits a time component
    it never had.
    """
    out = frame.copy()
    for key in keys:
        if pd.api.types.is_datetime64_any_dtype(out[key]):
            out[key] = out[key].dt.strftime("%Y-%m-%d")
    return out


def _publish(frame: pd.DataFrame, target: Target, tags: dict[str, str]) -> None:
    """Register the output and its tags in the same act as writing the data.

    Splitting these is how a catalog ends up describing last month's shape: the
    write succeeds, the crawler is late or never ran, and a subscriber reads a
    contract the pipeline was not executed against.
    """
    with catalog.connect() as con:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{target.database}"')
        con.register("_out", frame)
        con.execute(
            f'CREATE OR REPLACE TABLE "{target.database}"."{target.table}" AS SELECT * FROM _out'
        )
        cols = con.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [target.database, target.table],
        ).fetchall()

    version = 1
    schema = Schema(
        version=version,
        columns=tuple(
            catalog.Column(name=n, type=_duck_type(t), nullable=(nul == "YES"))
            for n, t, nul in cols
        ),
    )
    catalog.register(target, schema, tags)


def _duck_type(t: str) -> str:
    return {
        "VARCHAR": "VARCHAR",
        "BIGINT": "BIGINT",
        "INTEGER": "INTEGER",
        "DOUBLE": "DOUBLE",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_NS": "TIMESTAMP",
    }.get(t.upper(), t.upper())


# Two names, one body, on purpose. `iceberg` and `parquet` are genuinely
# different targets — one has snapshots and schema evolution, the other is files
# in a prefix — and the local backend cannot honour that difference. Collapsing
# them to a single registry entry would hide it; keeping both entries keeps the
# descriptors' distinction visible and marks exactly where an `aws` backend
# diverges.
def write_iceberg(frame: pd.DataFrame, target: Target, tags: dict[str, str]) -> None:
    _write(frame, target, tags)


def write_parquet(frame: pd.DataFrame, target: Target, tags: dict[str, str]) -> None:
    _write(frame, target, tags)


WRITERS = {"iceberg": write_iceberg, "parquet": write_parquet}


# ---------------------------------------------------------------- steps


def run_sql(frame: pd.DataFrame, step: Step, domain_root: Path) -> pd.DataFrame:
    """Run the domain's SQL against the frame.

    The frame is registered as `frame`, which is the one name a step author has
    to know. `params` are *bound*, not interpolated: `$model_version` reaches
    DuckDB as a parameter, so a descriptor cannot smuggle SQL through a param
    value even though the descriptor and the SQL are owned by the same team.
    """
    path = domain_root / step.query_file
    if not path.is_file():
        raise PipelineError(f"step {step.id}: query_file not found: {path}")

    con = duckdb.connect()
    try:
        con.register("frame", frame)
        return con.execute(path.read_text(), step.params or {}).df()
    except duckdb.Error as exc:
        raise PipelineError(f"step {step.id}: {exc}") from exc
    finally:
        con.close()


def run_python_module(frame: pd.DataFrame, step: Step, domain_root: Path) -> pd.DataFrame:
    """The escape hatch — part 1 § 3.

    Arbitrary domain Python, but reached *through* the descriptor, so the
    pipeline still declares its source, target, contract, suite and legal basis.
    Lineage, assertions and tag propagation all still happen. An escape hatch
    that bypassed the executor would forfeit every one of them.
    """
    module = importlib.import_module(step.module)
    fn = getattr(module, "run", None)
    if fn is None:
        raise PipelineError(f"step {step.id}: {step.module} has no run(frame, params)")
    result = fn(frame, step.params)
    if not isinstance(result, pd.DataFrame):
        raise PipelineError(f"step {step.id}: {step.module}.run returned {type(result).__name__}")
    return result


STEPS = {"sql": run_sql, "python": run_python_module}


class BACKEND:  # noqa: N801 — a namespace, addressed as backends.load().READERS
    name = "local"
    READERS = READERS
    WRITERS = WRITERS
    STEPS = STEPS
