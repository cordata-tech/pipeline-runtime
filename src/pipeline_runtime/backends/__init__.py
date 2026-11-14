"""The three registry contracts, stated once — part 1 § 3.

Anything registered has to satisfy one of them, which is what keeps "add a
source type" a one-line change.

A *backend* is one complete set of those bindings. The published descriptors say
`kind: glue_table` and `kind: iceberg`; the `local` backend binds those keys to
DuckDB and partitioned Parquet, an `aws` backend would bind them to Glue and
Iceberg-on-S3. The descriptor does not change, because part 1 § 2 rule 1 says it
declares intent and not mechanism — and this is where that stops being an
assertion and starts being a thing you can run.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Callable
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..catalog import Schema
    from ..descriptor import Source, Step, Target


class WriteMode(StrEnum):
    """What a writer did to the contents that were already there.

    Part of the writer contract rather than of the lineage code, because only
    the writer knows: both local writers replace an output whole — `_write`
    unlinks the stale Parquet and `_publish` issues `CREATE OR REPLACE TABLE`
    — while an appending writer for the same `kind` would not, and nothing
    outside it could tell the two apart.

    `emit` maps `REPLACE` to OpenLineage's `OVERWRITE` lifecycle state (#15).
    A mode it has no mapping for, `APPEND` today, produces no facet, so a
    writer that appends cannot inherit a claim that it replaced anything.
    The vocabulary is this project's rather than the spec's so that a backend
    stays a storage binding and does not depend on the lineage client.
    """

    REPLACE = "replace"
    APPEND = "append"


type Frame = pd.DataFrame
type Reader = Callable[[Source, Schema], Frame]
type Writer = Callable[[Frame, Target, dict[str, str]], WriteMode]
type StepRunner = Callable[[Frame, Step, Path], Frame]


class Backend:
    name: str
    READERS: dict[str, Reader]
    WRITERS: dict[str, Writer]
    STEPS: dict[str, StepRunner]


def load(name: str | None = None) -> Backend:
    """Which backend is a property of the deployment, never of a pipeline."""
    name = name or os.environ.get("CORDATA_BACKEND", "local")
    module = importlib.import_module(f".{name}", __package__)
    return module.BACKEND
