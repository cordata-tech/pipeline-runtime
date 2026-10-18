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
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ..catalog import Schema
    from ..descriptor import Source, Step, Target

type Frame = pd.DataFrame
type Reader = Callable[[Source, Schema], Frame]
type Writer = Callable[[Frame, Target, dict[str, str]], None]
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
