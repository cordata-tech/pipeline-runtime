"""`cordata.tech/v2` — both contracts named, each under the side it applies to.

v1 names one contract: the top-level `contract:` block, which is what a pipeline
promises downstream. What it depends on upstream is `source.schema_version`, a
field with no name of its own. v2 moves each under the table it binds —
`source.contract` and `target.contract` — and adds nothing, so every v2
descriptor has an exact v1 equivalent and the executor keeps running one shape.

A separate module rather than an edit to `descriptor.py`, which is part 1 § 3
verbatim. `run.MODELS` registers this beside v1, so the published descriptors
keep parsing while domains migrate one at a time. The reasoning is recorded on
cordata-tech/pipeline-runtime#1.
"""

from typing import Literal

from pydantic import Field

from . import descriptor as v1
from .descriptor import Expectations, Metadata, Processing, Step, Strict, TableRef


class InputContract(Strict):
    """What this pipeline depends on upstream.

    Checked against the catalog before a row is read, and read by
    `pipeline_runtime.consumers` when a producer asks who a change would break.
    Declared meaning — units, definitions — would belong here too once a
    pipeline varies in it.
    """

    schema_version: int = Field(ge=1)  # the catalog version this was written against


class OutputContract(v1.Contract):
    """What this pipeline promises downstream — v1's top-level `contract:`, moved."""


class Source(TableRef):
    kind: str  # key into READERS
    contract: InputContract


class Target(TableRef):
    kind: str  # key into WRITERS
    partition_by: list[str] = Field(default_factory=list)
    location: str
    contract: OutputContract


class Descriptor(Strict):
    apiVersion: Literal["cordata.tech/v2"]  # noqa: N815 — the wire spelling, not ours
    kind: Literal["TransformPipeline", "IngestionPipeline"]
    metadata: Metadata
    source: Source
    steps: list[Step] = Field(min_length=1)
    target: Target
    expectations: Expectations
    processing: Processing

    def as_v1(self) -> v1.Descriptor:
        """The same declaration in the shape the executor runs.

        Lossless while v2 only moves fields. A v2 field with no v1 home would
        have to be carried some other way, and this is where that shows up.
        """
        raw = self.model_dump()
        pin = raw["source"].pop("contract")
        promise = raw["target"].pop("contract")
        return v1.Descriptor.model_validate(
            {
                **raw,
                "apiVersion": "cordata.tech/v1",
                "source": {**raw["source"], **pin},
                "contract": promise,
            }
        )
