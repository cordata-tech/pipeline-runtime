"""The shape every descriptor has to satisfy.

Published as part 1 § 3. This file is the article's code, not a paraphrase of
it — `tests/test_post_conformance.py` asserts as much against the markdown when
the site repo is checked out alongside.
"""

from datetime import timedelta
from enum import Enum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Strict(BaseModel):
    """Shared config for every descriptor model.

    `frozen` stops anything mutating a descriptor after load.
    `str_strip_whitespace` absorbs the trailing spaces hand-edited YAML
    collects — without it `fraud_raw ` is a different database from `fraud_raw`.
    """

    # Deliberately not strict=True: strict mode wants a timedelta instance and
    # would reject `freshness_sla: PT4H`.
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class OnFailure(str, Enum):
    BLOCK_PUBLISH = "block_publish"
    QUARANTINE = "quarantine"
    WARN = "warn"


class Metadata(Strict):
    name: str = Field(pattern=r"^[a-z][a-z0-9-]{2,63}$")
    domain: str
    owner: str  # routed to on failure


class TableRef(Strict):
    """A catalog address. Source and Target are each one, plus their own extras."""

    database: str
    table: str

    @property
    def fqn(self) -> str:
        return f"{self.database}.{self.table}"


class Source(TableRef):
    kind: str  # key into READERS
    schema_version: int = Field(ge=1)  # the catalog version this was written against


class Step(Strict):
    id: str
    kind: str  # key into STEPS
    query_file: str | None = None
    module: str | None = None  # the escape hatch — see below
    params: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def exactly_one_body(self) -> Self:
        if bool(self.query_file) == bool(self.module):
            raise ValueError(f"step {self.id}: set exactly one of query_file / module")
        return self


class Target(TableRef):
    kind: str  # key into WRITERS
    partition_by: list[str] = Field(default_factory=list)
    location: str


class Contract(Strict):
    freshness_sla: timedelta  # ISO-8601, e.g. PT4H
    lf_tags: dict[str, str]  # validated against the ontology — § 6


class Expectations(Strict):
    suite: str
    on_failure: OnFailure


class Processing(Strict):
    """DSGVO Art. 30 fields. Travel outward on the lineage event."""

    purpose: str
    legal_basis: Literal[
        "consent",
        "contract",
        "legal-obligation",
        "vital-interests",
        "public-task",
        "legitimate-interest",
    ]


class Descriptor(Strict):
    apiVersion: Literal["cordata.tech/v1"]  # noqa: N815 — the wire spelling, not ours
    kind: Literal["TransformPipeline", "IngestionPipeline"]
    metadata: Metadata
    source: Source
    steps: list[Step] = Field(min_length=1)
    target: Target
    contract: Contract
    expectations: Expectations
    processing: Processing


def quarantined(target: Target) -> Target:
    """The side location a `quarantine` run writes to instead of the declared target.

    Deliberately derived rather than declared: part 1 § 2 rule 4 says no field
    may require its author to know how the executor is implemented, and where
    the quarantine area lives is exactly that kind of knowledge. A domain
    engineer writes `on_failure: quarantine` and nothing else.

    The table name changes as well as the prefix. Same-named tables in two
    places is how a consumer eventually reads the quarantined copy by accident.
    """
    return target.model_copy(
        update={
            "table": f"{target.table}_quarantined",
            "location": target.location.rstrip("/") + "_quarantined/",
        }
    )
