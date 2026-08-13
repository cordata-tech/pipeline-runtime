"""The declared expectation suite — part 1 § 5, and the result part 2 § 3 emits.

Great Expectations validates a *frame*, not a warehouse, which is the property
part 1 § 5 reaches for it over dbt tests: the same suite shape works behind any
reader, at any step, in any pipeline. Its results arrive as structured objects
rather than log lines, which is what lets one uniform assertion result reach `emit()`
regardless of what ran.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import great_expectations as gx
import pandas as pd
import yaml
from openlineage.client.facet_v2 import data_quality_assertions_dataset as dqa


@dataclass(frozen=True)
class Assertion:
    """One expectation's outcome, in the vocabulary the lineage facet uses."""

    assertion: str
    success: bool
    column: str | None = None
    expected: str | None = None
    actual: str | None = None
    detail: str = ""


@dataclass(frozen=True)
class Validation:
    """The uniform assertion result. Every failure path in the executor hands one
    of these to `emit()`, or `None` if it never got as far as running the suite."""

    suite_name: str
    assertions: tuple[Assertion, ...]

    @property
    def success(self) -> bool:
        return all(a.success for a in self.assertions)

    @property
    def passed(self) -> int:
        return sum(a.success for a in self.assertions)

    @property
    def failed(self) -> int:
        return len(self.assertions) - self.passed

    @property
    def summary(self) -> str:
        return f"{self.passed} passed, {self.failed} failed"

    def as_facet(self) -> dqa.DataQualityAssertionsDatasetFacet:
        """The facet part 2 § 3 emits, per assertion rather than per suite.

        An auditor asking *which* rule failed on the 4th of March gets an answer
        from the event itself rather than from whoever still has the logs — and
        `expected`/`actual` are what make a distributional failure legible
        without going back to the data: 0.19 against a bound of 0.15 says how
        far the model moved, where a bare `false` says only that it did.
        """
        return dqa.DataQualityAssertionsDatasetFacet(
            assertions=[
                dqa.Assertion(
                    assertion=a.assertion,
                    success=a.success,
                    column=a.column,
                    expected=a.expected,
                    actual=a.actual,
                )
                for a in self.assertions
            ]
        )


@dataclass(frozen=True)
class Suite:
    name: str
    expectations: tuple[dict, ...]


def load_suite(path: Path) -> Suite:
    """A versioned artefact the pipeline *reads* — an inbound metadata stream,
    not a test file that happens to live nearby."""
    if not path.is_file():
        raise FileNotFoundError(f"expectation suite not found: {path}")
    raw = yaml.safe_load(path.read_text())
    domain_root = path.parents[1]
    return Suite(
        name=raw["suite_name"],
        expectations=tuple(_resolve_baselines(e, domain_root) for e in raw["expectations"]),
    )


def _resolve_baselines(spec: dict, domain_root: Path) -> dict:
    """Expand `partition_object: <name>` into the stored baseline it names.

    A distributional expectation is only meaningful against a reference, and
    that reference is a domain artefact with the same properties as the suite:
    versioned, reviewable, and changed by pull request. Naming it rather than
    inlining a hundred bin weights is what keeps the suite readable — and it
    means "we re-baselined the fraud score" shows up in a diff with an author on
    it, which is exactly the moment governance wants a record of.
    """
    kwargs = spec.get("kwargs", {})
    ref = kwargs.get("partition_object")
    if not isinstance(ref, str):
        return spec

    baseline = domain_root / "baselines" / f"{ref}.json"
    if not baseline.is_file():
        raise FileNotFoundError(
            f"expectation {spec['type']} references baseline {ref!r}, but {baseline} does not exist"
        )
    return {**spec, "kwargs": {**kwargs, "partition_object": json.loads(baseline.read_text())}}


def validate(frame: pd.DataFrame, suite: Suite) -> Validation:
    """Rules in, result out.

    Runs at the pipeline boundary — after the declared steps, before the write.
    That seam is what makes `on_failure: block_publish` mean something: failing
    there means nothing is published and no consumer sees the bad batch.
    """
    context = gx.get_context(mode="ephemeral")
    batch = (
        context.data_sources.add_pandas("frame")
        .add_dataframe_asset("frame")
        .add_batch_definition_whole_dataframe("batch")
        .get_batch(batch_parameters={"dataframe": frame})
    )

    results = []
    for spec in suite.expectations:
        expectation = _build(spec)
        # An expectation that raises — a regex against a numeric column, a
        # partition_object naming a baseline that does not exist — is a failed
        # assertion, not a crashed run. The suite is domain-authored input; bad
        # input has to produce a result the same way bad data does, or a typo
        # in the suite silently takes the pipeline down with no event to show
        # for it.
        try:
            outcome = batch.validate(expectation)
            success = bool(outcome.success)
            detail = "" if success else _describe(outcome)
            actual = _observed(outcome)
        except Exception as exc:  # noqa: BLE001 — a broken rule is a failed rule
            success, detail, actual = False, f"{type(exc).__name__}: {exc}", None

        results.append(
            Assertion(
                # The suite's own `type:` value, unshortened, so an assertion on
                # the event traces back to the exact line of the exact suite
                # that produced it.
                assertion=spec["type"],
                success=success,
                column=spec.get("kwargs", {}).get("column"),
                expected=_bound(spec),
                actual=actual,
                detail=detail,
            )
        )

    return Validation(suite_name=suite.name, assertions=tuple(results))


def _build(spec: dict):
    import great_expectations.expectations as gxe

    name = "".join(part.title() for part in spec["type"].split("_"))
    # GX spells the acronym upper-case in the class name; title() lower-cases it.
    name = name.replace("Kl", "KL")
    cls = getattr(gxe, name, None)
    if cls is None:
        raise ValueError(f"unknown expectation type in suite: {spec['type']}")
    return cls(**spec.get("kwargs", {}))


def _bound(spec: dict) -> str | None:
    """The threshold the suite declared, as a string, for the facet's `expected`.

    Only the scalar bounds. A `partition_object` is a hundred bin weights and
    belongs in the baseline file, not on every event that references it.
    """
    kwargs = spec.get("kwargs", {})
    for key in ("threshold", "value", "max_value", "value_set"):
        if key in kwargs:
            bounds = [str(kwargs[k]) for k in ("min_value", "max_value") if k in kwargs]
            return " to ".join(bounds) if bounds else str(kwargs[key])
    return None


def _observed(outcome) -> str | None:
    value = (outcome.result or {}).get("observed_value")
    return None if value is None else str(value)


def _describe(outcome) -> str:
    r = outcome.result or {}
    if "observed_value" in r:
        return f"observed {r['observed_value']}"
    if "unexpected_count" in r:
        return f"{r['unexpected_count']} unexpected of {r.get('element_count', '?')}"
    return "failed"
