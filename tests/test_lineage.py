"""What the run tells the outside world — part 2.

The events are checked against the OpenLineage spec's own typing rather than
against what looks reasonable, because "well-formed enough to send" and
"somewhere a consumer will look" are different bars and only the second one
matters. See `docs/post-corrections.md` for the one place the published article
clears the first and not the second.
"""

from __future__ import annotations

import uuid

import pytest
from openlineage.client.generated.base import InputDatasetFacet, RunFacet
from openlineage.client.generated.data_quality_assertions_dataset import (
    DataQualityAssertionsDatasetFacet,
)

from pipeline_runtime import __version__
from pipeline_runtime.run import load, run

from .paths import CLAIMS, FRAUD

PRODUCER = f"https://github.com/cordata-tech/pipeline-runtime/tree/{__version__}"


@pytest.fixture
def emitted(env, events):
    def go(descriptor=FRAUD):
        run(descriptor, str(uuid.uuid4()))
        return events()

    return go


def test_every_event_carries_producer_and_schema_url(emitted):
    """`BaseEvent` requires both, and a facet that omits `_producer` cannot be
    attributed to the thing that made the claim."""
    for event in emitted():
        assert event["producer"] == PRODUCER
        assert event["schemaURL"].startswith("https://openlineage.io/spec/")
        for facet in event["run"]["facets"].values():
            assert facet["_producer"] and facet["_schemaURL"]


def test_one_run_id_spans_every_event_it_produced(emitted):
    """Part 2 § 4: facets accumulate against a runId. Several events per run is
    the normal case, which is why idempotency keys on the payload and not on
    the run."""
    events = emitted()
    assert len({e["run"]["runId"] for e in events}) == 1
    assert len(events) > 1


def test_exactly_one_terminal_event(emitted):
    terminal = [e for e in emitted() if e["eventType"] in ("COMPLETE", "FAIL", "ABORT")]
    assert len(terminal) == 1


def test_assertions_go_where_the_spec_says_they_go(emitted):
    """`DataQualityAssertionsDatasetFacet` is typed as an `InputDatasetFacet`.

    Its only standard home is `InputDataset.inputFacets`, and both reference
    integrations — dbt and Great Expectations, in
    `openlineage-integration-common` — emit it there, against an event whose
    input is the dataset that was tested. Putting it on an output dataset's
    `facets` produces a well-formed event whose verdict nothing reads.
    """
    assert issubclass(DataQualityAssertionsDatasetFacet, InputDatasetFacet)

    events = emitted()
    carriers = [
        d
        for e in events
        for d in e["inputs"]
        if "dataQualityAssertions" in (d.get("inputFacets") or {})
    ]
    assert len(carriers) == 1, "the verdict should ride on exactly one dataset"

    assert not [
        d
        for e in events
        for d in e["outputs"]
        if "dataQualityAssertions" in (d.get("facets") or {})
    ], "an output dataset's `facets` is not where a consumer looks for assertions"


def test_the_tested_dataset_is_the_one_that_was_written(emitted):
    pipeline = load(FRAUD)
    carrier = next(
        d
        for e in emitted()
        for d in e["inputs"]
        if "dataQualityAssertions" in (d.get("inputFacets") or {})
    )
    assert carrier["name"] == pipeline.target.fqn


def test_every_expectation_in_the_suite_reaches_the_event(emitted):
    """Per-assertion, not a single pass/fail: an auditor asking *which* rule
    failed gets an answer from the event rather than from whoever has the logs."""
    pipeline = load(FRAUD)
    declared = len(
        __import__("yaml").safe_load((FRAUD.parents[1] / pipeline.expectations.suite).read_text())[
            "expectations"
        ]
    )
    facet = next(
        d["inputFacets"]["dataQualityAssertions"]
        for e in emitted()
        for d in e["inputs"]
        if "dataQualityAssertions" in (d.get("inputFacets") or {})
    )
    assert len(facet["assertions"]) == declared


def test_provenance_pins_the_declaration_that_ran(emitted):
    pipeline = load(FRAUD)
    facet = emitted()[0]["run"]["facets"]["cordata_provenance"]

    assert (
        facet["descriptor_sha256"] == __import__("hashlib").sha256(FRAUD.read_bytes()).hexdigest()
    )
    assert facet["source_table"] == pipeline.source.fqn
    assert facet["source_schema_version"] == pipeline.source.schema_version
    assert facet["executor_version"] == f"pipeline-runtime {__version__}"
    assert facet["step_params"]["score"]["model_version"] == "2026-07-fraud-v3"


def test_provenance_is_a_run_facet_not_a_job_facet(emitted):
    """It describes one execution — which bytes ran, against which commit — so
    it cannot be a job facet, which describes the pipeline across all runs."""
    from pipeline_runtime.emit import ProvenanceRunFacet

    assert issubclass(ProvenanceRunFacet, RunFacet)
    assert "cordata_provenance" in emitted()[0]["run"]["facets"]


def test_art_30_fields_travel_as_a_job_facet(emitted):
    """`purpose` and `legal_basis` are properties of the pipeline, not of one
    execution of it — and they are what lets the RoPA maintain itself."""
    pipeline = load(CLAIMS)
    facet = emitted(CLAIMS)[0]["job"]["facets"]["processing"]
    assert facet["purpose"] == pipeline.processing.purpose
    assert facet["legal_basis"] == pipeline.processing.legal_basis


def test_the_namespace_is_the_domain_not_the_bucket(emitted):
    pipeline = load(FRAUD)
    event = emitted()[0]
    assert event["job"]["namespace"] == f"cordata.{pipeline.metadata.domain}"
    assert event["inputs"][0]["namespace"] == f"cordata://{pipeline.metadata.domain}"


def test_datasets_report_the_shape_the_run_actually_saw(emitted):
    """From the catalog and the frame, never echoed back from the descriptor —
    a schema facet that restates the declaration cannot be wrong, which makes
    it worthless."""
    event = emitted()[0]
    fields = {f["name"] for f in event["inputs"][0]["facets"]["schema"]["fields"]}
    assert {"tx_id", "iban", "amount_eur"} <= fields

    produced = {f["name"] for f in event["outputs"][0]["facets"]["schema"]["fields"]}
    assert "fraud_score" in produced, "the column the pipeline exists to produce"
    assert "status" not in produced, "filtered out by the first step, so not in the output"
