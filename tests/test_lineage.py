"""What the run tells the outside world — part 2.

The events are checked against the OpenLineage spec's own typing rather than
against what looks reasonable, because "well-formed enough to send" and
"somewhere a consumer will look" are different bars and only the second one
matters. `docs/post-corrections.md` § 1 is the place the published article
cleared the first and not the second — which is what these tests were written to
catch, and did.
"""

from __future__ import annotations

import json
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
    `facets` produces a well-formed event whose assertion results nothing reads.
    """
    assert issubclass(DataQualityAssertionsDatasetFacet, InputDatasetFacet)

    events = emitted()
    carriers = [
        d
        for e in events
        for d in e["inputs"]
        if "dataQualityAssertions" in (d.get("inputFacets") or {})
    ]
    assert len(carriers) == 1, "the result should ride on exactly one dataset"

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


def test_provenance_names_the_release_that_published_what_the_run_read(emitted):
    """The chain past the pipeline's own commit, to the application change.

    `descriptor_git_commit_signed` reaches the commit that authorised the
    pipeline. These two reach the release that set the shape it read, which is
    the half a published number was missing.
    """
    facet = emitted()[0]["run"]["facets"]["cordata_provenance"]
    assert facet["source_published_by"] == "card-ledger"
    assert facet["source_published_release"] == "v4.11.0"


def test_an_unattributed_version_names_nobody_rather_than_guessing(monkeypatch, tmp_path):
    """What a Glue version written by a crawler looks like from here.

    Reporting the previous release instead would name a team that did not make
    the change, which is worse than saying nothing. Seeded into its own root
    rather than the session's, because stripping attribution from the shared
    catalog would make every later test fail for the wrong reason.
    """
    from pipeline_runtime import catalog

    from .conftest import _seed, environment

    root = tmp_path / "unattributed"
    root.mkdir()
    _seed(root)
    log = tmp_path / "lineage.ndjson"
    for key, value in {**environment(root), "CORDATA_LINEAGE_OUT": str(log)}.items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)

    with catalog.connect() as con:
        con.execute('DELETE FROM _catalog.versions WHERE "table" = ?', ["transactions"])

    run(FRAUD, str(uuid.uuid4()))

    facet = json.loads(log.read_text().splitlines()[0])["run"]["facets"]["cordata_provenance"]
    assert facet.get("source_published_by") is None
    assert facet.get("source_published_release") is None
    assert facet["source_schema_version"] == 7, "the version itself is still known"


def test_a_run_that_read_nothing_claims_no_release(seeded, monkeypatch, tmp_path):
    """A run that died on drift touched no version, so it has none to name."""
    from pipeline_runtime.errors import SchemaDrift

    from .conftest import _seed, environment

    root = tmp_path / "drifted"
    root.mkdir()
    _seed(root, drift=True)
    log = tmp_path / "lineage.ndjson"
    for key, value in {**environment(root), "CORDATA_LINEAGE_OUT": str(log)}.items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)

    with pytest.raises(SchemaDrift):
        run(FRAUD, str(uuid.uuid4()))

    facet = json.loads(log.read_text().splitlines()[0])["run"]["facets"]["cordata_provenance"]
    assert facet.get("source_published_by") is None
    assert facet.get("source_published_release") is None
    assert facet["source_schema_version"] == 7, "what it pinned, not what it read"


def test_a_blocked_publish_still_names_the_release_it_read(env, events, scenario):
    """It got as far as validating, so it did read a version.

    The drift case above is the one with nothing to name; a run that read the
    source and then refused to publish is not, and an event that dropped the
    release here would lose the attribution exactly when someone is
    investigating.
    """
    from pipeline_runtime.errors import PublishBlocked

    from .conftest import IMPOSSIBLE

    descriptor = scenario(FRAUD, expectations=IMPOSSIBLE)
    with pytest.raises(PublishBlocked):
        run(descriptor, str(uuid.uuid4()))

    facet = events()[0]["run"]["facets"]["cordata_provenance"]
    assert facet["source_published_by"] == "card-ledger"
    assert facet["source_published_release"] == "v4.11.0"


def test_the_facet_matches_the_schema_it_publishes(emitted):
    """`schemas/provenance.json` is the contract the facet's `_schemaURL` points
    at, so a field on one and not the other is a broken promise to a consumer.

    Equality here, not containment: this run reads an attributed version, so
    every declared field has a value to carry. Fields whose value is unknown
    are absent rather than null, because the OpenLineage client drops them on
    the way out — which the schema says.
    """
    from .paths import REPO

    published = json.loads((REPO / "schemas/provenance.json").read_text())["allOf"][1]
    emitted_keys = {k for k in emitted()[0]["run"]["facets"]["cordata_provenance"] if k[0] != "_"}

    assert emitted_keys == set(published["properties"])
    assert set(published["required"]) <= emitted_keys


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
