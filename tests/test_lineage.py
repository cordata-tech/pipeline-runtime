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


def test_the_commit_is_also_in_the_standard_facet(emitted):
    """A consumer that does not know `cordata_provenance` still finds the commit.

    `SourceCodeLocationJobFacet` is where OpenLineage puts this, and `qedro`
    read only that facet, so a run from a checkout reported a commit nothing
    could reach (#13).
    """
    event = emitted()[0]
    location = event["job"]["facets"]["sourceCodeLocation"]
    provenance = event["run"]["facets"]["cordata_provenance"]

    assert location["type"] == "git"
    assert location["version"] == provenance["descriptor_git_commit"]
    assert location["path"] == provenance["descriptor_path"]
    assert location["repoUrl"] == "https://github.com/cordata-tech/pipeline-runtime"
    assert location["url"] == location["repoUrl"]


def test_the_two_facets_cannot_disagree_about_the_commit(emitted):
    """One `git log` per run, read by both facets.

    Two fields naming the same commit is the cost of keeping
    `descriptor_git_commit` where `descriptor_git_commit_signed` can judge it,
    so what has to be true is that they are the same value — asserted across
    every event, since the job facet travels on the assertion event too.
    """
    for event in emitted():
        location = event["job"]["facets"]["sourceCodeLocation"]
        provenance = event["run"]["facets"].get("cordata_provenance")
        if provenance is not None:
            assert location["version"] == provenance["descriptor_git_commit"]


def test_the_standard_facet_reaches_the_validation_event_too(emitted):
    """Inherited rather than rebuilt, which is what #9 made true.

    Asserted because nothing else would notice if a later change went back to
    building the validation job from scratch: the facet would quietly stop
    travelling, exactly as `processing` did.
    """
    locations = [e["job"]["facets"]["sourceCodeLocation"] for e in emitted()]
    assert len(locations) > 1
    assert all(location == locations[0] for location in locations)


def test_no_source_code_location_when_the_run_can_name_no_repository(env, events, scenario):
    """Absent rather than partial, which is #11's rule again.

    A copied domain is in no work tree, so there is no commit and no remote.
    Half a facet — a `type` and a `url` with no version — would claim a
    location nobody can resolve.
    """
    run(scenario(FRAUD), str(uuid.uuid4()))

    for event in events():
        assert "sourceCodeLocation" not in event["job"]["facets"]
        assert "processing" in event["job"]["facets"], "the other job facet still travels"


@pytest.mark.parametrize(
    ("configured", "emitted_url"),
    [
        ("https://github.com/org/repo.git", "https://github.com/org/repo"),
        ("https://github.com/org/repo", "https://github.com/org/repo"),
        # What a CI checkout leaves in the config, and the reason this function
        # exists rather than the raw remote being emitted.
        (
            "https://x-access-token:ghp_secret@github.com/org/repo.git",
            "https://github.com/org/repo",
        ),
        ("https://user@gitlab.example.com/group/repo.git", "https://gitlab.example.com/group/repo"),
        ("git@github.com:org/repo.git", "https://github.com/org/repo"),
        ("ssh://git@github.com/org/repo.git", "https://github.com/org/repo"),
        # Not an http URL, and a path would put the host's layout back on the
        # wire — which is what #12 took off it.
        ("/srv/git/repo.git", None),
        ("file:///srv/git/repo.git", None),
        ("", None),
        (None, None),
    ],
)
def test_a_remote_is_emitted_as_an_http_url_naming_no_secret(configured, emitted_url):
    from pipeline_runtime.emit import _normalize_remote

    assert _normalize_remote(configured) == emitted_url


def test_the_descriptor_is_named_relative_to_its_work_tree(emitted):
    """Two checkouts of one commit have to report one string.

    The value is asserted rather than the key's presence, which is the gap that
    let an absolute path disagree with `schemas/provenance.json` through five
    releases (#12): the schema test compares key sets, so nothing held a field
    to what its own description promised.
    """
    facet = emitted()[0]["run"]["facets"]["cordata_provenance"]
    assert facet["descriptor_path"] == "example/domains/fraud/pipelines/transactions_scored.yml"


def test_a_descriptor_outside_a_work_tree_names_its_domain_and_not_the_host(
    env, events, scenario, tmp_path
):
    """What a run from an installed copy emits.

    `scenario` copies a domain into a directory that is not a work tree, which
    is the shape of a `pip install`ed example: the descriptor is real, the
    repository is not there, and the absolute path would be a site-packages
    directory under somebody's temporary files. The domain survives because it
    is part of the descriptor's identity; nothing above it does.
    """
    run(scenario(FRAUD), str(uuid.uuid4()))

    facet = events()[0]["run"]["facets"]["cordata_provenance"]
    assert facet["descriptor_path"] == "fraud/pipelines/transactions_scored.yml"
    assert str(tmp_path) not in facet["descriptor_path"]
    assert not facet["descriptor_path"].startswith("/")


def test_an_installed_descriptor_is_not_named_after_the_reader_s_repository(tmp_path):
    """The layout almost every Python project uses: `.venv` inside the repo.

    A reader who runs the demo in their own checkout has the packaged example
    sitting under a work tree that has nothing to do with it, and naming the
    descriptor relative to that work tree would emit a path describing where
    the wheel was unpacked. `site-packages` in the path is what settles it.
    """
    import subprocess

    from pipeline_runtime.emit import _descriptor_location

    project = tmp_path / "their-project"
    descriptor = (
        project
        / ".venv/lib/python3.13/site-packages/pipeline_runtime/example"
        / "domains/fraud/pipelines/transactions_scored.yml"
    )
    descriptor.parent.mkdir(parents=True)
    descriptor.touch()
    subprocess.run(["git", "init", "-q"], cwd=project, check=True)

    assert _descriptor_location(descriptor) == "fraud/pipelines/transactions_scored.yml"


def test_the_signature_verdict_never_travels_without_a_commit(env, events, scenario):
    """A verdict with nothing to attach it to is worse than no verdict.

    Until 0.2.4 a run from outside a checkout emitted no `descriptor_git_commit`
    — the client drops a None — beside `descriptor_git_commit_signed: false`,
    which a consumer can only read as a commit reported unsigned. `qedro` read
    it exactly that way. The scenario fixture copies the domain into a
    directory that is not a repository, which is what an installed copy looks
    like from here (#11).
    """
    run(scenario(FRAUD), str(uuid.uuid4()))
    outside = events()[0]["run"]["facets"]["cordata_provenance"]
    assert "descriptor_git_commit" not in outside
    assert "descriptor_git_commit_signed" not in outside
    assert outside["descriptor_sha256"], "which bytes ran is still known"

    run(FRAUD, str(uuid.uuid4()))
    named = [
        e["run"]["facets"]["cordata_provenance"]
        for e in events()
        if "cordata_provenance" in e["run"]["facets"]
    ][-1]
    assert named["descriptor_git_commit"]
    assert isinstance(named["descriptor_git_commit_signed"], bool), (
        "whether it verified depends on the signer's keys; that it is reported does not"
    )


def test_a_dataset_carries_the_same_classification_in_every_event_naming_it(emitted):
    """The target appears twice in a run — written, then tested.

    It arrived classified on the terminal event's output and unclassified on
    the assertion event's input, so a consumer counting datasets per activity
    called its own output unclassified (#10). Asserted as the general property
    rather than on the one pair, so a third event cannot reintroduce the split.
    """
    seen: dict[tuple[str, str], list] = {}
    for event in emitted():
        for ref in (*event["inputs"], *event["outputs"]):
            seen.setdefault((ref["namespace"], ref["name"]), []).append(ref["facets"].get("tags"))

    repeated = {ref: facets for ref, facets in seen.items() if len(facets) > 1}
    assert repeated, "the target is written and then tested, so it appears in two events"
    for ref, facets in repeated.items():
        assert all(facet == facets[0] for facet in facets), ref
        assert facets[0] is not None, ref


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


def test_every_event_a_run_emits_carries_the_same_processing_facet(emitted):
    """The validation job is the pipeline's job under a longer name.

    Checking a pipeline's output is part of the same processing activity — the
    purpose and lawful basis that justify writing the rows justify checking
    them — so the assertion event inherits the facet rather than declaring
    nothing. Built from scratch, as it was until 0.2.3, the `.validate` job
    carried no facets at all, and an Art. 30 record rebuilt from the events
    listed it as an activity with no purpose and no legal basis (#9).
    """
    pipeline = load(CLAIMS)
    events = emitted(CLAIMS)

    assert len(events) > 1
    assert {e["job"]["name"] for e in events} == {
        pipeline.metadata.name,
        f"{pipeline.metadata.name}.validate",
    }
    assert len({e["job"]["namespace"] for e in events}) == 1

    facets = [e["job"]["facets"]["processing"] for e in events]
    assert all(f == facets[0] for f in facets)
    assert facets[0]["purpose"] == pipeline.processing.purpose
    assert facets[0]["legal_basis"] == pipeline.processing.legal_basis


def test_the_resolved_tags_travel_on_the_dataset_they_classify(emitted):
    """Part 1 § 6's tags, resolved against the ontology and then emitted.

    `TagsDatasetFacet` is a plain `DatasetFacet`, so it belongs in `facets` —
    the opposite of the quality facet in `docs/post-corrections.md` § 1, and
    worth asserting rather than assuming, since the two read alike in a payload.
    """
    from openlineage.client.generated.base import DatasetFacet
    from openlineage.client.generated.tags_dataset import TagsDatasetFacet

    assert issubclass(TagsDatasetFacet, DatasetFacet)

    pipeline = load(FRAUD)
    facet = emitted()[0]["outputs"][0]["facets"]["tags"]
    assert {t["key"]: t["value"] for t in facet["tags"]} == pipeline.contract.lf_tags
    assert {t["source"] for t in facet["tags"]} == {"CORDATA_PIPELINE_RUNTIME"}


def test_a_pipeline_does_not_classify_the_table_it_read(emitted):
    """`contract.lf_tags` describe what this pipeline publishes.

    Copying them onto the input would assert a classification for someone
    else's table that nobody checked. The fraud source carries only its owning
    domain in the catalog, so that is all its facet says.
    """
    read = emitted()[0]["inputs"][0]["facets"]["tags"]["tags"]
    assert {t["key"]: t["value"] for t in read} == {"domain": "fraud"}


def test_a_source_the_catalog_has_classified_carries_that_classification(monkeypatch, tmp_path):
    """When the table read *is* another pipeline's product, the catalog knows.

    Seeded into its own root, with the classification written the way
    `register` writes one at publish, because the shared session catalog has no
    tags for the fraud source and stamping some would leak into every test
    after this one.
    """
    from pipeline_runtime import catalog

    from .conftest import _seed, environment

    root = tmp_path / "classified"
    root.mkdir()
    _seed(root)
    log = tmp_path / "lineage.ndjson"
    for key, value in {**environment(root), "CORDATA_LINEAGE_OUT": str(log)}.items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)

    upstream = {"sensitivity": "high", "residency": "eu", "retention": "7y"}
    with catalog.connect() as con:
        con.executemany(
            "INSERT INTO _catalog.lf_tags VALUES ('fraud_raw', 'transactions', ?, ?)",
            list(upstream.items()),
        )

    run(FRAUD, str(uuid.uuid4()))

    event = json.loads(log.read_text().splitlines()[0])
    read = event["inputs"][0]["facets"]["tags"]["tags"]
    assert {t["key"]: t["value"] for t in read} == {**upstream, "domain": "fraud"}

    written = event["outputs"][0]["facets"]["tags"]["tags"]
    assert {t["key"] for t in written} == set(load(FRAUD).contract.lf_tags), (
        "the output keeps its own contract's tags rather than inheriting the source's"
    )


def test_a_quarantined_copy_keeps_the_classification(env, events, scenario):
    """Failing a quality rule does not make the data less personal, and the
    side copy is the one somebody eventually opens without the descriptor in
    front of them."""
    from .conftest import IMPOSSIBLE

    descriptor = scenario(CLAIMS, expectations=IMPOSSIBLE)
    run(descriptor, str(uuid.uuid4()))

    output = events()[0]["outputs"][0]
    assert output["name"].endswith("_quarantined")
    assert {t["key"]: t["value"] for t in output["facets"]["tags"]["tags"]} == load(
        CLAIMS
    ).contract.lf_tags


def test_the_namespace_is_the_domain_not_the_bucket(emitted):
    pipeline = load(FRAUD)
    event = emitted()[0]
    assert event["job"]["namespace"] == f"cordata.{pipeline.metadata.domain}"
    assert event["inputs"][0]["namespace"] == f"cordata://{pipeline.metadata.domain}"


def test_a_table_two_domains_read_is_one_dataset(emitted):
    """#7: the input is named by the domain that owns it, not the one reading it.

    Fraud and finance both read `fraud_raw.transactions`. OpenLineage takes
    namespace plus name as a dataset's identity, so naming the input by the
    reader made it two datasets, and a lineage graph lost finance's read.
    """
    from .paths import FINANCE

    fraud = emitted(FRAUD)[0]["inputs"][0]
    finance = emitted(FINANCE)[0]["inputs"][0]
    assert (fraud["namespace"], fraud["name"]) == ("cordata://fraud", "fraud_raw.transactions")
    assert (finance["namespace"], finance["name"]) == (fraud["namespace"], fraud["name"])


def test_a_source_with_no_recorded_owner_is_attributed_to_the_reader_and_says_so(
    monkeypatch, tmp_path
):
    """Without a `domain` tag the run knows only its own domain. It uses that
    rather than failing, and says so in the trace, because a silent fallback is
    exactly how the split in #7 went unnoticed."""
    import io

    from pipeline_runtime import catalog
    from pipeline_runtime.trace import Trace

    from .conftest import _seed, environment
    from .paths import FINANCE

    root = tmp_path / "unowned"
    root.mkdir()
    _seed(root)
    log = tmp_path / "lineage.ndjson"
    for key, value in {**environment(root), "CORDATA_LINEAGE_OUT": str(log)}.items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)
    with catalog.connect() as con:
        con.execute("DELETE FROM _catalog.lf_tags WHERE tag_key = 'domain'")

    out = io.StringIO()
    run_id = str(uuid.uuid4())
    run(FINANCE, run_id, Trace(run_id, stream=out))

    event = json.loads(log.read_text().splitlines()[0])
    assert event["inputs"][0]["namespace"] == "cordata://finance"
    assert "no domain tag in the catalog; attributed to finance" in out.getvalue()


def test_input_and_output_schemas_use_the_same_type_names(emitted):
    """#7: outputs carried pandas dtype names (`STR`, `FLOAT64`) while inputs
    carried the catalog's, so a column could not be followed from one to the
    other by type."""
    event = emitted()[0]
    read = {f["name"]: f["type"] for f in event["inputs"][0]["facets"]["schema"]["fields"]}
    written = {f["name"]: f["type"] for f in event["outputs"][0]["facets"]["schema"]["fields"]}
    for column in ("tx_id", "iban", "amount_eur", "booked_at"):
        assert written[column] == read[column], column
    assert written["fraud_score"] == "DOUBLE"


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
