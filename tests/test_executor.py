"""The claim: one generic executor, an arbitrary number of pipelines as data.

Both descriptors here are the ones published in part 1 § 2, byte for byte. They
differ in four values — `source.kind`, `target.kind`, `expectations.on_failure`,
`processing.legal_basis` — and nothing in the executor changes to run either.
"""

from __future__ import annotations

import uuid

import pandas as pd
import pytest

from pipeline_runtime import catalog
from pipeline_runtime.backends.local import local_path
from pipeline_runtime.run import load, run
from tests.paths import CLAIMS, FRAUD

DESCRIPTORS = [FRAUD, CLAIMS]


@pytest.mark.parametrize("descriptor", DESCRIPTORS, ids=lambda p: p.stem)
def test_runs_end_to_end(env, events, descriptor):
    run(descriptor, str(uuid.uuid4()))

    pipeline = load(descriptor)
    written = list(local_path(pipeline.target.location).rglob("*.parquet"))
    assert written, f"{pipeline.metadata.name} wrote nothing"
    assert [e["eventType"] for e in events()] == ["COMPLETE", "OTHER"]


def test_the_two_descriptors_exercise_different_registries():
    fraud, claims = load(FRAUD), load(CLAIMS)
    assert fraud.source.kind != claims.source.kind
    assert fraud.target.kind != claims.target.kind
    assert fraud.expectations.on_failure != claims.expectations.on_failure
    assert fraud.processing.legal_basis != claims.processing.legal_basis


def test_the_write_registers_the_contract_it_was_executed_against(env):
    """Part 1 § 4: subscribers read the same contract the pipeline ran under.

    Only holds if publishing the data and publishing its tags are one act — so
    the tags have to be in the catalog the moment the parquet is, with no
    crawler in between.
    """
    pipeline = load(FRAUD)
    run(FRAUD, str(uuid.uuid4()))

    with catalog.connect(read_only=True) as con:
        tags = dict(
            con.execute(
                "SELECT tag_key, tag_value FROM _catalog.lf_tags "
                'WHERE database = ? AND "table" = ?',
                [pipeline.target.database, pipeline.target.table],
            ).fetchall()
        )
    assert tags == pipeline.contract.lf_tags


def test_partitioning_follows_the_declaration(env):
    pipeline = load(FRAUD)
    run(FRAUD, str(uuid.uuid4()))

    out = local_path(pipeline.target.location)
    partitions = [d.name for d in out.iterdir() if d.is_dir()]
    assert partitions, "partition_by was declared but nothing was partitioned"
    assert all(p.startswith("scored_date=") for p in partitions), partitions


def test_step_params_reach_the_query(env):
    """`model_version` is declared in the descriptor and bound into score.sql.

    Worth asserting because the alternative — string interpolation — would pass
    every test here and still be the wrong mechanism.
    """
    pipeline = load(FRAUD)
    run(FRAUD, str(uuid.uuid4()))

    out = local_path(pipeline.target.location)
    frame = pd.concat([pd.read_parquet(f) for f in out.rglob("*.parquet")])
    declared = pipeline.steps[1].params["model_version"]
    assert set(frame["model_version"].unique()) == {declared}


def test_cdc_collapse_drops_deletes_and_keeps_the_latest_update(env):
    """`read_dms_landing` collapses the CDC log before anything downstream sees it.

    The fixture lands inserts, updates and deletes out of order across four
    batches, so a reader that concatenated files would over-report and one that
    ignored `Op` would resurrect deleted claims.
    """
    from pipeline_runtime.backends.local import collapse_cdc

    raw = pd.DataFrame(
        {
            "claim_id": ["a", "a", "b", "c", "c"],
            "amount_eur": [1.0, 2.0, 5.0, 9.0, 9.0],
            "Op": ["I", "U", "I", "I", "D"],
            "_dms_seq": [4, 9, 1, 2, 7],
        }
    )
    out = collapse_cdc(raw, keys=["claim_id"]).set_index("claim_id")

    assert set(out.index) == {"a", "b"}, "the deleted claim came back"
    assert out.loc["a", "amount_eur"] == 2.0, "the earlier insert won over the later update"
    assert "Op" not in out.columns and "_dms_seq" not in out.columns
