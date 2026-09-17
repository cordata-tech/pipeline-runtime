"""`cordata.tech/v2`: both contracts named, nothing else changed.

The decision and its reasoning are on cordata-tech/pipeline-runtime#1. What these
tests hold is the part a later edit could quietly break — that v2 only moves
fields, so the executor still runs one shape and v1 still parses.
"""

from __future__ import annotations

import io
import uuid

import pytest
import yaml
from pydantic import ValidationError

from pipeline_runtime import descriptor_v2
from pipeline_runtime.run import load, parse, run
from pipeline_runtime.trace import Trace

from .paths import CLAIMS, FINANCE, FRAUD


def as_v2(v1_raw: dict) -> dict:
    """A v1 descriptor rewritten by hand the way a domain would migrate it."""
    raw = {k: v for k, v in v1_raw.items() if k != "contract"}
    source = {k: v for k, v in raw["source"].items() if k != "schema_version"}
    return {
        **raw,
        "apiVersion": "cordata.tech/v2",
        "source": {**source, "contract": {"schema_version": v1_raw["source"]["schema_version"]}},
        "target": {**raw["target"], "contract": v1_raw["contract"]},
    }


def test_the_v2_example_parses_as_v2():
    assert isinstance(parse(FINANCE), descriptor_v2.Descriptor)


@pytest.mark.parametrize("path", [FRAUD, CLAIMS], ids=lambda p: p.stem)
def test_migrating_a_published_descriptor_changes_nothing_the_executor_sees(path):
    """v2 moves three fields and adds none, so the v1 shape comes back exactly."""
    migrated = descriptor_v2.Descriptor.model_validate(as_v2(yaml.safe_load(path.read_text())))
    assert migrated.as_v1() == load(path)


def test_each_contract_sits_under_the_side_it_binds():
    pipeline = parse(FINANCE)
    assert pipeline.source.contract.schema_version == 7
    assert set(pipeline.target.contract.lf_tags) == {"sensitivity", "residency", "subject_type"}


REJECTIONS = [
    (
        "the v1 top-level contract block",
        lambda d: d.update({"contract": d["target"]["contract"]}),
    ),
    ("the v1 pin on the source itself", lambda d: d["source"].update({"schema_version": 7})),
    ("a source with no input contract", lambda d: d["source"].pop("contract")),
    ("a target with no output contract", lambda d: d["target"].pop("contract")),
    (
        "schema_version below 1",
        lambda d: d["source"]["contract"].update({"schema_version": 0}),
    ),
]


@pytest.mark.parametrize("label,mutate", REJECTIONS, ids=[c[0] for c in REJECTIONS])
def test_v2_rejects_the_v1_spellings_and_missing_contracts(label, mutate):
    raw = yaml.safe_load(FINANCE.read_text())
    mutate(raw)
    with pytest.raises(ValidationError):
        descriptor_v2.Descriptor.model_validate(raw)


def test_declared_paths_stay_inside_the_domain():
    pipeline = load(FINANCE)
    root = FINANCE.parents[1]
    assert (root / pipeline.expectations.suite).is_file()
    assert all((root / s.query_file).is_file() for s in pipeline.steps if s.query_file)


def test_the_trace_reports_the_version_the_file_declares(env, events):
    """Converted to v1 for execution, but the log says what was written."""
    out = io.StringIO()
    run(FINANCE, str(uuid.uuid4()), trace=Trace("0000", stream=out))
    assert "apiVersion v2 OK" in out.getvalue().splitlines()[0]
