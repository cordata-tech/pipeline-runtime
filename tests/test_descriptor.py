"""What the schema rejects, and why each rejection was worth a rule.

Every case here is something part 1 § 3 promises the model catches at
pull-request time rather than at 3am. A rejection that stops being enforced is
not a smaller schema — it is a descriptor field that silently stops meaning
anything, which is the failure mode `extra="forbid"` exists to prevent.
"""

from __future__ import annotations

import copy

import pytest
import yaml
from pydantic import ValidationError

from pipeline_runtime.descriptor import Descriptor, quarantined
from pipeline_runtime.run import load

from .paths import CLAIMS, FRAUD

PUBLISHED = [FRAUD, CLAIMS]


@pytest.mark.parametrize("path", PUBLISHED, ids=lambda p: p.stem)
def test_published_descriptors_validate(path):
    load(path)


REJECTIONS = [
    ("typo'd key is a failed PR", lambda d: d["expectations"].update({"on_fail": "block"})),
    (
        "invented legal_basis cannot reach the RoPA",
        lambda d: d["processing"].update({"legal_basis": "because-we-can"}),
    ),
    (
        "step with both query_file and module",
        lambda d: d["steps"][0].update({"module": "domains.fraud.x"}),
    ),
    ("step with neither query_file nor module", lambda d: d["steps"][0].pop("query_file", None)),
    ("schema_version below 1", lambda d: d["source"].update({"schema_version": 0})),
    ("schema_version that is not a number", lambda d: d["source"].update({"schema_version": "s"})),
    (
        "metadata.name with spaces and capitals",
        lambda d: d["metadata"].update({"name": "Tx Daily"}),
    ),
    ("no steps at all", lambda d: d.update({"steps": []})),
    ("an apiVersion nobody registered", lambda d: d.update({"apiVersion": "cordata.tech/v2"})),
    ("a kind nobody registered", lambda d: d.update({"kind": "StreamingPipeline"})),
]


@pytest.mark.parametrize("label,mutate", REJECTIONS, ids=[c[0] for c in REJECTIONS])
def test_model_rejects_what_the_post_claims(label, mutate):
    base = yaml.safe_load(FRAUD.read_text())
    mutate(base)
    # Deliberately narrow: catching bare Exception here would let a broken model
    # masquerade as correct rejection.
    with pytest.raises(ValidationError):
        Descriptor.model_validate(base)


def test_a_descriptor_cannot_be_mutated_after_load():
    """`frozen=True`. A descriptor that can be edited mid-run is a descriptor
    whose sha256 on the lineage event describes something that no longer ran."""
    pipeline = load(FRAUD)
    with pytest.raises(ValidationError):
        pipeline.source.schema_version = 8  # type: ignore[misc]


def test_whitespace_does_not_fork_an_identifier():
    raw = yaml.safe_load(FRAUD.read_text())
    raw["source"]["database"] = "fraud_raw "
    assert Descriptor.model_validate(raw).source.fqn == "fraud_raw.transactions"


def test_declared_paths_stay_inside_the_domain():
    """`sql/…` and `expectations/…` are domain-root relative.

    Regression guard for a real bug found in review, where the suite resolved
    against the descriptor's own directory and pointed one level too deep.
    """
    for descriptor in PUBLISHED:
        pipeline = load(descriptor)
        root = descriptor.parents[1]
        assert (root / pipeline.expectations.suite).is_file()
        for step in pipeline.steps:
            if step.query_file:
                assert not step.query_file.startswith(("/", "../"))
                assert (root / step.query_file).is_file()


def test_the_source_names_its_table_exactly_once():
    """Regression guard: an earlier draft carried a `catalog://db/table@vN` URI
    alongside `database` and `table`, restating both."""
    for descriptor in PUBLISHED:
        source = yaml.safe_load(descriptor.read_text())["source"]
        restated = [
            k
            for k, v in source.items()
            if k not in ("database", "table")
            and isinstance(v, str)
            and (source["database"] in v or source["table"] in v)
        ]
        assert not restated, f"{descriptor.stem}: source table restated in {restated}"


def test_quarantine_moves_both_the_table_and_the_prefix():
    """Same-named tables in two places is how a consumer eventually reads the
    quarantined copy by accident."""
    target = load(FRAUD).target
    side = quarantined(target)
    assert side.table != target.table
    assert side.location != target.location
    assert side.database == target.database


def test_a_domain_engineer_never_declares_where_quarantine_lives():
    """Part 1 § 2 rule 4: no field may require its author to know how the
    executor is implemented. The quarantine location is derived, so there is no
    field to get wrong — and adding one later would be the regression."""
    fields = set(Descriptor.model_json_schema()["$defs"]["Target"]["properties"])
    assert not {f for f in fields if "quarantine" in f}


def test_an_unquoted_param_is_rejected_rather_than_coerced():
    """`params: {lookback_days: "30"}` and never `30`.

    The value crosses into SQL as a bound parameter and onto the lineage event
    as provenance. Coercing `30` to `"30"` would be the friendlier behaviour and
    the wrong one: it makes the quoted and unquoted descriptors indistinguishable
    downstream while leaving them different in the diff a reviewer reads.
    Rejecting means the author fixes the YAML once, at pull-request time.
    """
    raw = copy.deepcopy(yaml.safe_load(FRAUD.read_text()))
    raw["steps"][1]["params"]["model_version"] = 7
    with pytest.raises(ValidationError):
        Descriptor.model_validate(raw)
