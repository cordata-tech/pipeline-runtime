"""The producer-side check: a proposed version against the pins that read the table.

Pillar § 9 sells a change that would break a registered consumer failing the
producing application's build. These tests hold the two things that make that
true here: the consumer list comes from the descriptors themselves, and the exit
status is what a CI job reads.
"""

from __future__ import annotations

import io
import shutil

import pytest
import yaml

from pipeline_runtime.consumers import main

from .paths import CLAIMS, EXAMPLE, FINANCE, FRAUD

DOMAINS = EXAMPLE / "domains"
BREAKING = EXAMPLE / "proposals/card-ledger-v5.0.0.yml"
ADDITIVE = EXAMPLE / "proposals/card-ledger-v4.12.0.yml"


def check(*argv) -> tuple[int, str]:
    out = io.StringIO()
    code = main([str(a) for a in argv], out=out)
    return code, out.getvalue()


def blocks(text: str) -> dict[str, str]:
    """Each consumer's block, keyed by the status and name on its first line."""
    found = {}
    for block in text.split("\n\n")[1:-1]:
        status, name = block.split()[:2]
        found[name] = status
    return found


def test_a_breaking_change_fails_the_build_and_names_every_consumer(env):
    code, text = check(BREAKING, DOMAINS)

    assert code == 1, "an application's CI reads the exit status"
    assert blocks(text) == {
        "merchant-settlement-daily": "BREAKS",
        "transactions-scored-daily": "BREAKS",
    }
    assert "- merchant_id (VARCHAR, not null)   <- breaks" in text
    assert "owner fraud-data@example.com" in text and "owner finance-data@example.com" in text


def test_the_consumer_list_is_whatever_descriptors_read_the_table(env):
    """Claims reads another table, and the expectation suites are not descriptors."""
    _, text = check(BREAKING, DOMAINS)
    assert "2 descriptors under" in text
    assert str(CLAIMS) not in text and "claims-ingest" not in text


def test_a_pipeline_added_later_is_on_the_list_without_being_registered(env, tmp_path):
    shutil.copytree(DOMAINS, tmp_path / "domains")
    extra = tmp_path / "domains/finance/pipelines/merchant_settlement_weekly.yml"
    raw = yaml.safe_load(FINANCE.read_text())
    raw["metadata"]["name"] = "merchant-settlement-weekly"
    extra.write_text(yaml.safe_dump(raw))

    _, text = check(BREAKING, tmp_path / "domains")
    assert "merchant-settlement-weekly" in blocks(text)


def test_an_additive_change_passes_and_lists_who_has_to_bump(env):
    code, text = check(ADDITIVE, DOMAINS)

    assert code == 0
    assert set(blocks(text).values()) == {"BUMP"}
    assert text.count("stops at its next run until the pin moves to v8") == 2


def test_several_paths_are_scanned_as_one_list(env):
    """Several repositories are several checkouts on the command line."""
    code, text = check(BREAKING, FRAUD, FINANCE.parents[1])
    assert code == 1 and len(blocks(text)) == 2


def test_a_descriptor_that_reads_the_table_but_does_not_parse_fails_the_check(env, tmp_path):
    raw = yaml.safe_load(FRAUD.read_text())
    raw["expectations"]["on_fail"] = "block"
    broken = tmp_path / "broken.yml"
    broken.write_text(yaml.safe_dump(raw))

    code, text = check(ADDITIVE, broken)
    assert code == 1
    assert text.split("\n\n")[1].startswith("UNREADABLE")


def test_a_pin_the_catalog_does_not_have_fails_the_check(env, scenario, tmp_path):
    scenario(FRAUD, patch={"source.schema_version": 5})

    code, text = check(ADDITIVE, tmp_path)
    assert code == 1
    assert "UNKNOWN PIN" in text and "the catalog has no v5" in text


def test_a_table_nobody_reads_passes(env, tmp_path):
    proposal = yaml.safe_load(ADDITIVE.read_text())
    proposal["table"] = "fraud_raw.chargebacks"
    path = tmp_path / "proposal.yml"
    path.write_text(yaml.safe_dump(proposal))

    code, text = check(path, DOMAINS)
    assert code == 0
    assert "the table is not in the catalog yet" in text
    assert "no descriptor under" in text


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.pop("release"),
        lambda p: p.update({"table": "transactions"}),
        lambda p: p.update({"columns": []}),
    ],
    ids=["no release", "unqualified table", "no columns"],
)
def test_an_unusable_proposal_is_a_usage_error_not_a_verdict(env, tmp_path, mutate):
    proposal = yaml.safe_load(ADDITIVE.read_text())
    mutate(proposal)
    path = tmp_path / "proposal.yml"
    path.write_text(yaml.safe_dump(proposal))

    assert check(path, DOMAINS)[0] == 2
