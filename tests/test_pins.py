"""The readers of a table, found from the pins and with the base install.

`test_consumers.py` holds the producer-side check built on these. This holds the
list itself, which a catalog server serves without a catalog to compare against
(catalog-mcp#2), so it is tested without the seeded catalog the check needs.
"""

from __future__ import annotations

import yaml

from pipeline_runtime.descriptor import TableRef
from pipeline_runtime.pins import readers

from .paths import CLAIMS, EXAMPLE, FINANCE, FRAUD

DOMAINS = EXAMPLE / "domains"
TRANSACTIONS = TableRef(database="fraud_raw", table="transactions")


def test_a_table_two_domains_read_has_both_readers_in_path_order():
    found = readers(TRANSACTIONS, [DOMAINS])
    assert [r.path for r in found] == [FINANCE, FRAUD]
    assert [(r.pipeline.metadata.name, r.pipeline.metadata.domain) for r in found] == [
        ("merchant-settlement-daily", "finance"),
        ("transactions-scored-daily", "fraud"),
    ]


def test_a_v2_reader_comes_back_in_the_shape_the_executor_runs():
    """Its pin is `source.contract.schema_version` as written, `source.schema_version` here."""
    finance = readers(TRANSACTIONS, [FINANCE])[0].pipeline
    assert finance.apiVersion == "cordata.tech/v1"
    assert finance.source.schema_version == 7


def test_a_table_nobody_reads_has_no_readers():
    assert readers(TableRef(database="fraud_curated", table="transactions_scored"), [DOMAINS]) == []


def test_the_claims_table_has_only_its_own_reader():
    found = readers(TableRef(database="policy_raw", table="claims"), [DOMAINS])
    assert [r.path for r in found] == [CLAIMS]


def test_a_reader_that_does_not_parse_is_still_a_reader(tmp_path):
    raw = yaml.safe_load(FRAUD.read_text())
    raw["expectations"]["on_fail"] = "block"
    broken = tmp_path / "broken.yml"
    broken.write_text(yaml.safe_dump(raw))

    [reader] = readers(TRANSACTIONS, [broken])
    assert reader.pipeline is None
    assert reader.detail, "the reason it does not parse travels with it"


def test_a_file_that_is_not_yaml_is_not_a_reader(tmp_path):
    (tmp_path / "notes.yml").write_text("source: [unclosed\n")
    assert readers(TRANSACTIONS, [tmp_path]) == []
