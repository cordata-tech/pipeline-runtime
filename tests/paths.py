"""Where the example domain lives. Imported by conftest and by the tests."""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "example"
ONTOLOGY = EXAMPLE / "ontology.json"

FRAUD = EXAMPLE / "domains/fraud/pipelines/transactions_scored.yml"
CLAIMS = EXAMPLE / "domains/policy/pipelines/claims_ingest.yml"

# Not published in part 1, and written against cordata.tech/v2.
FINANCE = EXAMPLE / "domains/finance/pipelines/merchant_settlement.yml"
