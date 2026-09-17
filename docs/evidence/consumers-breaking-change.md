# Transcript: the producer-side check failing on a breaking change

Captured by `tools/capture_evidence.py` on 2026-09-17, from pipeline-runtime 0.1.0 at commit `48ca993` with a clean working tree, on Python 3.12.13. The output below is what each command printed, stdout and stderr together, unedited, followed by its exit status. Run the same commands from the repository root to reproduce it; the four-character run id differs on every run.

The producer side. The catalog is at v7. card-ledger release v5.0.0 proposes renaming `merchant_id` to `merchant_ref`; the check finds the two descriptors that pin the table and exits 1. The second run is the contrast: release v4.12.0 only adds a column, and the check exits 0 while listing the pipelines whose pins have to move.

```console
$ ./.venv/bin/python -m tools.seed --clean
catalog   example/catalog.duckdb
fraud_raw.transactions   50,000 rows at v7
policy_raw.claims        10,240 CDC rows in 4 batches under example/warehouse/landing/policy_raw/claims
```

Exit status 0.

```console
$ ./.venv/bin/python -m pipeline_runtime.consumers example/proposals/card-ledger-v5.0.0.yml example/domains
card-ledger release v5.0.0 proposes fraud_raw.transactions v8; the catalog is at v7
2 descriptors under example/domains read fraud_raw.transactions

BREAKS      merchant-settlement-daily (finance)  pins v7  owner finance-data@example.com
            example/domains/finance/pipelines/merchant_settlement.yml
            + merchant_ref (VARCHAR, not null)
            - merchant_id (VARCHAR, not null)   <- breaks

BREAKS      transactions-scored-daily (fraud)  pins v7  owner fraud-data@example.com
            example/domains/fraud/pipelines/transactions_scored.yml
            + merchant_ref (VARCHAR, not null)
            - merchant_id (VARCHAR, not null)   <- breaks

2 of 2 consumers fail this check
```

Exit status 1.

```console
$ ./.venv/bin/python -m pipeline_runtime.consumers example/proposals/card-ledger-v4.12.0.yml example/domains
card-ledger release v4.12.0 proposes fraud_raw.transactions v8; the catalog is at v7
2 descriptors under example/domains read fraud_raw.transactions

BUMP        merchant-settlement-daily (finance)  pins v7  owner finance-data@example.com
            example/domains/finance/pipelines/merchant_settlement.yml
            + merchant_category_code (VARCHAR, nullable)
            stops at its next run until the pin moves to v8

BUMP        transactions-scored-daily (fraud)  pins v7  owner fraud-data@example.com
            example/domains/fraud/pipelines/transactions_scored.yml
            + merchant_category_code (VARCHAR, nullable)
            stops at its next run until the pin moves to v8

0 of 2 consumers fail this check
```

Exit status 0.
