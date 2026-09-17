# Transcript: a `SchemaDrift` failure that names the release

Captured by `tools/capture_evidence.py` on 2026-09-17, from pipeline-runtime 0.1.0 at commit `48ca993` with a clean working tree, on Python 3.12.13. The output below is what each command printed, stdout and stderr together, unedited, followed by its exit status. Run the same commands from the repository root to reproduce it; the four-character run id differs on every run.

The consumer side. `fraud_raw.transactions` is published at v7 by card-ledger release v4.11.0, then moved to v8 by card-ledger release v4.12.0, which adds a column. The fraud pipeline still pins v7 and stops before reading a row.

```console
$ ./.venv/bin/python -m tools.seed --clean
catalog   example/catalog.duckdb
fraud_raw.transactions   50,000 rows at v7
policy_raw.claims        10,240 CDC rows in 4 batches under example/warehouse/landing/policy_raw/claims
```

Exit status 0.

```console
$ ./.venv/bin/python -m tools.seed --drift
catalog   example/catalog.duckdb
fraud_raw.transactions   50,000 rows at v8
policy_raw.claims        10,240 CDC rows in 4 batches under example/warehouse/landing/policy_raw/claims

fraud_raw.transactions is now at v8, published by card-ledger release v4.12.0; the descriptors pin v7.
```

Exit status 0.

```console
$ ./.venv/bin/python -m pipeline_runtime example/domains/fraud/pipelines/transactions_scored.yml
[0478] descriptor  transactions-scored-daily (fraud)      apiVersion v1 OK
[0478] schema      fraud_raw.transactions                 pinned v7, catalog v8
[0478] FAILED      SchemaDrift:                           fraud_raw.transactions is at v8, descriptor pins v7. v8 was published by card-ledger release v4.12.0. Diff: + merchant_category_code (VARCHAR, nullable). Bump the pin to v8 to accept.
pipeline-runtime: SchemaDrift: fraud_raw.transactions is at v8, descriptor pins v7. v8 was published by card-ledger release v4.12.0. Diff: + merchant_category_code (VARCHAR, nullable). Bump the pin to v8 to accept.
```

Exit status 1.
