# Transcript: a successful run carrying the release behind its input

Captured by `tools/capture_evidence.py` on 2026-09-18, from pipeline-runtime 0.1.0 at commit `aa00291` with a clean working tree, on Python 3.12.13. The output below is what each command printed, stdout and stderr together, unedited, followed by its exit status. Run the same commands from the repository root to reproduce it; the four-character run id differs on every run.

The chain, on a run that worked. `fraud_raw.transactions` is at v7, published by card-ledger release v4.11.0, and the pipeline pins v7 — so nothing fails, and the event it emits still says which application release gave the input its shape. `descriptor_git_commit_signed` reaches the reviewed commit that authorised the pipeline; `source_published_by` and `source_published_release` reach the change upstream of it.

```console
$ ./.venv/bin/python -m tools.seed --clean
catalog   example/catalog.duckdb
fraud_raw.transactions   50,000 rows at v7
policy_raw.claims        10,240 CDC rows in 4 batches under example/warehouse/landing/policy_raw/claims
```

Exit status 0.

```console
$ ./.venv/bin/python -m pipeline_runtime example/domains/fraud/pipelines/transactions_scored.yml
[12cc] descriptor  transactions-scored-daily (fraud)      apiVersion v1 OK
[12cc] schema      fraud_raw.transactions                 pinned v7, catalog v7 OK
[12cc] read        glue_table                             50,000 rows
[12cc] step        filter_settled        (sql)            35,919 rows
[12cc] step        score                 (sql)            35,919 rows
[12cc] policy      sensitivity=high residency=eu subject_type=customer resolved against ontology
[12cc] expect      transactions_scored                    5 passed, 0 failed
[12cc] write       fraud_curated.transactions_scored      iceberg, 1 partition key
[12cc] emit        COMPLETE → lineage adapter             inputs=1 outputs=1
```

Exit status 0.

```console
$ ./.venv/bin/python -m tools.show_provenance
COMPLETE  run 12cce55f-9abb-4d11-821d-bb7e37ed3780
{
  "descriptor_git_commit": "1728854481e05ebb401e732925fc3763f6199a96",
  "descriptor_git_commit_signed": true,
  "descriptor_path": "example/domains/fraud/pipelines/transactions_scored.yml",
  "descriptor_sha256": "e1970c0bcc93756dbac6439757e6ff57f4ac00b93bbcfbeb4d564d12d0e4dd4a",
  "executor_version": "pipeline-runtime 0.1.0",
  "source_published_by": "card-ledger",
  "source_published_release": "v4.11.0",
  "source_schema_version": 7,
  "source_table": "fraud_raw.transactions",
  "step_params": {
    "score": {
      "model_version": "2026-07-fraud-v3"
    }
  }
}
```

Exit status 0.
