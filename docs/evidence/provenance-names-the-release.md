# Transcript: a successful run carrying the release behind its input

Captured by `tools/capture_evidence.py` on 2026-09-18, from pipeline-runtime 0.1.0 at commit `afeda9e` with a clean working tree, on Python 3.12.13. The output below is what each command printed, stdout and stderr together, unedited, followed by its exit status. Run the same commands from the repository root to reproduce it; the four-character run id differs on every run.

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
[702f] descriptor  transactions-scored-daily (fraud)      apiVersion v1 OK
[702f] schema      fraud_raw.transactions                 pinned v7, catalog v7 OK
[702f] read        glue_table                             50,000 rows
[702f] step        filter_settled        (sql)            35,919 rows
[702f] step        score                 (sql)            35,919 rows
[702f] policy      sensitivity=high residency=eu subject_type=customer resolved against ontology

Calculating Metrics:   0%|          | 0/8 [00:00<?, ?it/s]
Calculating Metrics:   0%|          | 0/8 [00:00<?, ?it/s]
Calculating Metrics:  25%|██▌       | 2/8 [00:00<00:00, 3623.59it/s]
Calculating Metrics:  25%|██▌       | 2/8 [00:00<00:00, 3320.91it/s]
Calculating Metrics:  38%|███▊      | 3/8 [00:00<00:00, 4716.23it/s]
Calculating Metrics:  38%|███▊      | 3/8 [00:00<00:00, 4537.65it/s]
Calculating Metrics:  50%|█████     | 4/8 [00:00<00:00, 4115.09it/s]
Calculating Metrics:  50%|█████     | 4/8 [00:00<00:00, 4000.29it/s]
Calculating Metrics: 100%|██████████| 8/8 [00:00<00:00, 3957.82it/s]
Calculating Metrics: 100%|██████████| 8/8 [00:00<00:00, 3908.04it/s]
Calculating Metrics: 100%|██████████| 8/8 [00:00<00:00, 3871.96it/s]
Calculating Metrics: 100%|██████████| 8/8 [00:00<00:00, 3815.17it/s]

Calculating Metrics:   0%|          | 0/10 [00:00<?, ?it/s]
Calculating Metrics:   0%|          | 0/10 [00:00<?, ?it/s]
Calculating Metrics:  20%|██        | 2/10 [00:00<00:00, 19239.93it/s]
Calculating Metrics:  20%|██        | 2/10 [00:00<00:00, 14074.85it/s]
Calculating Metrics:  30%|███       | 3/10 [00:00<00:00, 17975.59it/s]
Calculating Metrics:  30%|███       | 3/10 [00:00<00:00, 15630.95it/s]
Calculating Metrics:  50%|█████     | 5/10 [00:00<00:00, 2659.67it/s] 
Calculating Metrics:  50%|█████     | 5/10 [00:00<00:00, 2615.23it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3780.70it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3748.26it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3723.31it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3681.80it/s]

Calculating Metrics:   0%|          | 0/10 [00:00<?, ?it/s]
Calculating Metrics:   0%|          | 0/10 [00:00<?, ?it/s]
Calculating Metrics:  20%|██        | 2/10 [00:00<00:00, 20971.52it/s]
Calculating Metrics:  20%|██        | 2/10 [00:00<00:00, 15141.89it/s]
Calculating Metrics:  30%|███       | 3/10 [00:00<00:00, 19210.55it/s]
Calculating Metrics:  30%|███       | 3/10 [00:00<00:00, 16644.06it/s]
Calculating Metrics:  50%|█████     | 5/10 [00:00<00:00, 2082.37it/s] 
Calculating Metrics:  50%|█████     | 5/10 [00:00<00:00, 2056.84it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3216.24it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3192.74it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3175.58it/s]
Calculating Metrics: 100%|██████████| 10/10 [00:00<00:00, 3149.35it/s]

Calculating Metrics:   0%|          | 0/9 [00:00<?, ?it/s]
Calculating Metrics:   0%|          | 0/9 [00:00<?, ?it/s]
Calculating Metrics:  22%|██▏       | 2/9 [00:00<00:00, 24456.58it/s]
Calculating Metrics:  22%|██▏       | 2/9 [00:00<00:00, 18724.57it/s]
Calculating Metrics:  56%|█████▌    | 5/9 [00:00<00:00, 19991.92it/s]
Calculating Metrics:  56%|█████▌    | 5/9 [00:00<00:00, 18331.75it/s]
Calculating Metrics:  78%|███████▊  | 7/9 [00:00<00:00, 5347.93it/s] 
Calculating Metrics:  78%|███████▊  | 7/9 [00:00<00:00, 5256.02it/s]
Calculating Metrics:  89%|████████▉ | 8/9 [00:00<00:00, 5818.35it/s]
Calculating Metrics:  89%|████████▉ | 8/9 [00:00<00:00, 5746.61it/s]
Calculating Metrics: 100%|██████████| 9/9 [00:00<00:00, 6368.94it/s]
Calculating Metrics: 100%|██████████| 9/9 [00:00<00:00, 6294.60it/s]
Calculating Metrics: 100%|██████████| 9/9 [00:00<00:00, 6232.25it/s]
Calculating Metrics: 100%|██████████| 9/9 [00:00<00:00, 6135.01it/s]

Calculating Metrics:   0%|          | 0/1 [00:00<?, ?it/s]
Calculating Metrics:   0%|          | 0/1 [00:00<?, ?it/s]
Calculating Metrics: 100%|██████████| 1/1 [00:00<00:00, 24244.53it/s]
Calculating Metrics: 100%|██████████| 1/1 [00:00<00:00, 16320.25it/s]
Calculating Metrics: 100%|██████████| 1/1 [00:00<00:00, 12787.51it/s]
Calculating Metrics: 100%|██████████| 1/1 [00:00<00:00, 9892.23it/s] 
[702f] expect      transactions_scored                    5 passed, 0 failed
[702f] write       fraud_curated.transactions_scored      iceberg, 1 partition key
[702f] emit        COMPLETE → lineage adapter             inputs=1 outputs=1
```

Exit status 0.

```console
$ ./.venv/bin/python -m tools.show_provenance
COMPLETE  run 702f4176-d290-423f-956a-33e371e11dca
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
