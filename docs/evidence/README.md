# Evidence

Runs that published writing quotes. Each was captured from a real run, and the file records one run rather than being regenerated; a later run prints a different run id.

The first two were captured on 2026-09-17 at commit `48ca993` by `tools/capture_evidence.py`, which refuses to run on a dirty working tree, so the commit a file names is the code that produced its output. They are for `cordata-tech/platform#49` § 13 to quote, as the captured-run acceptance item on [#1](https://github.com/cordata-tech/pipeline-runtime/issues/1) asks. The third answers that issue's Glue question and needs an AWS account, so it was run by hand.

## `schema-drift-names-the-release.md`

The consumer side. The seed publishes `fraud_raw.transactions` at v7 as card-ledger release v4.11.0, and `--drift` publishes v8 as card-ledger release v4.12.0, adding a nullable `merchant_category_code`. The fraud pipeline still pins v7, so the run stops before reading a row, and the failure names the release alongside the column diff:

```
fraud_raw.transactions is at v8, descriptor pins v7. v8 was published by card-ledger release v4.12.0. Diff: + merchant_category_code (VARCHAR, nullable). Bump the pin to v8 to accept.
```

The message appears twice in the transcript: once on the run trace, and once on stderr from the CLI before it exits 1. `tests/test_failure_modes.py::test_schema_drift_fails_before_reading_anything` asserts the release, the column and the remedy are in the message, and `tests/test_catalog.py::test_drift_names_the_release_that_published_the_version` asserts the whole sentence.

The release here comes from the local catalog. On AWS the same values would be keys a publishing job sets in the Glue table's `Parameters`; that is confirmed against the API reference but has not been run against Glue, as recorded on #1.

## `consumers-breaking-change.md`

The producer side, with the catalog at v7. card-ledger release v5.0.0 proposes renaming `merchant_id` to `merchant_ref`. `python -m pipeline_runtime.consumers` scans `example/domains`, finds the two descriptors whose source is `fraud_raw.transactions` — fraud's `transactions-scored-daily`, a v1 descriptor, and finance's `merchant-settlement-daily`, a v2 one — and reports both as broken by the removed column, with their owners. It exits 1, which is what fails an application's build.

The second run in the same file is the contrast. Release v4.12.0 only adds a column, so the check exits 0 and lists both pipelines as needing their pins moved to v8. `tests/test_consumers.py` asserts the verdicts, the exit statuses and which descriptors are on the list, but not the wording.

## `glue-updatetable-parameters.md`

Whether the attribution the local catalog keeps in `_catalog.versions` has a carrier in the real one. Run against a live Glue Data Catalog on 2026-09-18 by `tools/glue_probe.py`, which creates a throwaway database, writes three table versions and deletes the database again.

`UpdateTable` replaces a table's `Parameters` wholesale: a key the job leaves out is dropped, and each archived `TableVersion` keeps what it was written with. `catalog.publish` is built for exactly that — both values required on every call, nothing carried forward — so the local catalog and Glue agree. What a crawler run or an Iceberg commit does to those keys is still open.
