# Transcript: Glue's `UpdateTable` replaces `Parameters` wholesale

**`UpdateTable` keeps only the `Parameters` the call itself sends.** A key the publishing job leaves out is dropped from the current table, and each archived `TableVersion` keeps whatever it was written with. So a producer and a release reference are a usable carrier for attribution, and a job that ships a schema change has to write both on every update or the attribution silently goes missing.

Run on 2026-09-18 against a live Glue Data Catalog in `us-east-2`, with `boto3` 1.43.97, by `tools/glue_probe.py` as committed here. The script created one throwaway database, wrote three table versions into it, and deleted the database again. The output below is what it printed, unedited.

This is the question [#1](https://github.com/cordata-tech/pipeline-runtime/issues/1) and `cordata-tech/platform#49` both recorded as unverified; the AWS API reference does not state it either way.

```console
$ pip install boto3
$ AWS_PROFILE=… python -m tools.glue_probe

--- 1. after CreateTable
    VersionId  0
    columns    ['tx_id', 'amount_eur']
    Parameters {"classification": "parquet", "cordata:producer": "card-ledger", "cordata:release": "v4.11.0"}

--- 2. after UpdateTable with Parameters={producer} only
    VersionId  1
    columns    ['tx_id', 'amount_eur', 'merchant_category_code']
    Parameters {"cordata:producer": "card-ledger"}

--- 3. after UpdateTable with no Parameters at all
    VersionId  2
    columns    ['tx_id', 'amount_eur', 'merchant_category_code']
    Parameters {}

--- GetTableVersions: 3 versions
    v0  columns=['tx_id', 'amount_eur']
         Parameters {"classification": "parquet", "cordata:producer": "card-ledger", "cordata:release": "v4.11.0"}
    v1  columns=['tx_id', 'amount_eur', 'merchant_category_code']
         Parameters {"cordata:producer": "card-ledger"}
    v2  columns=['tx_id', 'amount_eur', 'merchant_category_code']
         Parameters {}

=== answers
    release survived a partial update:  False
    unrelated key survived:             False
    producer survived an omitted block: False
    versions archived by default:       3
    oldest archived version still carries release: 'v4.11.0'

cleaned up: database cordata_contract_probe deleted
```

Exit status 0.

## What each step settles

**Step 2 is the one the design turns on.** The update set `cordata:producer` and nothing else; `cordata:release` was gone from the table afterwards, and so was `classification`, a key Glue itself uses and the job had no reason to touch. There is no merge: the block sent replaces the block stored.

**Step 3 shows an omitted block is the same as an empty one.** Leaving `Parameters` out of the `TableInput` entirely left the table with none, rather than leaving the previous ones alone.

**The version history is unaffected by either.** Three versions exist after one create and two updates, so `UpdateTable` archives by default, and v0 still carries the release it was published with. Attribution written at publish time stays readable for as long as the version does, which is what `SchemaDrift` needs when a pinned consumer is several versions behind.

## What this repo does with it

`catalog.publish` requires a producer and a release on every call, copies nothing from the version before, and writes the attribution row in the same transaction as the version's columns. That was the design before this was measured, on the assumption that replacement was the riskier possibility; the measurement confirms it is the actual one. `tests/test_catalog.py::test_nothing_is_carried_forward_from_the_version_before` holds it.

A version with no attribution is reported by name — *published by an unrecorded producer* — rather than inheriting the previous release. Step 3 is what that looks like upstream.

## What it does not settle

Whether a **crawler** run, or an **Iceberg** commit through Glue, overwrites parameters a publishing job set. Both create versions by a path this probe did not exercise, and a crawler that wipes the keys on its next run would make attribution unreliable for tables under a crawler. Worth a second probe before anything depends on it in a deployment.
