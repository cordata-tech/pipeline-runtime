# Transcript: an Iceberg commit keeps the producer keys and discards the version history

**A schema change committed through Iceberg keeps `cordata:producer` and `cordata:release`, and by default it throws away the Glue table version that carried them.** The attribution on the *current* version survives every commit; the version a consumer pinned does not, because `pyiceberg` asks Glue not to archive the version it replaces. One property, `glue.skip-archive=false`, restores the history — and with it, the version a pin refers to.

That is the opposite of what [#4](https://github.com/cordata-tech/pipeline-runtime/issues/4) predicted. It expected the keys to be dropped, on the grounds that `UpdateTable` replaces `Parameters` wholesale; what happens instead is that the engine reads the current map and sends it back whole, so the keys ride along. The risk was real and in the wrong place.

Run on 2026-09-30 against a live Glue Data Catalog in `us-east-2`, with `pyiceberg` 0.12.0, by `tools/glue_probe.py --iceberg` as committed here. The probe creates a throwaway bucket and database, writes two tables — one with `pyiceberg`'s defaults, one with `glue.skip-archive=false` — and deletes both the database and the bucket. The output below is unedited.

```console
$ pip install "pyiceberg[glue,pyarrow]"
$ AWS_PROFILE=… python -m tools.glue_probe --iceberg
warehouse  s3://cordata-contract-probe-a9f76e34

================ pyiceberg defaults

--- [transactions] after Iceberg CreateTable
    VersionId  0
    columns    ['tx_id', 'amount_eur']
    Parameters {"metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00000-8353f77e-f8d3-4ba0-a42f-3cdf707084b2.metadata.json", "table_type": "ICEBERG"}

--- [transactions] after the publishing job stamps v4.11.0
    VersionId  1
    columns    ['tx_id', 'amount_eur']
    Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00000-8353f77e-f8d3-4ba0-a42f-3cdf707084b2.metadata.json", "table_type": "ICEBERG"}

--- [transactions] after two Iceberg schema commits
    VersionId  3
    columns    ['tx_id', 'amount_eur', 'merchant_category_code', 'settlement_currency']
    Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00002-55a4da40-2af0-4501-bbd7-288f44096dbb.metadata.json", "previous_metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00001-2e1bfba6-e723-4dbb-aa64-12704a42293a.metadata.json", "table_type": "ICEBERG"}

--- GetTableVersions: 2 versions
    v0  columns=['tx_id', 'amount_eur']
         Parameters {"metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00000-8353f77e-f8d3-4ba0-a42f-3cdf707084b2.metadata.json", "table_type": "ICEBERG"}
    v3  columns=['tx_id', 'amount_eur', 'merchant_category_code', 'settlement_currency']
         Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00002-55a4da40-2af0-4501-bbd7-288f44096dbb.metadata.json", "previous_metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions/metadata/00001-2e1bfba6-e723-4dbb-aa64-12704a42293a.metadata.json", "table_type": "ICEBERG"}

=== answers [pyiceberg defaults]
    producer survived the commits:        True
    release survived the commits:         True
    metadata_location moved:              True
    the pinned version was v1, stamped cordata:release=v4.11.0
    versions Glue still holds:            [0, 3]
    the pinned version is one of them:    False

================ glue.skip-archive=false

--- [transactions_archived] after Iceberg CreateTable
    VersionId  0
    columns    ['tx_id', 'amount_eur']
    Parameters {"metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00000-26300ac2-25b3-4dce-a4f2-b176437ce63b.metadata.json", "table_type": "ICEBERG"}

--- [transactions_archived] after the publishing job stamps v4.11.0
    VersionId  1
    columns    ['tx_id', 'amount_eur']
    Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00000-26300ac2-25b3-4dce-a4f2-b176437ce63b.metadata.json", "table_type": "ICEBERG"}

--- [transactions_archived] after two Iceberg schema commits
    VersionId  3
    columns    ['tx_id', 'amount_eur', 'merchant_category_code', 'settlement_currency']
    Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00002-6309831b-a707-4d2a-9dca-362cb3d3d9a4.metadata.json", "previous_metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00001-b963d904-df9b-4b64-aedd-864bc7c1b56c.metadata.json", "table_type": "ICEBERG"}

--- GetTableVersions: 4 versions
    v0  columns=['tx_id', 'amount_eur']
         Parameters {"metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00000-26300ac2-25b3-4dce-a4f2-b176437ce63b.metadata.json", "table_type": "ICEBERG"}
    v1  columns=['tx_id', 'amount_eur']
         Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00000-26300ac2-25b3-4dce-a4f2-b176437ce63b.metadata.json", "table_type": "ICEBERG"}
    v2  columns=['tx_id', 'amount_eur', 'merchant_category_code']
         Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00001-b963d904-df9b-4b64-aedd-864bc7c1b56c.metadata.json", "previous_metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00000-26300ac2-25b3-4dce-a4f2-b176437ce63b.metadata.json", "table_type": "ICEBERG"}
    v3  columns=['tx_id', 'amount_eur', 'merchant_category_code', 'settlement_currency']
         Parameters {"cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00002-6309831b-a707-4d2a-9dca-362cb3d3d9a4.metadata.json", "previous_metadata_location": "s3://cordata-contract-probe-a9f76e34/warehouse/cordata_contract_probe.db/transactions_archived/metadata/00001-b963d904-df9b-4b64-aedd-864bc7c1b56c.metadata.json", "table_type": "ICEBERG"}

=== answers [glue.skip-archive=false]
    producer survived the commits:        True
    release survived the commits:         True
    metadata_location moved:              True
    the pinned version was v1, stamped cordata:release=v4.11.0
    versions Glue still holds:            [0, 1, 2, 3]
    the pinned version is one of them:    True

cleaned up: database cordata_contract_probe deleted
cleaned up: bucket cordata-contract-probe-a9f76e34 deleted
```

Exit status 0.

## Why the keys survive

`pyiceberg` loads the table, adds its own two keys — `metadata_location`, and `previous_metadata_location` once there is a previous one — and sends the whole map back to `UpdateTable`. Since the map it sends includes everything it read, a publishing job's keys ride along untouched. The wholesale replacement measured in [`glue-updatetable-parameters.md`](glue-updatetable-parameters.md) is still what `UpdateTable` does; the engine simply does not omit anything.

This also confirms the rule the local catalog already follows from the other direction: a job that sent only its own two keys would drop `metadata_location` and break the table, so *send the full map* is not a nicety, it is how an Iceberg table stays readable.

## Why the pinned version disappears

`pyiceberg` passes `SkipArchive=True` on every commit. It is a documented property with a default, in `pyiceberg/catalog/glue.py`:

```python
GLUE_SKIP_ARCHIVE = "glue.skip-archive"
GLUE_SKIP_ARCHIVE_DEFAULT = True
```

With archiving skipped, Glue replaces the current version rather than filing it, so the version that was current when the commit landed is gone. In the first run that is v1, the version the publishing job had stamped: `GetTableVersions` returns `[0, 3]`, and the pin's own version is not there. The second table, created from a catalog configured with `glue.skip-archive=false`, keeps `[0, 1, 2, 3]` — every version, each with the attribution it was written with.

## What this means for this repository

A descriptor pins a version number, and the executor resolves that number against the catalog before reading a row. Under `pyiceberg`'s default that number can stop resolving: two commits after the pin was written, the version it names is no longer in the catalog. The failure is loud rather than silent — `SchemaDrift` cannot find the pinned version, and `pipeline_runtime.consumers` reports `UNKNOWN PIN` — but it is a worse failure than drift, because there is nothing left to diff against.

The local DuckDB catalog keeps every version it has ever published, so it models Glue **with archiving on**. A deployment that points descriptors at Iceberg tables has to make the real catalog match that by setting `glue.skip-archive=false`, or accept that pins expire for reasons nobody recorded.

## What it does not settle

- **Crawlers**, which are the other half of #4 and still untested.
- **Other engines.** This measured `pyiceberg` 0.12.0. Spark, Trino and Athena each have their own Glue catalog implementation and their own default for the same archive decision, and a deployment mixing engines is only as safe as the loosest one.
- **Iceberg's own metadata chain** is separate from Glue's versions: `previous_metadata_location` walks backwards through Iceberg metadata files even when Glue kept no version. A reader that understands Iceberg can therefore recover a schema this repository's version pin cannot, which is a different design from the one the articles describe rather than a fix for it.
