# Transcript: a crawler keeps the producer keys and the version they were written on

**A Glue crawler re-crawling a table it manages preserves `cordata:producer` and `cordata:release`, rewrites its own parameters, and archives the version it replaced.** Every worry [#4](https://github.com/cordata-tech/pipeline-runtime/issues/4) opened with about crawlers turns out not to apply: the keys survive, and so does the version a descriptor pinned. Of the three write paths now measured, only an Iceberg commit loses anything, and what it loses is the version rather than the keys.

Run on 2026-09-30 against a live Glue Data Catalog in `us-east-2`, by `tools/glue_probe.py --crawler` as committed here. The probe creates a throwaway bucket, the IAM role a crawler runs as, a database and a crawler; crawls a CSV prefix; stamps attribution the way a publishing job would; changes the data's shape; crawls again; and deletes all of it. Two crawls bill a ten-minute minimum of DPU time each, so this run cost a few cents. The output below is unedited.

```console
$ AWS_PROFILE=… python -m tools.glue_probe --crawler
warehouse  s3://cordata-contract-probe-f2caadb9
role       cordata-contract-probe-crawler

--- first crawl, which creates the table
    waiting for the role to propagate (InvalidInputException)
    waiting for the role to propagate (InvalidInputException)
    crawl finished: SUCCEEDED in 40s

--- 1. after the crawler created the table
    VersionId  0
    columns    ['tx_id', 'amount_eur']
    Parameters {"CRAWL_RUN_ID": "ae817492-b097-4724-9fd3-a12e8851d78b", "CrawlerSchemaDeserializerVersion": "1.0", "CrawlerSchemaSerializerVersion": "1.0", "UPDATED_BY_CRAWLER": "cordata-contract-probe", "areColumnsQuoted": "false", "averageRecordSize": "14", "classification": "csv", "columnsOrdered": "true", "compressionType": "none", "delimiter": ",", "objectCount": "1", "recordCount": "6", "sizeKey": "87", "skip.header.line.count": "1", "typeOfData": "file"}

--- [transactions] after the publishing job stamps v4.11.0
    VersionId  1
    columns    ['tx_id', 'amount_eur']
    Parameters {"CRAWL_RUN_ID": "ae817492-b097-4724-9fd3-a12e8851d78b", "CrawlerSchemaDeserializerVersion": "1.0", "CrawlerSchemaSerializerVersion": "1.0", "UPDATED_BY_CRAWLER": "cordata-contract-probe", "areColumnsQuoted": "false", "averageRecordSize": "14", "classification": "csv", "columnsOrdered": "true", "compressionType": "none", "cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "delimiter": ",", "objectCount": "1", "recordCount": "6", "sizeKey": "87", "skip.header.line.count": "1", "typeOfData": "file"}

--- second crawl, after the data gained a column
    crawl finished: SUCCEEDED in 40s

--- 3. after the crawler re-crawled it
    VersionId  2
    columns    ['tx_id', 'amount_eur', 'merchant_category_code']
    Parameters {"CRAWL_RUN_ID": "0b9afe97-403e-4308-8575-cb1cae0ba550", "CrawlerSchemaDeserializerVersion": "1.0", "CrawlerSchemaSerializerVersion": "1.0", "UPDATED_BY_CRAWLER": "cordata-contract-probe", "areColumnsQuoted": "false", "averageRecordSize": "25", "classification": "csv", "columnsOrdered": "true", "compressionType": "none", "cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "delimiter": ",", "objectCount": "1", "recordCount": "5", "sizeKey": "135", "skip.header.line.count": "1", "typeOfData": "file"}
    the shape the crawler now reports: ['tx_id', 'amount_eur', 'merchant_category_code']

--- GetTableVersions: 3 versions
    v0  columns=['tx_id', 'amount_eur']
         Parameters {"CRAWL_RUN_ID": "ae817492-b097-4724-9fd3-a12e8851d78b", "CrawlerSchemaDeserializerVersion": "1.0", "CrawlerSchemaSerializerVersion": "1.0", "UPDATED_BY_CRAWLER": "cordata-contract-probe", "areColumnsQuoted": "false", "averageRecordSize": "14", "classification": "csv", "columnsOrdered": "true", "compressionType": "none", "delimiter": ",", "objectCount": "1", "recordCount": "6", "sizeKey": "87", "skip.header.line.count": "1", "typeOfData": "file"}
    v1  columns=['tx_id', 'amount_eur']
         Parameters {"CRAWL_RUN_ID": "ae817492-b097-4724-9fd3-a12e8851d78b", "CrawlerSchemaDeserializerVersion": "1.0", "CrawlerSchemaSerializerVersion": "1.0", "UPDATED_BY_CRAWLER": "cordata-contract-probe", "areColumnsQuoted": "false", "averageRecordSize": "14", "classification": "csv", "columnsOrdered": "true", "compressionType": "none", "cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "delimiter": ",", "objectCount": "1", "recordCount": "6", "sizeKey": "87", "skip.header.line.count": "1", "typeOfData": "file"}
    v2  columns=['tx_id', 'amount_eur', 'merchant_category_code']
         Parameters {"CRAWL_RUN_ID": "0b9afe97-403e-4308-8575-cb1cae0ba550", "CrawlerSchemaDeserializerVersion": "1.0", "CrawlerSchemaSerializerVersion": "1.0", "UPDATED_BY_CRAWLER": "cordata-contract-probe", "areColumnsQuoted": "false", "averageRecordSize": "25", "classification": "csv", "columnsOrdered": "true", "compressionType": "none", "cordata:producer": "card-ledger", "cordata:release": "v4.11.0", "delimiter": ",", "objectCount": "1", "recordCount": "5", "sizeKey": "135", "skip.header.line.count": "1", "typeOfData": "file"}

=== answers [crawler]
    producer survived the re-crawl:       True
    release survived the re-crawl:        True
    the crawler's own keys are still set: True
    the crawler rewrote its own keys:     True
    the crawler saw the new column:       True
    the pinned version was v1, stamped cordata:release=v4.11.0
    versions Glue still holds:            [0, 1, 2]
    the pinned version is one of them:    True
    keys the crawler wrote at creation:   ['CRAWL_RUN_ID', 'CrawlerSchemaDeserializerVersion', 'CrawlerSchemaSerializerVersion', 'UPDATED_BY_CRAWLER', 'areColumnsQuoted', 'averageRecordSize', 'classification', 'columnsOrdered', 'compressionType', 'delimiter', 'objectCount', 'recordCount', 'sizeKey', 'skip.header.line.count', 'typeOfData']
cleaned up: crawler and role deleted

cleaned up: database cordata_contract_probe deleted
cleaned up: bucket cordata-contract-probe-f2caadb9 deleted
```

Exit status 0.

## What each part settles

**A crawler merges rather than replaces.** It writes fifteen parameters of its own, and the second crawl rewrote several of them — `CRAWL_RUN_ID` changed, and `averageRecordSize`, `recordCount` and `sizeKey` followed the new file — while leaving `cordata:producer` and `cordata:release` exactly as the publishing job had set them. So attribution on a crawler-managed table is not at risk from crawls.

**A crawler archives.** `GetTableVersions` holds `[0, 1, 2]`: the version the crawler created, the version the publishing job stamped, and the version the re-crawl produced. The v1 that a descriptor would have pinned is still there, with the shape it promised and the release that published it.

**A crawler did see the schema change**, moving the table from two columns to three, which is what makes the previous two answers meaningful rather than a crawl that did nothing.

## A note on the fixture, because the first attempt measured the wrong thing

An earlier run of this probe used a two-line CSV, and the crawler named the columns `col0` and `col1`. The CSV classifier infers a header row from its types differing from the rows beneath it, and two rows of identical strings give it nothing to compare, so it treated the header as data. The fixture now writes five typed rows and the names come through, which is why `skip.header.line.count` appears in the parameters above and did not before. The finding did not change — the keys and the versions survived in both runs — but the transcript would have looked like a crawler that renames the world.

## What it does not settle

- **A crawler configured differently.** This used `UpdateBehavior: UPDATE_IN_DATABASE` and `DeleteBehavior: LOG`, which are the defaults a console-created crawler gets. `UpdateBehavior: LOG` would leave the schema alone, and the delete behaviours that drop tables or mark them deprecated were not tried.
- **A crawler that creates the table after a publishing job did.** Here the crawler created the table first, which is the ordinary case for a crawler-managed prefix.
- **Other engines**, as with the Iceberg probe. Spark and Athena reach the same catalog by their own paths.
