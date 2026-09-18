"""What survives in a Glue table's `Parameters`, measured against the real catalog.

The questions a local stand-in cannot answer, because they are about the
catalog's behaviour rather than about the descriptor. Both decide whether
`catalog.publish` may carry a producer forward from the previous version, so they
are measured rather than assumed — see
`docs/evidence/glue-updatetable-parameters.md` for the runs they produced.

Unlike everything else here this needs an AWS account, which is why the
dependencies are not the repository's and this is not part of the quickstart:

    pip install boto3                                 # the UpdateTable probe
    pip install "pyiceberg[glue,pyarrow]"             # and the Iceberg one

    AWS_PROFILE=… python -m tools.glue_probe            # UpdateTable, then clean up
    AWS_PROFILE=… python -m tools.glue_probe --iceberg  # an Iceberg commit instead
    AWS_PROFILE=… python -m tools.glue_probe --keep     # leave what it made behind

The default mode creates one throwaway database and writes three table versions
into it. `--iceberg` also creates a throwaway S3 bucket, because an Iceberg table
needs a warehouse to write metadata to. Both delete what they made, touch nothing
else in the account, and refuse to run if the database already exists.
"""

from __future__ import annotations

import contextlib
import json
import secrets
import sys

import boto3

DB = "cordata_contract_probe"
TABLE = "transactions"

PRODUCER = "cordata:producer"
RELEASE = "cordata:release"

COLUMNS_V1 = [
    {"Name": "tx_id", "Type": "string"},
    {"Name": "amount_eur", "Type": "double"},
]
COLUMNS_V2 = [*COLUMNS_V1, {"Name": "merchant_category_code", "Type": "string"}]

glue = boto3.client("glue")


def table_input(columns: list[dict], parameters: dict | None) -> dict:
    """A minimal external table. `parameters=None` omits the block entirely."""
    body = {
        "Name": TABLE,
        "TableType": "EXTERNAL_TABLE",
        "StorageDescriptor": {
            "Columns": columns,
            "Location": "s3://cordata-contract-probe/transactions/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        },
    }
    if parameters is not None:
        body["Parameters"] = parameters
    return body


def show(label: str, table_name: str = TABLE) -> dict:
    table = glue.get_table(DatabaseName=DB, Name=table_name)["Table"]
    params = table.get("Parameters", {})
    print(f"\n--- {label}")
    print(f"    VersionId  {table.get('VersionId')}")
    print(f"    columns    {[c['Name'] for c in table['StorageDescriptor']['Columns']]}")
    print(f"    Parameters {json.dumps(params, sort_keys=True)}")
    return params


def versions(table_name: str = TABLE) -> list[dict]:
    found = glue.get_table_versions(DatabaseName=DB, TableName=table_name)["TableVersions"]
    print(f"\n--- GetTableVersions: {len(found)} versions")
    for v in sorted(found, key=lambda v: int(v["VersionId"])):
        columns = [c["Name"] for c in v["Table"]["StorageDescriptor"]["Columns"]]
        print(f"    v{v['VersionId']}  columns={columns}")
        print(f"         Parameters {json.dumps(v['Table'].get('Parameters', {}), sort_keys=True)}")
    return found


def cleanup() -> None:
    for delete in (
        lambda: glue.delete_table(DatabaseName=DB, Name=TABLE),
        lambda: glue.delete_database(Name=DB),
    ):
        with contextlib.suppress(glue.exceptions.EntityNotFoundException):
            delete()
    print(f"\ncleaned up: database {DB} deleted")


# ---------------------------------------------------------------- the Iceberg probe


def make_bucket() -> str:
    """A throwaway warehouse. An Iceberg table has to write its metadata somewhere."""
    region = boto3.session.Session().region_name
    name = f"cordata-contract-probe-{secrets.token_hex(4)}"
    boto3.client("s3").create_bucket(
        Bucket=name, CreateBucketConfiguration={"LocationConstraint": region}
    )
    print(f"warehouse  s3://{name}")
    return name


def empty_and_delete_bucket(name: str) -> None:
    s3 = boto3.client("s3")
    pages = s3.get_paginator("list_object_versions").paginate(Bucket=name)
    for page in pages:
        doomed = [
            {"Key": o["Key"], "VersionId": o["VersionId"]}
            for key in ("Versions", "DeleteMarkers")
            for o in page.get(key, [])
        ]
        if doomed:
            s3.delete_objects(Bucket=name, Delete={"Objects": doomed})
    s3.delete_bucket(Bucket=name)


def stamp(table_name: str, release: str) -> dict:
    """What a publishing job does: send the whole map back, with attribution added.

    Reading the current parameters first is not politeness, it is required —
    `UpdateTable` replaces the map, so a job that sent only its own two keys
    would drop Iceberg's pointer and break the table.
    """
    table = glue.get_table(DatabaseName=DB, Name=table_name)["Table"]
    glue.update_table(
        DatabaseName=DB,
        TableInput={
            "Name": table_name,
            "TableType": table["TableType"],
            "StorageDescriptor": table["StorageDescriptor"],
            "Parameters": {
                **table.get("Parameters", {}),
                PRODUCER: "card-ledger",
                RELEASE: release,
            },
        },
    )
    return show(f"[{table_name}] after the publishing job stamps {release}", table_name)


def probe_iceberg(bucket: str) -> None:
    """Does a schema change committed through Iceberg keep a publishing job's keys,
    and does the version a consumer pinned survive the commit?

    The published descriptors declare `target.kind: iceberg`, so on AWS this is
    the write path for a pipeline's own output rather than an edge case. Iceberg
    keeps its own pointer in the same `Parameters` map the attribution lives in,
    and it has an opinion about Glue's version history, so both are measured.

    Two tables, one with pyiceberg's defaults and one with `glue.skip-archive`
    turned off, because if the default loses history the setting is the fix a
    deployment needs to know about.
    """
    from pyiceberg.catalog.glue import GlueCatalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import DoubleType, NestedField, StringType

    region = boto3.session.Session().region_name

    def catalog_with(**extra: str) -> GlueCatalog:
        return GlueCatalog(
            "probe",
            **{
                "warehouse": f"s3://{bucket}/warehouse",
                "glue.region": region,
                "s3.region": region,
                **extra,
            },
        )

    def schema() -> Schema:
        return Schema(
            NestedField(1, "tx_id", StringType(), required=True),
            NestedField(2, "amount_eur", DoubleType(), required=False),
        )

    for table_name, catalog, label in (
        (TABLE, catalog_with(), "pyiceberg defaults"),
        (
            f"{TABLE}_archived",
            catalog_with(**{"glue.skip-archive": "false"}),
            "glue.skip-archive=false",
        ),
    ):
        print(f"\n================ {label}")

        catalog.create_table((DB, table_name), schema=schema())
        created = show(f"[{table_name}] after Iceberg CreateTable", table_name)

        # The version a consumer would pin: stamped by the publishing job, and
        # the shape that version promises.
        pinned = stamp(table_name, "v4.11.0")
        pinned_version = glue.get_table(DatabaseName=DB, Name=table_name)["Table"]["VersionId"]

        # Two schema commits through Iceberg, because one is not enough to show
        # what happens to the version in between.
        for column in ("merchant_category_code", "settlement_currency"):
            loaded = catalog.load_table((DB, table_name))
            with loaded.update_schema() as update:
                update.add_column(column, StringType())
        after = show(f"[{table_name}] after two Iceberg schema commits", table_name)

        kept = versions(table_name)
        ids = sorted(int(v["VersionId"]) for v in kept)

        print(f"\n=== answers [{label}]")
        print(f"    producer survived the commits:        {PRODUCER in after}")
        print(f"    release survived the commits:         {RELEASE in after}")
        print(
            "    metadata_location moved:              "
            f"{created.get('metadata_location') != after.get('metadata_location')}"
        )
        print(f"    the pinned version was v{pinned_version}, stamped {RELEASE}={pinned[RELEASE]}")
        print(f"    versions Glue still holds:            {ids}")
        print(f"    the pinned version is one of them:    {int(pinned_version) in ids}")


# ---------------------------------------------------------------- entry point


def main() -> int:
    try:
        glue.get_database(Name=DB)
    except glue.exceptions.EntityNotFoundException:
        pass
    else:
        print(f"database {DB} already exists; refusing to touch it", file=sys.stderr)
        return 1

    if "--iceberg" in sys.argv:
        bucket = make_bucket()
        glue.create_database(
            DatabaseInput={"Name": DB, "Description": "throwaway, pipeline-runtime#4"}
        )
        try:
            probe_iceberg(bucket)
        finally:
            if "--keep" not in sys.argv:
                cleanup()
                empty_and_delete_bucket(bucket)
                print(f"cleaned up: bucket {bucket} deleted")
        return 0

    glue.create_database(DatabaseInput={"Name": DB, "Description": "throwaway, pipeline-runtime#1"})
    try:
        # 1. CreateTable carrying producer, release, and one unrelated key.
        glue.create_table(
            DatabaseName=DB,
            TableInput=table_input(
                COLUMNS_V1,
                {PRODUCER: "card-ledger", RELEASE: "v4.11.0", "classification": "parquet"},
            ),
        )
        show("1. after CreateTable")

        # 2. UpdateTable adding a column, setting ONLY the producer key. This is
        #    the publishing job that forgot one key, which is the case that
        #    decides the design.
        glue.update_table(
            DatabaseName=DB, TableInput=table_input(COLUMNS_V2, {PRODUCER: "card-ledger"})
        )
        after_partial = show("2. after UpdateTable with Parameters={producer} only")

        # 3. UpdateTable again with no Parameters key at all.
        glue.update_table(DatabaseName=DB, TableInput=table_input(COLUMNS_V2, None))
        after_none = show("3. after UpdateTable with no Parameters at all")

        archived = versions()
        oldest = min(archived, key=lambda v: int(v["VersionId"]))

        print("\n=== answers")
        print(f"    release survived a partial update:  {RELEASE in after_partial}")
        print(f"    unrelated key survived:             {'classification' in after_partial}")
        print(f"    producer survived an omitted block: {PRODUCER in after_none}")
        print(f"    versions archived by default:       {len(archived)}")
        print(
            "    oldest archived version still carries release: "
            f"{oldest['Table'].get('Parameters', {}).get(RELEASE)!r}"
        )
    finally:
        if "--keep" not in sys.argv:
            cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
