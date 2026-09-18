"""Does Glue's `UpdateTable` replace a table's `Parameters` wholesale?

The one question in this repo that a local stand-in cannot answer, because it is
about the real catalog's behaviour rather than about the descriptor. The answer
decides whether `catalog.publish` may carry a producer forward from the previous
version, so it is measured rather than assumed — see
`docs/evidence/glue-updatetable-parameters.md` for the run it produced.

Unlike everything else here this needs an AWS account and `boto3`, which is why
it is not a dependency and not part of the quickstart:

    pip install boto3
    AWS_PROFILE=… python -m tools.glue_probe          # run and clean up
    AWS_PROFILE=… python -m tools.glue_probe --keep   # leave the database behind

It creates one throwaway database, writes three table versions into it, and
deletes the database again. Nothing else in the account is touched, and it
refuses to run if a database of that name already exists.
"""

from __future__ import annotations

import contextlib
import json
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


def show(label: str) -> dict:
    table = glue.get_table(DatabaseName=DB, Name=TABLE)["Table"]
    params = table.get("Parameters", {})
    print(f"\n--- {label}")
    print(f"    VersionId  {table.get('VersionId')}")
    print(f"    columns    {[c['Name'] for c in table['StorageDescriptor']['Columns']]}")
    print(f"    Parameters {json.dumps(params, sort_keys=True)}")
    return params


def versions() -> list[dict]:
    found = glue.get_table_versions(DatabaseName=DB, TableName=TABLE)["TableVersions"]
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


def main() -> int:
    try:
        glue.get_database(Name=DB)
    except glue.exceptions.EntityNotFoundException:
        pass
    else:
        print(f"database {DB} already exists; refusing to touch it", file=sys.stderr)
        return 1

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
