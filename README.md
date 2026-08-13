# pipeline-runtime

A pipeline is a descriptor, not a program.

This is the runnable version of the executor described in two posts on
[cordata.tech](https://cordata.tech):

- **[The pipeline half, part 1 — a pipeline is a descriptor, not a program](https://cordata.tech/en/blog/pipelines-as-descriptors)**
- **[The pipeline half, part 2 — OpenLineage, Great Expectations, and what governance actually reads](https://cordata.tech/en/blog/pipeline-half-openlineage-gx)**

The posts argue that one small generic executor can run an arbitrary number of
pipelines declared as data, and that the metadata it emits is what a governance
layer reads. A reader has every right to want that executed rather than
described. So: no cloud account, no credentials, two commands.

## Five-minute quickstart

Python **3.12+** (the code uses `match` and PEP 695 `type` aliases).

```bash
git clone https://github.com/cordata-tech/pipeline-runtime && cd pipeline-runtime
python3.12 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"

./.venv/bin/python -m tools.seed                                    # build the local catalog + data
./.venv/bin/python -m pipeline_runtime example/domains/fraud/pipelines/transactions_scored.yml
```

```
[347b] descriptor  transactions-scored-daily (fraud)      apiVersion v1 OK
[347b] schema      fraud_raw.transactions                 pinned v7, catalog v7 OK
[347b] read        glue_table                             50,000 rows
[347b] step        filter_settled        (sql)            35,919 rows
[347b] step        score                 (sql)            35,919 rows
[347b] policy      sensitivity=high residency=eu subject_type=customer  resolved against ontology
[347b] expect      transactions_scored                    5 passed, 0 failed
[347b] write       fraud_curated.transactions_scored      iceberg, 1 partition key
[347b] emit        COMPLETE → lineage adapter             inputs=1 outputs=1
```

Now point the same command at a pipeline in a different domain, with a
different source technology, a different output format and a different failure
policy:

```bash
./.venv/bin/python -m pipeline_runtime example/domains/policy/pipelines/claims_ingest.yml
```

Nothing in the executor changed. That is the claim, and it is the only claim
this repo exists to make checkable.

The OpenLineage events both runs emitted are in `out/lineage.ndjson`.

## Watch it fail

The interesting path is the failure. Move the source table to v8 while the
descriptor still pins v7:

```bash
./.venv/bin/python -m tools.seed --drift
./.venv/bin/python -m pipeline_runtime example/domains/fraud/pipelines/transactions_scored.yml
```

```
[bc11] descriptor  transactions-scored-daily (fraud)      apiVersion v1 OK
[bc11] schema      fraud_raw.transactions                 pinned v7, catalog v8
[bc11] FAILED      SchemaDrift:                           fraud_raw.transactions is at v8,
                   descriptor pins v7. Diff: + merchant_category_code (VARCHAR, nullable).
                   Bump the pin to v8 to accept.
```

Nothing was read, nothing was written, nothing was published, and the error
names the exact column and the exact remedy. A `FAIL` event still reached the
lineage log carrying the reason — the point being that a supervisor can tell
"this run died on drift" apart from "nobody scheduled it".

`python -m tools.seed --clean` puts it back to v7.

## What is real and what is local

**Real**, and identical to the articles: the descriptor schema, the executor
loop, the error hierarchy, the three `on_failure` policies, the terminal-event
guarantee, the catalog version pin, LF-tag resolution against a
governance-owned ontology, the Great Expectations suite, and the OpenLineage
events including the provenance facet.

**Local stand-ins**, swapped inside `src/pipeline_runtime/backends/local.py`:

| The descriptor says | AWS would use | this repo uses |
| --- | --- | --- |
| `source.kind: glue_table` | Glue Data Catalog + Spark | a DuckDB table |
| `source.kind: dms_landing` | DMS CDC files on S3 | Parquet batches with `Op` / `_dms_seq` |
| `target.kind: iceberg` / `parquet` | Iceberg on S3 | partitioned local Parquet |
| catalog + version history | `glue.get_table().VersionId` | a `_catalog` schema in DuckDB |
| LF-tag ontology | `lakeformation.list_lf_tags()` | `example/ontology.json` |
| lineage transport | HTTP → the § 2 adapter → DataZone | a newline-delimited file |

The descriptors are **unchanged** between the two — byte for byte the ones
published in part 1 § 2, comments included, which
`tests/test_post_conformance.py` enforces. That is the substance of part 1 § 2
rule 1: a descriptor declares intent, not mechanism, so the mechanism can be
replaced underneath it. Swapping the backend is one entry in
`CORDATA_BACKEND`.

## Configuration

All of it is executor configuration, none of it is in a descriptor — which is
part 1 § 2 rule 3 applied to the runtime itself.

| Variable | Default | What it is |
| --- | --- | --- |
| `CORDATA_BACKEND` | `local` | which registry set to bind |
| `CORDATA_CATALOG` | `example/catalog.duckdb` | the metastore |
| `CORDATA_WAREHOUSE` | `example/warehouse` | where `s3://…` locations land |
| `CORDATA_ONTOLOGY` | `example/ontology.json` | the LF-tag vocabulary |
| `CORDATA_LINEAGE_OUT` | `out/lineage.ndjson` | where events are written |

## Tests

```bash
./.venv/bin/python -m pytest
```

Every test corresponds to a claim one of the articles makes in prose. If one
fails, either the code is wrong or the article is lying — both worth knowing.

- `test_descriptor.py` — everything the schema promises to reject
- `test_executor.py` — both descriptors end to end, one executor
- `test_failure_modes.py` — `block_publish`, `quarantine`, `warn`, schema
  drift, and the rule that outranks them: exactly one terminal event per run
- `test_lineage.py` — the emitted events, checked against the OpenLineage
  spec's own typing rather than against what looks reasonable
- `test_post_conformance.py` — extracts the code from the published markdown
  and compares. Skips unless `cordata-platform` is checked out alongside, which
  it will not be for anyone but us

## Where the articles were wrong

Building this turned up five of them, one substantive: part 2 attached the
data-quality result to a slot the OpenLineage spec does not define it for, so
no standard consumer would have read it. Four are now corrected in the published
posts; the fifth is a cosmetic ordering nit. All five, with the verification for
each, are in **[docs/post-corrections.md](docs/post-corrections.md)**.

That file is the most useful thing here. It is also the argument for the repo
existing: none of the five were visible from reading the drafts.

## What this is not

Not a product, and not a framework to adopt. It is an existence proof at the
smallest size that proves anything. Deliberately out of scope: real AWS
deployment, DataZone, MWAA, streaming sources, and anything resembling a
scheduler.

## Contributing

Issues and pull requests are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md)
for what is in scope. Corrections to the articles are especially welcome, and
the five above suggest there are more.

## Licence

MIT.
