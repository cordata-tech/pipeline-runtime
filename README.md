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

A third pipeline, `example/domains/finance/pipelines/merchant_settlement.yml`,
is not from the articles. It is written against `cordata.tech/v2`, which names
both contracts a pipeline has: `source.contract` pins the schema version it
reads, and `target.contract` holds what it promises downstream. v1 keeps
the output contract in a top-level `contract:` block and leaves the input side
unnamed. v2 is registered beside v1 rather than replacing it, so the two
published descriptors run unchanged; the reasoning is on
[#1](https://github.com/cordata-tech/pipeline-runtime/issues/1).

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
                   descriptor pins v7. v8 was published by card-ledger release v4.12.0.
                   Diff: + merchant_category_code (VARCHAR, nullable).
                   Bump the pin to v8 to accept.
```

Nothing was read, nothing was written, nothing was published, and the error
names the exact column, the release that changed it, and the exact remedy. The
release comes from the catalog: every source-table version is published with a
producer and a release reference (`catalog.publish`), so the person reading the
failure knows which team to ask without having to go and find out. The same
pair travels outward on a successful run, as `source_published_by` and
`source_published_release` in the `cordata_provenance` facet, which is what
carries the provenance chain past the pipeline's own commit to the application
change behind its input. A `FAIL`
event still reached the lineage log carrying the reason — the point being that a
supervisor can tell "this run died on drift" apart from "nobody scheduled it".

`python -m tools.seed --clean` puts it back to v7.

## Catch it in the producer's build instead

`SchemaDrift` fires when a pipeline runs, which is after the application release
that changed the table has shipped. The same check can run the other way round,
in the application's CI: given the version it is about to publish, which
pipelines would it break?

```bash
./.venv/bin/python -m pipeline_runtime.consumers example/proposals/card-ledger-v5.0.0.yml example/domains
```

```
card-ledger release v5.0.0 proposes fraud_raw.transactions v8; the catalog is at v7
2 descriptors under example/domains read fraud_raw.transactions

BREAKS      merchant-settlement-daily (finance)  pins v7  owner finance-data@example.com
            example/domains/finance/pipelines/merchant_settlement.yml
            + merchant_ref (VARCHAR, not null)
            - merchant_id (VARCHAR, not null)   <- breaks

BREAKS      transactions-scored-daily (fraud)  pins v7  owner fraud-data@example.com
            example/domains/fraud/pipelines/transactions_scored.yml
            + merchant_ref (VARCHAR, not null)
            - merchant_id (VARCHAR, not null)   <- breaks

2 of 2 consumers fail this check
```

The exit status is 1, which is what fails the build. The proposal is a YAML file
holding the table, the producer, the release and the columns it would publish.

**The list of consumers is not maintained anywhere.** It is every descriptor
under the given paths whose source reads the table, so it comes from the same
pins the pipelines run on and cannot fall behind them; a pipeline added
tomorrow is on it as soon as its descriptor is merged. An organisation with
several repositories passes several checkouts. Readers that declare nothing —
ad-hoc SQL, notebooks, BI tools — are not on it, and no check here can see them.

**Not every change fails the build.** A removed or retyped column fails it, and
so does a column that was not null becoming nullable, because a pipeline
written against the pinned version can rely on each of those. An added column
does not: `example/proposals/card-ledger-v4.12.0.yml` exits 0 and lists both
pipelines as `BUMP`. They still stop at their next run with `SchemaDrift` until
their pins move, as part 1 § 4 intends, but a pipeline cannot pin a version
before the version exists, so failing the producer's build on it would block
every change.
A descriptor that reads the table but does not parse, or that pins a version
the catalog does not have, also fails the check, since either hides what that
consumer depends on.

**Existing tools.** The [Data Contract
CLI](https://github.com/datacontract/datacontract-cli)'s `breaking` command
already runs a producer-side compatibility check in CI, against a contract
document. This command's difference is where the list of consumers comes from:
the pins in the descriptors that read the table, not a document someone keeps
up to date. It checks shape only — columns, types and nullability — and
nothing about meaning, which a version check cannot see.

## What is real and what is local

**Real**, and identical to the articles: the descriptor schema, the executor
loop, the error hierarchy, the three `on_failure` policies, the terminal-event
guarantee, the catalog version pin, LF-tag resolution against a
governance-owned ontology, the Great Expectations suite, and the OpenLineage
events including the provenance facet.

The events also carry the classification the run resolved, as the standard
`TagsDatasetFacet`: the dataset a run writes carries its contract's `lf_tags`,
and the dataset it reads carries whatever the catalog already holds for that
table, which is usually nothing for a table an application publishes. A
pipeline never classifies the table it read — its own `lf_tags` describe what it
publishes. The reasoning is on
[#3](https://github.com/cordata-tech/pipeline-runtime/issues/3).

**Local stand-ins**, swapped inside `src/pipeline_runtime/backends/local.py`:

| The descriptor says | AWS would use | this repo uses |
| --- | --- | --- |
| `source.kind: glue_table` | Glue Data Catalog + Spark | a DuckDB table |
| `source.kind: dms_landing` | DMS CDC files on S3 | Parquet batches with `Op` / `_dms_seq` |
| `target.kind: iceberg` / `parquet` | Iceberg on S3 | partitioned local Parquet |
| catalog + version history | `glue.get_table().VersionId` | a `_catalog` schema in DuckDB |
| producer and release per version | keys in `TableInput.Parameters` on `UpdateTable`, archived with each `TableVersion` | `_catalog.versions` |
| LF-tag ontology | `lakeformation.list_lf_tags()` | `example/ontology.json` |
| lineage transport | HTTP → the § 2 adapter → DataZone | a newline-delimited file |

The producer row was measured against Glue rather than read off the API
reference: `UpdateTable` archives a new version by default, each `TableVersion`
keeps the `Parameters` it was written with, and an update replaces `Parameters`
wholesale, so every key the job leaves out is dropped. The transcript is in
[docs/evidence/glue-updatetable-parameters.md](docs/evidence/glue-updatetable-parameters.md).
`catalog.publish` therefore requires both values on every call and copies
nothing from the version before, which is what a publishing job against Glue has
to do.

The descriptors are **unchanged** between the two — fraud and claims byte for
byte the ones published in part 1 § 2, comments included, which
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

Every test corresponds to a claim one of the articles, or
[#1](https://github.com/cordata-tech/pipeline-runtime/issues/1), makes in
prose. If one fails, either the code is wrong or the claim is — both worth
knowing.

- `test_descriptor.py` — everything the schema promises to reject
- `test_descriptor_v2.py` — v2 names both contracts and moves nothing else, so
  a migrated descriptor runs exactly as the v1 one did
- `test_executor.py` — all three descriptors end to end, one executor
- `test_catalog.py` — every version carries its producer and release, nothing
  is carried forward between versions, and the drift message names each
  release since the pin
- `test_consumers.py` — the producer-side check: consumers found from the
  pins, and the exit status a CI job reads
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
