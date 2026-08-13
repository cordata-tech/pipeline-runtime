# cordata-pipeline-runtime

Runnable reference implementation of the descriptor-driven pipeline executor
described in **The pipeline half, part 1 — a pipeline is a descriptor, not a
program** on [cordata.tech](https://cordata.tech).

> **Status: pre-launch scaffold.** Not yet public, not yet complete. Tracked in
> [cordata-tech/platform#23](https://github.com/cordata-tech/platform/issues/23),
> which ships this repo *after* both parts of the post are live.

## Why this repo exists

The post claims a small generic executor can run an arbitrary number of
pipelines declared as data. A reader has every right to want that claim
executed rather than described — so this is the version that runs, locally,
from a clean clone, with no cloud account.

AWS services are swapped for local equivalents (DuckDB and local Parquet for
Glue and Spark; a JSON file for the LF-tag ontology). The *shape* is the point;
the services are not.

## The post is the source of truth

Code is **not** hand-copied out of the article. Hand-copying desynchronises the
moment either side is edited, and a reference repo that contradicts the post it
references is worse than no repo. Instead:

```bash
python tools/extract_from_post.py     # pulls the model + descriptors from the .md
python -m pytest                      # asserts the published code still holds up
```

`tools/extract_from_post.py` reads the fenced blocks straight out of
`cordata-platform/content/blog/2026-08-10-pipelines-as-descriptors.md` (assumed
checked out alongside this repo) and writes them to `tests/fixtures/`, which is
gitignored precisely so it can never drift.

## What the conformance suite checks

Every test corresponds to a claim the article makes in prose. If one fails,
either the code is wrong or the article is lying — both worth knowing.

- Both published descriptors validate against the published model — no field
  drift between the YAML in § 2 and the schema in § 3
- The two descriptors genuinely exercise different readers and writers, which is
  the *one executor, many descriptors* claim
- The model rejects all six things the post promises it rejects: a typo'd key,
  an invented `legal_basis`, a step with both `query_file` and `module`, a step
  with neither, a `schema_ref` missing its `@vN` pin, a malformed
  `metadata.name`
- Descriptor paths stay domain-root relative — a regression guard for a real
  bug found during review, where the expectation suite resolved one directory
  too deep

## Requirements

Python **3.12+**. The published model uses PEP 695 `type` aliases (3.12),
`Self` (3.11), and `match` (3.10).

```bash
python -m venv .venv && ./.venv/bin/pip install -e ".[test]"
```

## Still to build

See [platform#23](https://github.com/cordata-tech/platform/issues/23) for the
full inventory. In short: the executor loop, the reader/writer/step registries,
the DuckDB-backed catalog and policy resolvers, and a fixture that reproduces a
`SchemaDrift` failure.

## Licence

MIT. Issues and pull requests welcome.
