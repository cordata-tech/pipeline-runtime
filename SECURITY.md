# Security

## Reporting

Email **privacy@cordata.tech**, or open a private advisory through GitHub's
*Report a vulnerability* on this repository. Please do not open a public issue
for anything exploitable.

Expect an acknowledgement within three working days. If a fix is warranted it
ships as a patch release with an entry in the changelog naming what was wrong;
credit is given unless you would rather it were not.

Supported: the most recent `0.x` release. There is no long-term support branch
before `1.0`.

## What this tool can reach, by construction

**It writes, deliberately, where you point it.** This is a reference
implementation of a pipeline executor, not a read-only reader: a run writes a
DuckDB catalog and an Iceberg or Parquet target under the paths `pipeline-runtime
demo` or a descriptor names, and a quarantined write lands beside the target it
was meant for, never inside it — `tests/test_failure_modes.py` asserts that
directly. It writes nothing else: no network call outside emitting lineage to
the console or the path configured for it, and no credential of its own.

**A git remote is emitted without any token it carried.** An emitter or a CI
checkout can leave a credential in a remote URL —
`https://x-access-token:<token>@github.com/…` is the common case —
`_normalize_remote()` in `emit.py` strips the userinfo before the
`sourceCodeLocation` facet is built, and rejects anything that is not an
`http(s)` URL outright rather than guess at a credential's shape.
`tests/test_lineage.py::test_a_remote_is_emitted_as_an_http_url_naming_no_secret`
checks this for every form a remote takes.

**A descriptor's path is named relative to its own work tree, never the
reader's.** `_work_tree()` and `_work_tree_root()` resolve `descriptor_path`
against the repository the descriptor lives in; a descriptor installed inside
someone else's checkout (for example, under their `.venv`) is never named after
that reader's own repository layout.

**User documents are parsed, never executed.** A pipeline descriptor is YAML
loaded with `yaml.safe_load`, so nothing in a descriptor can construct a Python
object or run code.

## What is in the lineage it emits

An OpenLineage event from this project carries dataset and job names, the
descriptor's classification tags, the commit the run's provenance points at
(and whether that commit's signature verified), and the lifecycle facet for
whatever a writer actually replaced. None of it is the data the pipeline
processes.

## Not a compliance verdict

Running this project proves that a pipeline executed against a descriptor and
emitted the lineage it claims to; it is not a statement that any obligation —
legal, contractual or otherwise — is met. `descriptor_git_commit_signed` says a
commit's signature verified, not that the pipeline it authorised is correct.

## Dependencies

Two at the base install — `pydantic` and `pyyaml` — chosen partly for how
little each brings with it; a tool reading the model (`catalog-mcp`, for
example) needs neither more. The `run` extra adds the execution stack —
`duckdb`, `pandas`, `pyarrow`, `great-expectations`, `openlineage-python` — and
is only needed to run a pipeline, not to read its model.

## Scope

In scope: anything that lets a crafted descriptor or a crafted OpenLineage
input cause code execution, a file write outside the target or quarantine path
a descriptor names, a credential leaking into an emitted lineage event, or an
emitted event that claims a commit's signature verified when it did not.

Out of scope: the contents of the data a pipeline processes, and anything
requiring an attacker who can already run code as you.
