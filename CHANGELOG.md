# Changelog

## 0.2.8 — 2026-10-25

**The job now carries the standard `sourceCodeLocation` facet, so the commit a
run executed is readable without knowing `cordata_provenance`** (#13). It had
only ever been in this project's own facet, and OpenLineage has
`SourceCodeLocationJobFacet` for exactly this — so a reader that did not know
the custom facet found nothing, even on a run that reported the commit twice
over. `qedro` was in that position: it took a step's commit from the standard
facet alone and said *"no code location was emitted"* against events carrying
one.

It carries `type: "git"`, `repoUrl`, `url`, `path` and `version`, built from the
provenance facet rather than from git a second time, so the commit and the path
cannot disagree between the two facets. Both URL fields name the repository
rather than the file: a per-file URL needs the host's path template
(`/blob/<sha>/<path>` on GitHub, `/-/blob/` on GitLab, `/src/` on Bitbucket),
which a remote does not reveal, and a wrong one is a dead link that reads as
authoritative.

**A remote is normalised before it is emitted, and credentials are dropped.**
`git remote get-url` returns whatever is configured, and an https remote can
carry a token in its userinfo — `https://x-access-token:…@github.com/org/repo`
is what a CI checkout often leaves behind — so emitting it raw would publish a
credential to every lineage consumer. SSH remotes are rewritten to https, and a
remote that is not an http URL at all (a local path, `file://`) means the facet
is absent rather than present with a filesystem location, which is what #12 took
off the wire.

**The facet is absent rather than partial**, which is #11's rule again: it needs
a work tree, a commit and an http remote, and a run from an installed copy has
none of the three. So `cordata_provenance` remains the field that always carries
the commit when there is one, and it keeps `descriptor_git_commit` for a second
reason — `descriptor_git_commit_signed` judges that commit and has to have it to
point at. `schemas/provenance.json` now says why each of the three remaining
fields has no standard home: no facet says whether a signature verified, and a
commit is not the bytes that ran.

Being a job facet, it is inherited by the `.validate` event along with
`processing`, which #9 made true and a test now holds.

## 0.2.7 — 2026-10-25

**`pipeline-runtime demo DIR` works from an editable install whatever the
working directory** (#14). It failed in a clone — the one place a contributor
runs it — unless the working directory happened to be the repository root, and
the message blamed the install:

```text
FileNotFoundError: no descriptor for the fraud domain under example — an install
missing part of its example, which the release workflow checks for
```

Nothing was missing. An editable install maps no `pipeline_runtime.example`, and
both fallbacks were relative paths resolved against the working directory, while
`demo` exists to be pointed at some other directory. The example is now found
from the module's own location instead. Searching upward from the working
directory would have been the wrong repair, since inside somebody else's
checkout it would find their `example/`.

**The same defect was in `policy.ontology_path()`**, so the demo depended on the
working directory in two places rather than one: with the descriptors found, a
run then reported *no LF-tag ontology at example/ontology.json* while the file
sat beside the package. Both now resolve through `example_root()`.

`example_root()` raises and names the directories it looked in when there is
genuinely no example, so "an install missing part of its example" is reserved
for an install that is missing part of its example.

Two tests changed rather than being added, which is the part worth reading. The
demo fixture chdir'd to the repository before each run, and
`test_without_a_packaged_copy_the_missing_file_is_reported` asserted the relative
path that does not resolve — so the suite had both encoded the defect and
hidden it. The fixture now runs from an unrelated directory, and the ontology
test asserts the file is found and readable, with a second test for the case
where there is no example anywhere.

One consequence beyond the command working: since #11 and #12 an installed copy
reports no commit and no work tree, so a clone is the only place a run can put
`descriptor_git_commit` and a signature verdict on the wire — and the demo is
how that capture is produced in one command. From an editable install it now
emits `example/domains/fraud/pipelines/transactions_scored.yml` with
`descriptor_git_commit_signed: true`.

## 0.2.6 — 2026-10-25

**`cordata_provenance.descriptor_path` is a relative path again, as
`schemas/provenance.json` has promised since the facet was published** (#12).
It was `str()` of whatever path the caller handed the CLI, so the field's value
depended on how a run was invoked rather than on which descriptor ran, and from
an installed copy it carried the host's filesystem layout —
`/private/tmp/…/site-packages/pipeline_runtime/example/domains/fraud/…` — into
every event.

It is now relative to the root of the work tree holding the descriptor, and
outside a work tree relative to the parent of its domain directory, which
`run()` already treats as the domain root. The two cases give
`example/domains/fraud/pipelines/transactions_scored.yml` from this repository
and `fraud/pipelines/transactions_scored.yml` from a wheel. A descriptor under
`site-packages` takes the second rule even when a work tree encloses it,
because the usual Python layout puts the virtual environment inside the
project, and the work tree found there is the reader's own repository rather
than the example's.

**For the invocation the README documents, the value does not change**, which is
why `docs/evidence/provenance-names-the-release.md` is still accurate. What
changes is an absolute invocation, and any run from an installed copy. A
consumer that stored the old value sees a different string for the same
descriptor; `descriptor_sha256` still proves which bytes ran and is unaffected.

The tests now assert the value rather than the key's presence. That was the gap
that let this stand through five releases:
`test_the_facet_matches_the_schema_it_publishes` compares the emitted key set
with the schema's properties, so nothing held a field to what its own
description promised.

## 0.2.5 — 2026-10-23

Two things a consumer reads off the wire change, both found by `qedro` reading
the 0.2.4 capture rather than by a test here.

**The assertion event's input now carries the contract's `tags` facet** (#10).
The dataset it names is this pipeline's own target, written and classified one
event earlier, so it arrived classified on the terminal event and unclassified
on the assertion event, and a consumer counting datasets per activity called
the pipeline's own output unclassified. `dataset()` still withholds tags from a
table the pipeline merely read — that classification is not this descriptor's
to assert — which is the same rule rather than an exception to it. Measured over
`pipeline-runtime demo` output with `qedro` 0.7.0: Art. 30(1)(c) and (e) are now
reported for 6 of 6 activities instead of 3, and 2 of 5 datasets are
unclassified instead of 5 — the two left are the raw source tables, which no
pipeline here claims to classify.

**`descriptor_git_commit_signed` is absent, not `false`, when no commit was
found** (#11). A run from an installed copy has no repository, so
`descriptor_git_commit` was already dropped on the way out while the flag beside
it serialised as `false` — a negative signature verdict about a commit nothing
named, which `qedro` reported as a step that *"ran under a commit that was
reported as not signed"*. The two fields now travel together or not at all, and
`schemas/provenance.json` says so as `dependentRequired` rather than listing the
flag in `required`. A consumer that stored `false` for runs outside a checkout
will stop receiving it; `descriptor_sha256` still proves which bytes ran.

`descriptor_path` is a third finding from the same reading, left open as #12: it
is whatever path the caller passed, while the schema promises one relative to
the repository root, and from an installed copy it carries the host's
filesystem layout. Changing it changes a value every stored event carries, so it
gets its own release.

## 0.2.4 — 2026-10-23

**The validation event's job now carries the pipeline's `processing` facet, so a
record rebuilt from events emitted before this release changes** (#9). Until
now the `OTHER` event carrying the assertion result built its job from scratch,
`<name>.validate` with no facets at all, and the purpose and legal basis the
descriptor declares travelled only on the terminal event. Read by `qedro` 0.7.0
over `pipeline-runtime demo` output, three of the six activities in the Art. 30
record came out with no purpose and no legal basis, and the record withheld its
mark on that count; they now carry both, and the only line it still withholds
for is the undeclared controller, which is the reader's to set.

The validation job is the pipeline's job with a longer name, because checking a
pipeline's output is part of the same processing activity: the purpose and
lawful basis that justify writing the rows justify checking them. The
alternative — a distinct declared purpose for validation — would have the
runtime assert something no descriptor says, and is argued on the issue.

This is the shape part 2 § 4 published from the start, so the repository was the
one that diverged. The article's own spelling of it does not run, though:
`dataclasses.replace` raises `TypeError` on the client's `Job`, which is an
attrs class, and `attr.evolve` is the equivalent that works.
`docs/post-corrections.md` § 6 has the working.

## 0.2.3 — 2026-10-23

**The quickstart no longer needs a clone.** `pipeline-runtime demo DIR` seeds the
example — the local catalog, the two source tables and the DMS batches — and then
runs all three pipelines against it, writing the catalog, the warehouse and
`out/lineage.ndjson` into `DIR` and nowhere else. Since 0.2.0 the descriptors
travelled in the wheel while the data they read did not, so every walkthrough
still began with `git clone`; the seeding code now lives in the package as
`pipeline_runtime.seed`, which is what `demo` calls (#8).

It refuses a directory holding anything it did not write, naming the files, and
without the `run` extra it names the missing extra the way a pipeline run already
does. `tools/seed.py` keeps working, `--drift` included, as a wrapper over the
moved code — CI, the tests and the README's drift scenario all call it.

The release workflow now installs the wheel into a clean environment and runs
`demo` there, so a release whose own demo cannot run does not publish. The test
suite cannot cover that path: an editable install maps no
`pipeline_runtime.example`, so only a built wheel reads the packaged descriptors.

## 0.2.2 — 2026-10-18

**Two emitted identities change, so a lineage consumer that stored the old ones
will see new datasets and new type names** (#7).

An input dataset is now named by the domain that owns the table, read from the
catalog's `domain` tag, instead of by the domain of the pipeline reading it.
Before this, a table read by two domains was emitted under two namespaces — the
finance pipeline's read of `fraud_raw.transactions` appeared as
`cordata://finance` beside fraud's `cordata://fraud` — and OpenLineage treats
those as two datasets. The published pipelines each read their own domain's
table, so their input namespaces do not change. `tools.seed` records the owner
of both source tables; a source with no `domain` tag falls back to the reader's
domain, with a `lineage` trace line saying so.

Output schema facets now use the catalog's type names (`VARCHAR`, `DOUBLE`,
`TIMESTAMP`) instead of pandas dtype names (`STR`, `FLOAT64`,
`DATETIME64[NS]`), so a column can be followed from an input to the output it
fed. Inputs already used them.

Inputs also gain a `tags` facet carrying that `domain` tag, because it is what
the catalog now holds for them.

## 0.2.1 — 2026-10-18

Metadata and the README only; the code is identical to 0.2.0.

The project page gains four badges — version, CI on `main`, the supported
Python range and the licence — each reading live data rather than a number
typed into the README, so a release cannot leave them stale.

The distribution now carries `Programming Language :: Python :: 3.12` and
`3.13` classifiers, which is what PyPI filters on and what a
`pypi/pyversions` badge reads. Those versions are now written in three
places — `requires-python`, the classifiers and the CI matrix — so
`tests/test_packaging.py` asserts that the classifiers are exactly the
versions CI runs and that the lowest one is the floor `requires-python`
promises. This release exists so that the badge can read the classifiers
instead of the file they come from (#6).

## 0.2.0 — 2026-10-18

**Running a pipeline now needs the `run` extra: `pip install
'cordata-pipeline-runtime[run]'`.** That is the one change an existing user has
to act on. The base install is now the descriptor model, the policy resolution
and the example, depending on Pydantic and PyYAML only — seven packages rather
than the whole execution stack — because the first dependent, `catalog-mcp`
(cordata-tech/platform#26), reads the model and never runs a pipeline. Without
the extra, `pipeline-runtime` names the missing piece and how to install it,
instead of failing on an import of DuckDB.

**The example travels in the wheel.** The ontology and every example domain's
descriptors, SQL, expectations and baselines are packaged as
`pipeline_runtime.example`, mapped from `example/` where it already is rather
than copied, so they stay byte-identical to the published descriptors. The
seeded catalog and warehouse are generated and stay out. `policy.ontology_path()`
keeps reading `example/ontology.json` in a clone and `CORDATA_ONTOLOGY` wherever
it is set; an installed copy with neither falls back to the packaged ontology.

`tests/test_packaging.py` holds both halves of the split and the fallback, and
the release workflow now installs the wheel twice — base and `[run]` — and
fails if the packaged domains differ from the repository's.

## 0.1.0 — 2026-10-18

The first release to PyPI, as **`cordata-pipeline-runtime`**. The repository has
been runnable from a clone since 2026-08-13; this release makes the descriptor
model, the executor and the policy resolution installable by name, so that other
tools can depend on them rather than copy them. The first such dependent is
`catalog-mcp` (cordata-tech/platform#26), which returns the descriptor models
from its tools so that its output schemas come from this package rather than
from a restatement of it.

Install it as `cordata-pipeline-runtime`. The bare `pipeline-runtime` on PyPI is
an unrelated package, and a dependency line using that name installs cleanly and
fails at import.

**What the wheel does not carry is the example data.** `example/` — the
descriptors, the ontology and the seeded catalog the quickstart runs against —
stays in the repository, because the example descriptors are byte-identical to
the ones the articles publish. The quickstart therefore still starts from a
clone. An installed copy reads its ontology and catalog from the paths in
`CORDATA_ONTOLOGY` and `CORDATA_CATALOG`, which default to the repository layout.
