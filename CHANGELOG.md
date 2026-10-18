# Changelog

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
