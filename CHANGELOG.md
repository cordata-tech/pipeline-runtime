# Changelog

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
