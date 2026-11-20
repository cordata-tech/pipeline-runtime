# AGENTS.md

Instructions for a coding agent, or a person, changing this repository. They are the
short form of [CONTRIBUTING.md](CONTRIBUTING.md), which gives the reasons; read it before
anything substantial. Where the two ever disagree, CONTRIBUTING.md is right and this file
is the bug.

This repository exists to make the claims in two published articles checkable. It is a
runnable reference implementation of the descriptor-driven pipeline executor they
describe, and nothing wider than that.

## How a change gets in

- **Fork, branch, open a pull request against `main`.** Contributors do not push to
  `main`, and do not create, move or delete a `v*` tag: pushing one publishes to PyPI,
  where a version can never be reused. Releases are the maintainer's.
- **The checks have to pass**, the same ones CI runs on Python 3.12 and 3.13:

  ```bash
  python3.12 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
  ./.venv/bin/python -m tools.seed
  ./.venv/bin/python -m pytest
  ./.venv/bin/ruff check . && ./.venv/bin/ruff format --check .
  ./.venv/bin/codespell
  # or once: pre-commit install, to run ruff and codespell before each commit
  ```

- **Add a test for the behaviour you change**, named for the claim it protects ("the
  descriptor is named relative to its work tree"), not for the function it calls.
- **Add a line under `## Unreleased` in `CHANGELOG.md`**, saying what a user would
  notice.
- **Sign off every commit** with `git commit -s`. The `Signed-off-by:` line is your
  statement under the [Developer Certificate of Origin](https://developercertificate.org)
  that you may contribute the change under this project's MIT licence; the DCO check
  fails a pull request where a commit lacks one matching its author. `git rebase
  --signoff main` adds the missing ones.
- **Sign the commit too, if you can** — see CONTRIBUTING.md for why `main` asks for it
  and what happens if you would rather not. Signing and signing off are different
  things: the signature proves who made the commit, the sign-off is what they certify
  about it.
- **Agents are welcome, and saying so is welcome too, not required.** If one helped, a
  line in the pull request on what it did and what you checked yourself helps a
  reviewer know where to look; a trailer such as `Assisted-by: <tool>` or
  `Co-Authored-By:` is fine as well. Whoever opens the pull request is responsible for
  every line in it and has read every one.
- **A pull request that touches a path listed separately in `.github/CODEOWNERS`** says
  in its description which commitment below it keeps.

## Ask before changing these

An agent asks the person it works for before modifying any of these, naming the path
and what the change would do to a claim the published articles or a downstream project
make — for example: *"This changes a field `descriptor.py` publishes, which `qedro`
generates its Art. 30 facet schema from. Do you want that?"*

- `src/pipeline_runtime/descriptor.py` and `src/pipeline_runtime/errors.py` — published
  in part 1 § 3, compared field for field against the markdown by
  `tests/test_post_conformance.py`. `cordata-tech/qedro` also generates its published
  Art. 30 facet schema from `descriptor.py`'s `Processing` model, so a change here is a
  change to somebody else's published schema, not only to this repository's article.
- `example/` — the two pipelines' descriptors are byte-identical to the ones the
  articles quote, comments included.
- `schemas/` — the OpenLineage facet schemas the articles describe and
  `docs/post-corrections.md` discusses.
- `.github/workflows/release.yml` — publishes to PyPI on a `v*` tag.

## Never

The repo's four scope limits, and the changes that most often erode them while looking
reasonable.

- **Never add real AWS deployment code** — no DataZone, no MWAA, no Terraform. A reader
  has to be able to run this with no account and no bill.
- **Never grow this into a product** — no scheduler, UI, permission model or server.
  The repo stays the size of the argument it exists to check.
- **Never add a descriptor field with the same value in every example pipeline.** Part 1
  § 2 rule 3: that is executor configuration wearing a descriptor's clothes. A new field
  needs a pipeline that genuinely varies in it.
- **Never add a heavier dependency** — no Spark, no real Iceberg catalog, no database
  server. The five-minute quickstart is a constraint, not an aspiration.
- **Never edit an example descriptor's values or comments without updating the article
  it quotes and the test that checks the two still agree.** `docs/post-corrections.md`
  is the record of every deliberate gap between them; an undocumented one is a bug in
  this repository, not in the article.

## How to work here

What has caught real defects in this repository, rather than general advice:

- **Test what the installed wheel does, away from the checkout, not only what an
  editable install does.** `example_root()` and `ontology_path()` both silently found
  the repository's own working directory instead of the packaged copy, because the
  editable install's assumption was never checked against a real install — see the
  release workflow's clean-venv steps and `tests/test_demo.py`'s `tmp_path` fixture.
- **A git remote URL can carry a credential before it reaches a public record.** A CI
  checkout commonly leaves a token in `https://x-access-token:<token>@github.com/…`;
  `_normalize_remote()` in `emit.py` strips it before any lineage event is written.
- **A lifecycle facet is a claim about what this run did, not about the output's kind.**
  Attach `lifecycleStateChange: OVERWRITE` only to a dataset a writer actually replaced,
  never to every `COMPLETE` output — a quarantined write and an appended one are not the
  same claim.
- **Check an OpenLineage class against `openlineage-python` itself before writing to
  it.** Its client classes are `attrs`, not `dataclasses` — `attr.evolve()`, not
  `dataclasses.replace()` — and a plausible-looking facet name that does not exist is
  the kind of error that looks most like working code.
- **One concern per pull request.** No reformatting, renaming or tidying along the way;
  they hide the change a reviewer needs to see.

## Where things are

- `src/pipeline_runtime/descriptor.py`, `errors.py` — the model published in the
  articles.
- `src/pipeline_runtime/emit.py` — lineage emission: provenance, processing and
  lifecycle facets.
- `src/pipeline_runtime/backends/` — binds a descriptor's `kind` keys to an
  implementation; a second backend is welcome, the published descriptors must run
  against it unchanged.
- `example/domains/` — the two example pipelines, byte-identical to the published
  descriptors.
- `docs/post-corrections.md` — every place this repository and the published articles
  are known to disagree, and why.
- `tools/seed.py`, `tools/extract_from_post.py` — generate the example warehouse, and
  extract the published markdown for `test_post_conformance.py`.

Python 3.12 is the floor; `match` and PEP 695 `type` aliases are used throughout, and
workarounds for older versions are not needed.
