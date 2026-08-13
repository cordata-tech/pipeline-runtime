# Contributing

Issues and pull requests are welcome. This page is mostly about scope, because
the fastest way to waste your afternoon here is to send something good that
does not belong.

## What this repo is for

Making the claims in two articles checkable. That is the whole remit, and it is
what decides every scope question below.

## Especially welcome

**Corrections to the articles.** Five are already listed in
[docs/post-corrections.md](docs/post-corrections.md), and none of them were
visible from reading the drafts — they turned up because something had to run.
If you find a sixth, open an issue. You do not need a patch; the claim and how
you checked it is enough.

**A failing test for a claim that does not hold.** More useful than a fix,
because it says precisely which sentence is wrong.

**A second backend.** `src/pipeline_runtime/backends/` binds the descriptors'
`kind` keys to implementations. The published descriptors must run against it
unchanged — that constraint is the point of the directory, not an obstacle to
work around.

## Out of scope

- **Real AWS deployment.** No DataZone, no MWAA, no Terraform. A reader has to
  be able to run this with no account and no bill.
- **Making it a product.** Scheduling, a UI, a permission model, a server. This
  repo stays the size of the argument it exists to check.
- **New descriptor fields.** Part 1 § 2 rule 3: anything with the same value in
  every descriptor is executor configuration wearing a descriptor's clothes. A
  new field needs a pipeline that genuinely varies in it.
- **Heavier dependencies.** Spark, a real Iceberg catalog, a database server.
  The five-minute quickstart is a constraint, not an aspiration.

## Ground rules

**`descriptor.py` and `errors.py` are the articles' code.** They are published
in part 1 § 3, and `tests/test_post_conformance.py` compares them field for
field against the markdown. Changing them means changing an article that
readers have already read, so it needs a reason that survives that.

**The example descriptors are byte-identical to the published ones**, comments
included. The comments explain which four fields differ between the two
pipelines; a repo that drops them has dropped the explanation.

**Every deliberate divergence gets an entry in `docs/post-corrections.md`**,
and a test that asserts the divergence still exists. If the article is later
corrected, that test fails and the note comes out — which is the mechanism that
stops the two drifting apart silently.

**Python 3.12+.** `match` and PEP 695 `type` aliases are used throughout.

## Running things

```bash
python3.12 -m venv .venv && ./.venv/bin/pip install -e ".[dev]"
./.venv/bin/python -m tools.seed
./.venv/bin/python -m pytest
./.venv/bin/ruff check . && ./.venv/bin/ruff format --check .
```

CI runs exactly those. `test_post_conformance.py` skips in CI, because it needs
the site repository, which is private.

## How a change gets in

Fork, branch, open a pull request. That is the only route — `main` takes no
direct pushes from anyone but the maintainer — and it needs no special access,
so there is nothing to ask for before you start.

## Commits, and the one bit of friction

`main` requires **signed commits**. Not hygiene theatre: part 2 § 4 publishes
`descriptor_git_commit_signed` on every lineage event and calls a run whose
provenance points at an unsigned commit a finding. Publishing that and not doing
it would be a strange position to hold.

Signing is a one-time setup and worth having anyway —
[GitHub's guide](https://docs.github.com/en/authentication/managing-commit-signature-verification)
covers SSH signing, which needs no GPG keyring:

```bash
git config --global gpg.format ssh
git config --global user.signingkey ~/.ssh/id_ed25519.pub
git config --global commit.gpgsign true
```

Then add the same public key to your GitHub account a second time, as a
**signing** key — authentication keys and signing keys are separate lists, and
an auth key alone will not verify.

**If you would rather not, send the pull request anyway.** The rule is on the
branch, not on you: unsigned work gets landed with `git commit --author=` so the
commit keeps your name and email in the log and in `git shortlog`, with a
maintainer signature on top. You lose nothing but the green *Verified* badge.
Nobody's correction is going to be turned away over a git config.
