# Contributing

Issues and pull requests are welcome. This page is mostly about scope, because
the fastest way to waste your afternoon here is to send something good that
does not belong. If you work with a coding agent, point it at
[AGENTS.md](AGENTS.md), the short form of this page.

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
./.venv/bin/codespell
```

CI runs exactly those. `test_post_conformance.py` skips in CI, because it needs
the site repository, which is private. `pre-commit install` (once) runs ruff
and codespell, at the versions CI uses, before each commit.

## How a change gets in

Fork, branch, open a pull request. That is the only route — `main` takes no
direct pushes from anyone but the maintainer — and it needs no special access,
so there is nothing to ask for before you start. A merge needs CI to pass and
an approving review from a code owner, given after the last push.
`.github/CODEOWNERS` requests that review automatically, and lists separately
the paths where a change is easy to make and expensive to get wrong — the
articles' code, the example descriptors, the schemas, the release workflow. A
pull request touching one of those is worth a sentence in its description
about which commitment it keeps.

**Coding agents are welcome.** Saying that one helped is welcome too, though
not required: a line in the pull request on what it did and what you checked
yourself tells a reviewer where to look harder, and a commit trailer such as
`Assisted-by: <tool>` or `Co-Authored-By:` is fine. Whoever opens the pull
request is responsible for every line in it, whatever wrote it, and has read
every one.

## Sign-off (DCO)

**Sign off every commit** with `git commit -s`, which adds a `Signed-off-by:`
line. It certifies, under the [Developer Certificate of
Origin](https://developercertificate.org), that you wrote the change or
otherwise have the right to submit it under this project's **MIT** licence.
The DCO check fails a pull request with a commit whose sign-off does not match
its author; `git rebase --signoff main` adds the missing ones. Signing and
signing off are different things: the signature below proves who made the
commit, the sign-off is what they certify about it.

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
