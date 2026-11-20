## What this changes, and why

<!-- What a reader would notice, and the reasoning behind the change. The diff shows the how. -->

## Which commitment it keeps

<!-- Only if it touches a path CODEOWNERS lists separately: descriptor.py / errors.py,
the example descriptors, schemas/, or the release workflow. Otherwise delete this
section. -->

## Checklist

- [ ] A test for the behaviour, named for the claim it protects; for a bug fix, it fails without the fix
- [ ] `pytest`, `ruff check .`, `ruff format --check .` and `codespell` pass
- [ ] A line under `## Unreleased` in `CHANGELOG.md`
- [ ] Every commit is signed off (`git commit -s`)

## Did a coding agent help? (optional)

<!-- Agents are welcome, and telling us is welcome, not required. If one helped, a line on
what it did and what you checked yourself tells a reviewer where to look. Either way, you
are responsible for every line and have read every one. -->
