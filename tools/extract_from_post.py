"""Pull the published code blocks out of the article markdown.

The posts and this repo are two copies of the same design, and two copies drift.
Rather than hand-comparing them, this extracts the fenced blocks so
`tests/test_post_conformance.py` can check the repo against what readers
actually see.

The output is gitignored: a committed copy would be a third source of truth,
free to drift from both.

    python tools/extract_from_post.py [--posts DIR] [--out DIR]

Defaults assume cordata-platform is checked out alongside this repo. When it is
not — which is the normal case for anyone but us, since that repository is
private — the conformance tests skip rather than fail.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

SITE = pathlib.Path(__file__).resolve().parents[2] / "cordata-platform"
DEFAULT_POSTS = SITE / "content/blog"
POSTS = {
    "part1": "2026-08-10-pipelines-as-descriptors.md",
    "part2": "2026-08-12-pipeline-half-openlineage-gx.md",
}

FENCE = re.compile(r"```(\w+)\n(.*?)```", re.S)


def blocks(md: str, lang: str) -> list[str]:
    return [body for tag, body in FENCE.findall(md) if tag == lang]


def extract(posts_dir: pathlib.Path, out: pathlib.Path) -> int:
    missing = [name for name in POSTS.values() if not (posts_dir / name).is_file()]
    if missing:
        print(f"not found under {posts_dir}: {', '.join(missing)}", file=sys.stderr)
        return 1

    out.mkdir(parents=True, exist_ok=True)
    written = 0

    part1 = (posts_dir / POSTS["part1"]).read_text()
    py = blocks(part1, "python")

    model = next((b for b in py if "class Descriptor" in b), None)
    if model is None:
        print("no block containing `class Descriptor` — did part 1 change?", file=sys.stderr)
        return 1
    (out / "descriptor_model.py").write_text(model)
    written += 1

    for label, needle in (("executor", "def execute("), ("errors", "class PipelineError")):
        if body := next((b for b in py if needle in b), None):
            (out / f"{label}.py").write_text(body)
            written += 1

    descriptors = [b for b in blocks(part1, "yaml") if "apiVersion:" in b]
    if not descriptors:
        print("no descriptor YAML blocks found in part 1", file=sys.stderr)
        return 1
    for i, body in enumerate(descriptors, 1):
        name = re.search(r"^\s*name:\s*(\S+)", body, re.M)
        (out / f"descriptor-{name.group(1) if name else i}.yml").write_text(body)
        written += 1

    part2 = (posts_dir / POSTS["part2"]).read_text()
    if emit := next((b for b in blocks(part2, "python") if "def emit(" in b), None):
        (out / "emit.py").write_text(emit)
        written += 1

    print(f"{written} blocks → {out}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--posts", type=pathlib.Path, default=DEFAULT_POSTS)
    ap.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parents[1] / "tests/fixtures/published",
    )
    return extract(*vars(ap.parse_args()).values())


if __name__ == "__main__":
    raise SystemExit(main())
