"""Extract the descriptor model and example descriptors from the published post.

The blog post is the source of truth, not this repo. Rather than hand-copying
code out of the article — which silently desynchronises the moment either side
is edited — this script pulls the fenced blocks straight out of the markdown so
the repo can always be regenerated from whatever the post currently says.

Usage:
    python tools/extract_from_post.py [--post PATH] [--out DIR]

Defaults assume cordata-platform is checked out alongside this repo.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

DEFAULT_POST = (
    pathlib.Path(__file__).resolve().parents[2]
    / "cordata-platform/content/blog/2026-08-10-pipelines-as-descriptors.md"
)

FENCE = re.compile(r"```(\w+)\n(.*?)```", re.S)


def blocks(md: str, lang: str) -> list[str]:
    return [body for tag, body in FENCE.findall(md) if tag == lang]


def extract(post: pathlib.Path, out: pathlib.Path) -> int:
    if not post.is_file():
        print(f"post not found: {post}", file=sys.stderr)
        return 1

    md = post.read_text()
    py = blocks(md, "python")
    yml = blocks(md, "yaml")

    model = next((b for b in py if "class Descriptor" in b), None)
    if model is None:
        print("no block containing `class Descriptor` — did the post change?", file=sys.stderr)
        return 1

    descriptors = [b for b in yml if "apiVersion:" in b]
    if not descriptors:
        print("no descriptor YAML blocks found", file=sys.stderr)
        return 1

    (out / "fixtures").mkdir(parents=True, exist_ok=True)
    (out / "fixtures/descriptor_model.py").write_text(model)
    for i, body in enumerate(descriptors, 1):
        name = re.search(r"^\s*name:\s*(\S+)", body, re.M)
        slug = name.group(1) if name else f"descriptor{i}"
        (out / f"fixtures/{slug}.yml").write_text(body)
        print(f"  fixtures/{slug}.yml")

    print(f"  fixtures/descriptor_model.py")
    print(f"extracted 1 model + {len(descriptors)} descriptors from {post.name}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--post", type=pathlib.Path, default=DEFAULT_POST)
    ap.add_argument("--out", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "tests")
    args = ap.parse_args()
    return extract(args.post, args.out)


if __name__ == "__main__":
    raise SystemExit(main())
