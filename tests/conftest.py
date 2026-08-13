"""Fail fast, and legibly, on an interpreter older than the published code.

`pyproject.toml` declares `requires-python = ">=3.12"`, but that only binds when
the package is installed through pip. Running `pytest` against a bare older
interpreter skips that check entirely, and the failure it produces is actively
misleading: the code uses `match` (3.10+) and PEP 695 `type` aliases (3.12+), so
an old interpreter reports a *SyntaxError in the article's code* rather than
"wrong Python". That reads as the post being broken when the post is fine.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

MINIMUM = (3, 12)

if sys.version_info < MINIMUM:
    raise RuntimeError(
        f"This suite requires Python {'.'.join(map(str, MINIMUM))}+, "
        f"got {sys.version.split()[0]} at {sys.executable}.\n"
        "The published samples use `match` and PEP 695 `type` aliases; an older "
        "interpreter will report a SyntaxError in the extracted post code, which "
        "looks like the article is wrong when it is not.\n"
        "Re-run with a modern interpreter, e.g. `python3.12 -m pytest`."
    )

import pytest  # noqa: E402

from tests.paths import ONTOLOGY, REPO  # noqa: E402


@pytest.fixture(scope="session")
def seeded(tmp_path_factory) -> Path:
    """One seeded catalog for the whole session, in a temp directory.

    Deliberately not the checked-in `example/catalog.duckdb`: a suite that
    mutates the thing the quickstart depends on will eventually leave a
    contributor with a v8 catalog and no idea why the README stopped working.
    """
    root = tmp_path_factory.mktemp("cordata")
    _seed(root)
    return root


def _seed(root: Path, *, drift: bool = False) -> None:
    env = environment(root)
    cmd = [sys.executable, "-m", "tools.seed", "--clean"] + (["--drift"] if drift else [])
    subprocess.run(cmd, cwd=REPO, env=env, check=True, capture_output=True)


def environment(root: Path) -> dict[str, str]:
    import os

    return {
        **os.environ,
        "CORDATA_CATALOG": str(root / "catalog.duckdb"),
        "CORDATA_WAREHOUSE": str(root / "warehouse"),
        "CORDATA_ONTOLOGY": str(ONTOLOGY),
        "CORDATA_LINEAGE_OUT": str(root / "lineage.ndjson"),
    }


@pytest.fixture
def env(seeded, monkeypatch) -> Path:
    """Point the executor at the seeded temp root for one test."""
    for key, value in environment(seeded).items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)
    return seeded


@pytest.fixture
def scenario(tmp_path):
    """A copy of a published domain with one thing changed.

    The failure modes need descriptors that fail, and the published pair does
    not — which is correct, because the quickstart should show two pipelines
    working rather than a menu of ways to break. So each scenario is the real
    domain, copied, with exactly the field under test patched. Copying rather
    than editing in place keeps `example/` honest.
    """
    import shutil

    import yaml

    def build(descriptor: Path, *, patch: dict | None = None, expectations: list | None = None):
        root = tmp_path / descriptor.parents[1].name
        shutil.copytree(descriptor.parents[1], root, dirs_exist_ok=True)

        raw = yaml.safe_load(descriptor.read_text())
        # Its own output location, so "the declared target is empty" means this
        # scenario wrote nothing rather than that nothing has run yet. The
        # warehouse is shared across the session; the assertion must not be.
        raw["target"]["location"] = f"s3://cordata-scenarios/{tmp_path.name}/"

        for dotted, value in (patch or {}).items():
            node = raw
            *parents, leaf = dotted.split(".")
            for part in parents:
                node = node[part]
            node[leaf] = value

        if expectations is not None:
            suite = root / raw["expectations"]["suite"]
            existing = yaml.safe_load(suite.read_text())
            suite.write_text(yaml.safe_dump({**existing, "expectations": expectations}))

        out = root / "pipelines" / descriptor.name
        out.write_text(yaml.safe_dump(raw, sort_keys=False))
        return out

    return build


# A suite that cannot pass, whatever the data: the row count is asserted to be
# in a band no run will ever produce. Failing for a reason unrelated to the
# fixture's contents is the point — these tests are about what the executor does
# with a failed result, not about which rule produced it.
IMPOSSIBLE = [{"type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 10**9}}]


@pytest.fixture
def events(env, monkeypatch, tmp_path):
    """A fresh lineage log per test, plus a reader for it."""
    log = tmp_path / "lineage.ndjson"
    monkeypatch.setenv("CORDATA_LINEAGE_OUT", str(log))

    def read() -> list[dict]:
        if not log.is_file():
            return []
        return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]

    return read
