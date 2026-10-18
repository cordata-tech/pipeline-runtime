"""What an installed copy promises a dependent — catalog-mcp first (platform#26).

The base install is the descriptor model, the policy resolution and the example
data; running a pipeline needs the `run` extra. These tests hold both halves of
that split and the order in which the ontology is looked for.

Whether the wheel really carries `example/` is checked where a wheel exists:
the release workflow installs it into a clean environment and compares the
packaged domains with the repository's. An editable install, which is what
these tests run against, does not map `pipeline_runtime.example` at all.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path

import yaml

from pipeline_runtime import policy

REPO = Path(__file__).resolve().parents[1]
EXECUTION = ("duckdb", "pandas", "numpy", "pyarrow", "great_expectations", "openlineage")


def _fresh(code: str) -> subprocess.CompletedProcess[str]:
    # A fresh interpreter, because this one has already imported the stack.
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=REPO)


# ------------------------------------------------- the Python version, in three places


def project() -> dict:
    return tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["project"]


def classifier_versions() -> list[tuple[int, int]]:
    found = [
        c.removeprefix("Programming Language :: Python :: ")
        for c in project()["classifiers"]
        if c.startswith("Programming Language :: Python :: ")
    ]
    return sorted(tuple(int(part) for part in v.split(".")) for v in found if "." in v)


def ci_versions() -> list[tuple[int, int]]:
    workflow = yaml.safe_load((REPO / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    declared = workflow["jobs"]["test"]["strategy"]["matrix"]["python-version"]
    return sorted(tuple(int(part) for part in str(v).split(".")) for v in declared)


def test_the_python_classifiers_are_the_versions_ci_runs():
    """The README's python badge reads these classifiers off PyPI.

    So they are a claim about which interpreters work, and the only evidence
    for that claim is the CI matrix. A classifier CI does not exercise is a
    promise nobody checks; a matrix entry with no classifier is work whose
    result the badge hides.
    """
    assert classifier_versions() == ci_versions()


def test_the_lowest_classifier_is_the_floor_requires_python_promises():
    """`requires-python` is what pip enforces, the classifiers are what a reader
    sees, and they have to start at the same version."""
    spec = project()["requires-python"]
    floor = re.fullmatch(r">=(\d+)\.(\d+)", spec)
    assert floor, (
        f"this test understands `>=X.Y` and requires-python is now {spec!r}; "
        "extend the test rather than deleting it"
    )
    assert classifier_versions()[0] == (int(floor[1]), int(floor[2]))


# ------------------------------------------------- what an install carries


def test_model_and_policy_import_without_the_execution_stack():
    out = _fresh(
        "import sys, pipeline_runtime.descriptor, pipeline_runtime.policy; "
        f"print(sorted(m for m in {EXECUTION!r} if m in sys.modules))"
    )
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "[]"


def test_cordata_ontology_wins_over_everything(tmp_path, monkeypatch):
    chosen = tmp_path / "mine.json"
    monkeypatch.setenv("CORDATA_ONTOLOGY", str(chosen))
    monkeypatch.chdir(REPO)
    assert policy.ontology_path() == chosen


def test_a_clone_reads_its_own_example(monkeypatch):
    monkeypatch.chdir(REPO)
    monkeypatch.delenv("CORDATA_ONTOLOGY", raising=False)
    assert policy.ontology_path() == Path("example/ontology.json")


def test_with_no_example_beside_it_the_packaged_ontology_is_used(tmp_path, monkeypatch):
    packaged = tmp_path / "pkg"
    packaged.mkdir()
    (packaged / "ontology.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(policy, "files", lambda _package: packaged)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CORDATA_ONTOLOGY", raising=False)
    assert policy.ontology_path() == packaged / "ontology.json"


def test_without_a_packaged_copy_the_missing_file_is_reported(tmp_path, monkeypatch):
    def unmapped(_package):
        raise ModuleNotFoundError("pipeline_runtime.example")

    monkeypatch.setattr(policy, "files", unmapped)
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("CORDATA_ONTOLOGY", raising=False)
    assert policy.ontology_path() == Path("example/ontology.json")


def test_the_cli_names_the_missing_extra():
    # duckdb blocked before anything is imported, as on a base install.
    out = _fresh(
        "import sys; sys.modules['duckdb'] = None; "
        "from pipeline_runtime.__main__ import main; "
        "raise SystemExit(main(['example/domains/fraud/pipelines/transactions_scored.yml']))"
    )
    assert out.returncode == 2, out.stderr
    # Which module it names depends on import order — pandas on a real base
    # install, duckdb here — so the test asks only that it names one of them.
    assert any(f"needs {m}" in out.stderr for m in EXECUTION), out.stderr
    assert "cordata-pipeline-runtime[run]" in out.stderr
