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

import subprocess
import sys
from pathlib import Path

from pipeline_runtime import policy

REPO = Path(__file__).resolve().parents[1]
EXECUTION = ("duckdb", "pandas", "numpy", "pyarrow", "great_expectations", "openlineage")


def _fresh(code: str) -> subprocess.CompletedProcess[str]:
    # A fresh interpreter, because this one has already imported the stack.
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, cwd=REPO)


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
