"""What an installed copy promises a dependent — catalog-mcp first (platform#26).

The base install is the descriptor model, the policy resolution and the example
data; running a pipeline needs the `run` extra. These tests hold both halves of
that split, and the fallback that lets an installed copy find its ontology with
no `example/` beside it.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from pipeline_runtime import policy

REPO = Path(__file__).resolve().parents[1]
EXECUTION = ("duckdb", "pandas", "numpy", "pyarrow", "great_expectations", "openlineage")


def test_model_and_policy_import_without_the_execution_stack():
    # A fresh interpreter, because this one has already imported half the stack.
    probe = (
        "import sys, pipeline_runtime.descriptor, pipeline_runtime.policy; "
        f"print(sorted(m for m in {EXECUTION!r} if m in sys.modules))"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert out.strip() == "[]"


def test_an_installed_copy_reads_the_ontology_it_carries(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no example/ here, as for any installed copy
    monkeypatch.delenv("CORDATA_ONTOLOGY", raising=False)
    path = policy.ontology_path()
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == json.loads(
        (REPO / "example" / "ontology.json").read_text(encoding="utf-8")
    )


def test_a_clone_still_reads_its_own_example(monkeypatch):
    monkeypatch.chdir(REPO)
    monkeypatch.delenv("CORDATA_ONTOLOGY", raising=False)
    assert policy.ontology_path() == Path("example/ontology.json")


def test_the_cli_names_the_missing_extra(monkeypatch, capsys):
    from pipeline_runtime import __main__ as cli

    # Simulate a base install: duckdb absent, and the modules that import it
    # not yet loaded, so `from .run import run` has to import it afresh.
    monkeypatch.setitem(sys.modules, "duckdb", None)
    for name in ("pipeline_runtime.run", "pipeline_runtime.catalog"):
        monkeypatch.delitem(sys.modules, name, raising=False)

    assert cli.main(["example/domains/fraud/pipelines/transactions_scored.yml"]) == 2
    err = capsys.readouterr().err
    assert "duckdb" in err
    assert "cordata-pipeline-runtime[run]" in err
