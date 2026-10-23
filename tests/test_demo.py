"""`pipeline-runtime demo` — the quickstart without a clone (#8).

The claim is that an installed copy can seed the example and run it, so these
tests exercise the command rather than the pieces: what lands in the directory,
what it refuses, and that the facts it produces are the ones the seeded clone
produces. Whether a *wheel* can do it is checked where a wheel exists — the
release workflow installs one into a clean environment and runs the demo there,
because an editable install maps no `pipeline_runtime.example` and so cannot
prove the packaged path.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline_runtime import catalog
from pipeline_runtime.demo import OURS, demo, descriptors, unexpected

from .paths import REPO


@pytest.fixture
def ran(tmp_path, monkeypatch) -> Path:
    """One demo run into an empty directory, with the environment it sets kept."""
    for key in ("CORDATA_CATALOG", "CORDATA_WAREHOUSE", "CORDATA_LINEAGE_OUT"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(REPO)  # a clone, so `example/` is the descriptor source

    target = tmp_path / "demo"
    assert demo(target, out=io.StringIO()) == 0
    return target


def events(target: Path) -> list[dict]:
    log = target / "out" / "lineage.ndjson"
    return [json.loads(line) for line in log.read_text().splitlines() if line.strip()]


def test_it_writes_a_catalog_a_warehouse_and_the_events(ran):
    assert (ran / "catalog.duckdb").is_file()
    assert list((ran / "warehouse").iterdir()), "the warehouse is empty"
    assert (ran / "out" / "lineage.ndjson").is_file()


def test_it_writes_nothing_outside_the_directory_it_was_given(ran):
    assert {p.name for p in ran.iterdir()} <= OURS


def test_all_three_pipelines_ran(ran):
    complete = [e for e in events(ran) if e["eventType"] == "COMPLETE"]
    assert sorted(e["job"]["name"] for e in complete) == [
        "claims-ingest",
        "merchant-settlement-daily",
        "transactions-scored-daily",
    ]


def test_the_two_fraud_readers_name_one_dataset(ran):
    """#7's domain tag, through the demo: fraud and finance read the same table,
    and the events have to agree on its namespace or the graph forks."""
    inputs = {
        (d["namespace"], d["name"])
        for e in events(ran)
        if e["eventType"] == "COMPLETE"
        for d in e["inputs"]
    }
    assert ("cordata://fraud", "fraud_raw.transactions") in inputs
    assert len([i for i in inputs if i[1] == "fraud_raw.transactions"]) == 1


def test_the_catalog_matches_what_the_seed_writes(ran, monkeypatch):
    """Same versions, same producer, same domain tags as `tools.seed --clean`.

    The seeded-clone path is asserted in `test_catalog.py` and `test_lineage.py`;
    what matters here is that the demo produced the same facts rather than a
    second, nearly-identical fixture.
    """
    monkeypatch.setenv("CORDATA_CATALOG", str(ran / "catalog.duckdb"))
    with catalog.connect(read_only=True) as con:
        versions = con.execute(
            'SELECT database, "table", version, producer, release '
            "FROM _catalog.versions ORDER BY database, version"
        ).fetchall()
        domains = con.execute(
            "SELECT database, tag_value FROM _catalog.lf_tags "
            "WHERE tag_key = 'domain' ORDER BY database"
        ).fetchall()
        rows = con.execute("SELECT count(*) FROM fraud_raw.transactions").fetchone()[0]

    assert versions == [
        ("fraud_raw", "transactions", 7, "card-ledger", "v4.11.0"),
        ("policy_raw", "claims", 3, "claims-portal", "v2.3.0"),
    ]
    assert domains == [("fraud_raw", "fraud"), ("policy_raw", "policy")]
    assert rows == 50_000


def test_a_directory_holding_someone_elses_files_is_refused(tmp_path, capsys):
    (tmp_path / "thesis.pdf").touch()
    (tmp_path / "notes.txt").touch()

    assert demo(tmp_path, out=io.StringIO()) == 1

    message = capsys.readouterr().err
    assert str(tmp_path) in message
    assert "thesis.pdf" in message and "notes.txt" in message
    assert not (tmp_path / "catalog.duckdb").exists(), "it wrote despite refusing"


def test_a_directory_holding_only_its_own_output_is_reused(ran):
    """A second demo into the same directory is the obvious thing to try, and it
    reports its own three runs rather than six.

    The lineage transport appends — deliberately, since part 2 § 4 has several
    events accumulating against one run id — so without clearing the log a
    re-run would leave a reader counting double.
    """
    assert unexpected(ran) == []
    assert demo(ran, out=io.StringIO()) == 0
    assert len([e for e in events(ran) if e["eventType"] == "COMPLETE"]) == 3


def test_every_domain_has_a_descriptor_to_run():
    found = descriptors()
    assert [p.parents[1].name for p in found] == ["fraud", "policy", "finance"]
    assert all(p.is_file() for p in found)


def test_the_cli_routes_demo_without_breaking_the_descriptor_form(tmp_path):
    """`pipeline-runtime <descriptor>` is what part 1 § 3 publishes, so `demo`
    had to arrive beside it rather than turn it into a subcommand."""
    out = subprocess.run(
        [sys.executable, "-m", "pipeline_runtime", "demo", "--help"],
        capture_output=True,
        text=True,
        cwd=REPO,
    )
    assert out.returncode == 0, out.stderr
    assert "pipeline-runtime demo" in out.stdout

    descriptor = subprocess.run(
        [sys.executable, "-m", "pipeline_runtime", "--help"],
        capture_output=True,
        text=True,
        cwd=REPO,
    )
    assert "descriptor" in descriptor.stdout
