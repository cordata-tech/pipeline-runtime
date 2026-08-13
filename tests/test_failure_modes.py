"""The three `on_failure` policies, and the rule that outranks all of them.

Part 1 § 3: *every run emits exactly one terminal event.* Success, quarantine,
or failure — the governance layer hears about all three. That is the property
these tests exist to hold, because it is the one that decays quietly: it
survives a code review and dies the first time somebody adds a fourth exit.
"""

from __future__ import annotations

import subprocess
import sys
import uuid

import pytest

from pipeline_runtime.backends.local import local_path
from pipeline_runtime.descriptor import quarantined
from pipeline_runtime.errors import PublishBlocked, SchemaDrift
from pipeline_runtime.run import load, run

from .conftest import IMPOSSIBLE, _seed, environment
from .paths import CLAIMS, FRAUD, REPO


def terminal(events: list[dict]) -> list[dict]:
    return [e for e in events if e["eventType"] in ("COMPLETE", "FAIL", "ABORT")]


def test_block_publish_writes_nothing_and_still_emits(env, events, scenario):
    descriptor = scenario(FRAUD, expectations=IMPOSSIBLE)
    pipeline = load(descriptor)

    with pytest.raises(PublishBlocked):
        run(descriptor, str(uuid.uuid4()))

    assert not list(local_path(pipeline.target.location).rglob("*.parquet")), (
        "block_publish published anyway — no consumer should ever see this batch"
    )
    assert [e["eventType"] for e in terminal(events())] == ["FAIL"]


def test_block_publish_carries_the_verdict_it_died_on(env, events, scenario):
    """`PublishBlocked` is the one failure that got as far as running the suite.

    So its event has both an error *and* assertions. Every other failure has an
    error and no verdict, which is why `emit` accepts `Validation | None`.
    """
    descriptor = scenario(FRAUD, expectations=IMPOSSIBLE)
    with pytest.raises(PublishBlocked):
        run(descriptor, str(uuid.uuid4()))

    kinds = [e["eventType"] for e in events()]
    assert kinds == ["FAIL", "OTHER"], kinds

    assertions = events()[1]["inputs"][0]["inputFacets"]["dataQualityAssertions"]["assertions"]
    assert any(a["success"] is False for a in assertions)


def test_quarantine_writes_beside_the_target_never_to_it(env, events, scenario):
    descriptor = scenario(CLAIMS, expectations=IMPOSSIBLE)
    pipeline = load(descriptor)

    run(descriptor, str(uuid.uuid4()))  # quarantine does not raise

    declared = local_path(pipeline.target.location)
    side = local_path(quarantined(pipeline.target).location)
    assert list(side.rglob("*.parquet")), "quarantine wrote nothing"
    assert not list(declared.rglob("*.parquet")), "quarantine wrote to the declared target"

    event = terminal(events())[0]
    assert event["eventType"] == "FAIL", "a quarantined run did not publish"
    assert event["outputs"][0]["name"].endswith("_quarantined")


def test_warn_publishes_and_says_so(env, events, scenario, caplog):
    descriptor = scenario(FRAUD, patch={"expectations.on_failure": "warn"}, expectations=IMPOSSIBLE)
    pipeline = load(descriptor)

    run(descriptor, str(uuid.uuid4()))

    assert list(local_path(pipeline.target.location).rglob("*.parquet")), "warn blocked the write"
    assert terminal(events())[0]["eventType"] == "COMPLETE"
    assert "0 passed, 1 failed" in caplog.text


def test_schema_drift_fails_before_reading_anything(seeded, monkeypatch, tmp_path):
    """The pin is the point. A source that has moved past it stops the run.

    Seeded into its own root at v8 rather than reusing the session fixture,
    because a test that leaves the shared catalog at v8 makes every test after
    it fail for the wrong reason.
    """
    root = tmp_path / "drifted"
    root.mkdir()
    _seed(root, drift=True)
    for key, value in environment(root).items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)

    with pytest.raises(SchemaDrift) as exc:
        run(FRAUD, str(uuid.uuid4()))

    message = str(exc.value)
    assert "is at v8" in message and "pins v7" in message
    assert "merchant_category_code" in message, "the diff has to name the column"
    assert "Bump the pin" in message, "and the remedy"


def test_a_failed_run_still_produces_an_event_with_the_reason(seeded, monkeypatch, tmp_path):
    """The alternative is a silent gap in the lineage graph where a run should
    be — which is exactly the shape a supervisor cannot distinguish from
    "nobody scheduled it"."""
    root = tmp_path / "drifted"
    root.mkdir()
    _seed(root, drift=True)
    log = tmp_path / "lineage.ndjson"
    for key, value in {**environment(root), "CORDATA_LINEAGE_OUT": str(log)}.items():
        if key.startswith("CORDATA_"):
            monkeypatch.setenv(key, value)

    with pytest.raises(SchemaDrift):
        run(FRAUD, str(uuid.uuid4()))

    import json

    events = [json.loads(line) for line in log.read_text().splitlines() if line.strip()]
    assert len(events) == 1, "a failed run emits one event, not none and not two"
    event = events[0]
    assert event["eventType"] == "FAIL"
    assert event["inputs"] == [] and event["outputs"] == [], (
        "a run that died on drift touched no datasets, and must not claim it did"
    )
    assert "merchant_category_code" in event["run"]["facets"]["errorMessage"]["message"]


def test_the_cli_exits_non_zero_when_the_run_fails(seeded, tmp_path):
    root = tmp_path / "drifted"
    root.mkdir()
    _seed(root, drift=True)

    result = subprocess.run(
        [sys.executable, "-m", "pipeline_runtime", str(FRAUD)],
        cwd=REPO,
        env=environment(root),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1, "an orchestrator reads the exit code, not the log"
    assert "SchemaDrift" in result.stderr
