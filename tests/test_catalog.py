"""Who published a version, and what changed between two of them.

`SchemaDrift` could always say which columns changed. These tests hold the half
added for #1: it also says which release changed them, and the catalog it reads
cannot quietly attribute a version to a release that did not publish it.
"""

from __future__ import annotations

import pytest

from pipeline_runtime import catalog
from pipeline_runtime.catalog import Column
from pipeline_runtime.descriptor import Source, TableRef
from pipeline_runtime.errors import SchemaDrift

REF = TableRef(database="app_raw", table="orders")

V1 = [
    Column("order_id", "VARCHAR", nullable=False, primary_key=True),
    Column("amount", "DOUBLE", nullable=False),
    Column("note", "VARCHAR", nullable=True),
]


@pytest.fixture
def con(tmp_path, monkeypatch):
    monkeypatch.setenv("CORDATA_CATALOG", str(tmp_path / "catalog.duckdb"))
    with catalog.connect() as connection:
        yield connection


def pin(version: int) -> Source:
    return Source(kind="glue_table", database=REF.database, table=REF.table, schema_version=version)


def drift_message(con, version: int) -> str:
    con.close()  # resolve opens its own read-only connection
    with pytest.raises(SchemaDrift) as exc:
        catalog.resolve(pin(version))
    return str(exc.value)


# ---------------------------------------------------------------- publishing


@pytest.mark.parametrize("producer,release", [("", "v1.0.0"), ("orders-api", " ")])
def test_a_version_cannot_be_published_without_both_producer_and_release(con, producer, release):
    with pytest.raises(ValueError, match="both required"):
        catalog.publish(con, REF, 1, V1, producer=producer, release=release)


def test_a_published_version_is_never_rewritten(con):
    catalog.publish(con, REF, 1, V1, producer="orders-api", release="v1.0.0")
    with pytest.raises(ValueError, match="not rewritten"):
        catalog.publish(con, REF, 1, V1[:2], producer="orders-api", release="v1.0.1")


def test_nothing_is_carried_forward_from_the_version_before(con):
    """The local model of a publishing job that writes the full set every time.

    Whether Glue's `UpdateTable` keeps parameters a job omits is untested, so
    each version's attribution here is exactly what its own publish wrote.
    """
    catalog.publish(con, REF, 1, V1, producer="orders-api", release="v1.0.0")
    catalog.publish(con, REF, 2, V1, producer="checkout", release="2026.09.1")

    found = catalog.publications(con, REF, after=0, upto=2)
    assert [(p.version, p.producer, p.release) for p in found] == [
        (1, "orders-api", "v1.0.0"),
        (2, "checkout", "2026.09.1"),
    ]


def test_a_failed_publish_leaves_no_half_written_version(con):
    catalog.publish(con, REF, 1, V1, producer="orders-api", release="v1.0.0")
    duplicate_column = [*V1, V1[0]]
    with pytest.raises(Exception, match="(?i)constraint"):
        catalog.publish(con, REF, 2, duplicate_column, producer="orders-api", release="v1.1.0")

    assert catalog.current_version(con, REF) == 1
    assert catalog.publications(con, REF, after=1, upto=2) == []


# ---------------------------------------------------------------- the drift message


def test_drift_names_the_release_that_published_the_version(con):
    catalog.publish(con, REF, 1, V1, producer="orders-api", release="v1.0.0")
    catalog.publish(con, REF, 2, V1[:2], producer="orders-api", release="v1.1.0")

    message = drift_message(con, 1)
    assert message == (
        "app_raw.orders is at v2, descriptor pins v1. "
        "v2 was published by orders-api release v1.1.0. "
        "Diff: - note (VARCHAR, nullable). "
        "Bump the pin to v2 to accept."
    )


def test_drift_names_every_release_since_the_pin(con):
    """The diff is cumulative, so any version after the pin may hold the change."""
    catalog.publish(con, REF, 1, V1, producer="orders-api", release="v1.0.0")
    catalog.publish(con, REF, 2, V1[:2], producer="orders-api", release="v1.1.0")
    catalog.publish(con, REF, 3, V1[:1], producer="checkout", release="2026.09.1")

    message = drift_message(con, 1)
    assert "Published since v1: v2 by orders-api release v1.1.0, " in message
    assert "v3 by checkout release 2026.09.1. " in message


def test_drift_says_so_when_a_version_has_no_producer(con):
    """What a Glue version written by a crawler, or by a job that set no
    parameters, would look like. Naming the previous release instead would
    blame the wrong team."""
    catalog.publish(con, REF, 1, V1, producer="orders-api", release="v1.0.0")
    con.executemany(
        "INSERT INTO _catalog.tables VALUES (?, ?, 2, ?, ?, ?, ?, ?)",
        [
            (REF.database, REF.table, c.name, c.type, c.nullable, c.primary_key, i)
            for i, c in enumerate(V1)
        ],
    )

    assert "v2 was published by an unrecorded producer. " in drift_message(con, 1)


# ---------------------------------------------------------------- column changes


def changes(newer: list[Column]) -> list[tuple[str, bool]]:
    return [(str(c), c.breaking) for c in catalog.changes(V1, newer)]


def test_an_added_column_does_not_break_a_reader_of_the_older_version():
    assert changes([*V1, Column("currency", "VARCHAR", nullable=True)]) == [
        ("+ currency (VARCHAR, nullable)", False)
    ]


def test_a_removed_or_retyped_column_breaks_it():
    assert changes([V1[0], Column("amount", "BIGINT", nullable=False)]) == [
        ("- note (VARCHAR, nullable)", True),
        ("~ amount: DOUBLE -> BIGINT", True),
    ]


def test_loosening_to_nullable_breaks_it_and_tightening_does_not():
    loosened = [V1[0], Column("amount", "DOUBLE", nullable=True), V1[2]]
    tightened = [*V1[:2], Column("note", "VARCHAR", nullable=False)]
    assert changes(loosened) == [("~ amount: not null -> nullable", True)]
    assert changes(tightened) == [("~ note: nullable -> not null", False)]
