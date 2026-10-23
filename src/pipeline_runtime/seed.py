"""Build the local catalog and the source data the example pipelines read.

Everything this writes is reproducible — deterministic by construction (fixed
seed), so the row counts in the README are the row counts you get. In a clone it
is gitignored.

In the package rather than in `tools/` because `pipeline-runtime demo` seeds from
an installed copy, where there is no `tools/` (#8). `tools/seed.py` is a wrapper
over this, kept because CI, the tests and the README's `--drift` scenario call
it. It needs the `run` extra, so nothing imports it at package import time.

    python -m tools.seed              # the happy path
    python -m tools.seed --drift      # move fraud_raw.transactions to v8
"""

from __future__ import annotations

import argparse
import shutil
import string
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from . import catalog, policy
from .backends.local import warehouse
from .descriptor import TableRef

SEED = 20260812
EPOCH = datetime(2026, 7, 1)

TRANSACTIONS_V7 = [
    ("tx_id", "VARCHAR", False, True),
    ("account_id", "VARCHAR", False, False),
    ("iban", "VARCHAR", False, False),
    ("amount_eur", "DOUBLE", False, False),
    ("currency", "VARCHAR", False, False),
    ("status", "VARCHAR", False, False),
    ("booked_at", "TIMESTAMP", False, False),
    ("merchant_id", "VARCHAR", False, False),
]

# What the upstream team ships when they add a column: same table, next version,
# one more field. The descriptor still pins v7, which is the whole point.
TRANSACTIONS_V8 = [*TRANSACTIONS_V7, ("merchant_category_code", "VARCHAR", True, False)]

CLAIMS_V3 = [
    ("claim_id", "VARCHAR", False, True),
    ("policy_id", "VARCHAR", False, False),
    ("claimant_iban", "VARCHAR", False, False),
    ("amount_eur", "DOUBLE", False, False),
    ("reported_at", "TIMESTAMP", False, False),
    ("status", "VARCHAR", False, False),
]

# Who publishes each source table, and from which release. In a deployment the
# application's own release pipeline sets these when it updates the Glue table;
# here the seed plays both applications.
TRANSACTIONS = TableRef(database="fraud_raw", table="transactions")
TRANSACTIONS_PRODUCER = "card-ledger"
TRANSACTIONS_RELEASES = {7: "v4.11.0", 8: "v4.12.0"}

CLAIMS = TableRef(database="policy_raw", table="claims")
CLAIMS_PRODUCER = "claims-portal"
CLAIMS_RELEASES = {3: "v2.3.0"}

# Which domain owns each source table, as the governance plane records it: the
# `domain` LF-tag, which the reference Terraform assigns to each domain's
# databases and Lake Formation passes down to their tables. Only `domain`,
# because classifying an application's table is its owner's decision, not the
# seed's. A run names its input by this, so a table two domains read stays one
# dataset in the lineage (#7).
OWNERS = [(TRANSACTIONS, "fraud"), (CLAIMS, "policy")]


def ibans(rng: np.random.Generator, n: int) -> np.ndarray:
    """`DE` + two check digits + 18 alphanumerics — the shape the suite's regex asserts."""
    alphabet = np.array(list(string.ascii_uppercase + string.digits))
    body = rng.choice(alphabet, size=(n, 18))
    check = rng.integers(10, 99, size=n)
    return np.array(["DE" + str(c) + "".join(b) for c, b in zip(check, body, strict=True)])


def transactions(rng: np.random.Generator, n: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tx_id": [f"tx-{i:09d}" for i in range(n)],
            "account_id": [f"acc-{i:06d}" for i in rng.integers(0, n // 12 + 1, size=n)],
            "iban": ibans(rng, n),
            # Lognormal, so most payments are small and a thin tail is not —
            # a uniform amount would make the distributional expectation
            # meaningless by making every batch look alike.
            "amount_eur": np.round(rng.lognormal(mean=4.1, sigma=1.05, size=n), 2),
            "currency": "EUR",
            "status": rng.choice(["settled", "pending", "reversed"], size=n, p=[0.72, 0.24, 0.04]),
            "booked_at": [
                EPOCH + timedelta(seconds=int(s)) for s in rng.integers(0, 14 * 86400, size=n)
            ],
            "merchant_id": [f"mrc-{i:05d}" for i in rng.integers(0, 4000, size=n)],
        }
    )


def claims_cdc(rng: np.random.Generator, n_claims: int) -> pd.DataFrame:
    """A DMS landing batch: inserts, updates and deletes, out of order.

    Deliberately messy — the reader's `collapse_cdc` has to earn its place, and
    a fixture where every key appears once proves nothing.
    """
    base = pd.DataFrame(
        {
            "claim_id": [f"clm-{i:07d}" for i in range(n_claims)],
            "policy_id": [
                f"pol-{i:06d}" for i in rng.integers(0, n_claims // 3 + 1, size=n_claims)
            ],
            "claimant_iban": ibans(rng, n_claims),
            "amount_eur": np.round(rng.lognormal(mean=6.2, sigma=0.9, size=n_claims), 2),
            "reported_at": [
                EPOCH + timedelta(seconds=int(s)) for s in rng.integers(0, 7 * 86400, size=n_claims)
            ],
            "status": rng.choice(["open", "settled", "void"], size=n_claims, p=[0.55, 0.4, 0.05]),
        }
    )
    base["Op"] = "I"

    # A quarter of the claims are updated at least once. The update carries the
    # later status, and only `_dms_seq` distinguishes it from the insert.
    updated = base.sample(frac=0.25, random_state=SEED).copy()
    updated["Op"] = "U"
    updated["status"] = rng.choice(["settled", "void"], size=len(updated), p=[0.85, 0.15])
    updated["amount_eur"] = np.round(updated["amount_eur"] * rng.uniform(0.9, 1.1, len(updated)), 2)

    # And a few are deleted outright. A collapse that ignored Op would resurrect
    # them, which is the classic way a "deduplicated" table over-reports.
    deleted = base.sample(frac=0.03, random_state=SEED + 1).copy()
    deleted["Op"] = "D"

    frame = pd.concat([base, updated, deleted], ignore_index=True)
    frame["_dms_seq"] = np.arange(len(frame), dtype="int64")
    return frame.sample(frac=1.0, random_state=SEED + 2).reset_index(drop=True)


def forget(con, ref: TableRef) -> None:
    """Drop the seed's own history for a table, so a re-run starts it again.

    `catalog.publish` never rewrites a version, which is right for a catalog
    and wrong for a fixture that `--drift` re-runs on top of.
    """
    for table in ("tables", "versions"):
        con.execute(
            f'DELETE FROM _catalog.{table} WHERE database = ? AND "table" = ?',
            [ref.database, ref.table],
        )


def own(con, ref: TableRef, domain: str) -> None:
    """Record which domain owns a table, checked against the ontology like any tag."""
    policy.resolve({"domain": domain})
    con.execute(
        "DELETE FROM _catalog.lf_tags WHERE database = ? AND \"table\" = ? AND tag_key = 'domain'",
        [ref.database, ref.table],
    )
    con.execute(
        "INSERT INTO _catalog.lf_tags VALUES (?, ?, 'domain', ?)",
        [ref.database, ref.table, domain],
    )


def publish(con, ref: TableRef, columns: list[tuple], version: int, producer: str, release: str):
    catalog.publish(
        con,
        ref,
        version,
        [catalog.Column(n, t, nullable, pk) for n, t, nullable, pk in columns],
        producer=producer,
        release=release,
    )


def seed(scale: int = 1, drift: bool = False, clean: bool = False) -> dict[str, object]:
    """Write the catalog and the source data, and report what was written.

    Separate from `main` so that `pipeline-runtime demo` can seed without going
    through a command line, and so a caller gets the counts back rather than
    having to scrape them off stdout.
    """
    if clean:
        shutil.rmtree(warehouse(), ignore_errors=True)
        catalog.catalog_path().unlink(missing_ok=True)

    rng = np.random.default_rng(SEED)
    n_tx = 50_000 * scale
    n_claims = 8_000 * scale

    tx = transactions(rng, n_tx)
    cdc = claims_cdc(rng, n_claims)

    with catalog.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS fraud_raw")
        con.register("_tx", tx)
        con.execute("CREATE OR REPLACE TABLE fraud_raw.transactions AS SELECT * FROM _tx")

        forget(con, TRANSACTIONS)
        forget(con, CLAIMS)
        publish(
            con, TRANSACTIONS, TRANSACTIONS_V7, 7, TRANSACTIONS_PRODUCER, TRANSACTIONS_RELEASES[7]
        )
        publish(con, CLAIMS, CLAIMS_V3, 3, CLAIMS_PRODUCER, CLAIMS_RELEASES[3])
        for ref, domain in OWNERS:
            own(con, ref, domain)

        if drift:
            # The column exists in the catalog *and* in the data, because that
            # is what actually happens: the upstream team ships the change and
            # the consumer finds out on its next run.
            con.execute(
                "ALTER TABLE fraud_raw.transactions ADD COLUMN merchant_category_code VARCHAR"
            )
            con.execute(
                "UPDATE fraud_raw.transactions SET merchant_category_code = "
                "printf('%04d', abs(hash(merchant_id)) % 10000)"
            )
            publish(
                con,
                TRANSACTIONS,
                TRANSACTIONS_V8,
                8,
                TRANSACTIONS_PRODUCER,
                TRANSACTIONS_RELEASES[8],
            )

    landing = warehouse() / "landing" / "policy_raw" / "claims"
    shutil.rmtree(landing, ignore_errors=True)
    landing.mkdir(parents=True, exist_ok=True)
    # Several files, because DMS lands one per batch and a reader that only
    # handles a single file passes its test and fails in production.
    for i, bounds in enumerate(np.array_split(np.arange(len(cdc)), 4)):
        cdc.iloc[bounds].to_parquet(landing / f"batch-{i:04d}.parquet", index=False)

    return {
        "catalog": catalog.catalog_path(),
        "transactions": len(tx),
        "transactions_version": 8 if drift else 7,
        "claims": len(cdc),
        "landing": landing,
        "drift": drift,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m tools.seed", description=__doc__)
    ap.add_argument("--scale", type=int, default=1, help="multiplier on row counts")
    ap.add_argument(
        "--drift",
        action="store_true",
        help="publish fraud_raw.transactions v8, which the descriptor's v7 pin will reject",
    )
    ap.add_argument("--clean", action="store_true", help="delete the catalog and warehouse first")
    args = ap.parse_args(argv)

    written = seed(scale=args.scale, drift=args.drift, clean=args.clean)

    print(f"catalog   {written['catalog']}")
    print(
        f"fraud_raw.transactions   {written['transactions']:,} rows "
        f"at v{written['transactions_version']}"
    )
    print(
        f"policy_raw.claims        {written['claims']:,} CDC rows in 4 batches "
        f"under {written['landing']}"
    )
    if args.drift:
        print(
            f"\nfraud_raw.transactions is now at v8, published by {TRANSACTIONS_PRODUCER} "
            f"release {TRANSACTIONS_RELEASES[8]}; the descriptors pin v7."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
