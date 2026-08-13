"""Build the local catalog and the source data both example pipelines read.

Everything this writes is reproducible and gitignored — a clean clone runs this
first and then runs the pipelines. Deterministic by construction (fixed seed),
so the row counts in the README are the row counts you get.

    python -m tools.seed              # the happy path
    python -m tools.seed --drift      # move fraud_raw.transactions to v8
"""

from __future__ import annotations

import argparse
import shutil
import string
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline_runtime import catalog  # noqa: E402
from pipeline_runtime.backends.local import warehouse  # noqa: E402

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


def register(con, database: str, table: str, columns: list[tuple], version: int) -> None:
    con.execute(
        'DELETE FROM _catalog.tables WHERE database = ? AND "table" = ? AND version = ?',
        [database, table, version],
    )
    con.executemany(
        "INSERT INTO _catalog.tables VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (database, table, version, name, typ, nullable, pk, i)
            for i, (name, typ, nullable, pk) in enumerate(columns)
        ],
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scale", type=int, default=1, help="multiplier on row counts")
    ap.add_argument(
        "--drift",
        action="store_true",
        help="publish fraud_raw.transactions v8, which the descriptor's v7 pin will reject",
    )
    ap.add_argument("--clean", action="store_true", help="delete the catalog and warehouse first")
    args = ap.parse_args(argv)

    if args.clean:
        shutil.rmtree(warehouse(), ignore_errors=True)
        catalog.catalog_path().unlink(missing_ok=True)

    rng = np.random.default_rng(SEED)
    n_tx = 50_000 * args.scale
    n_claims = 8_000 * args.scale

    tx = transactions(rng, n_tx)
    cdc = claims_cdc(rng, n_claims)

    with catalog.connect() as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS fraud_raw")
        con.register("_tx", tx)
        con.execute("CREATE OR REPLACE TABLE fraud_raw.transactions AS SELECT * FROM _tx")

        register(con, "fraud_raw", "transactions", TRANSACTIONS_V7, version=7)
        register(con, "policy_raw", "claims", CLAIMS_V3, version=3)

        if args.drift:
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
            register(con, "fraud_raw", "transactions", TRANSACTIONS_V8, version=8)

    landing = warehouse() / "landing" / "policy_raw" / "claims"
    shutil.rmtree(landing, ignore_errors=True)
    landing.mkdir(parents=True, exist_ok=True)
    # Several files, because DMS lands one per batch and a reader that only
    # handles a single file passes its test and fails in production.
    for i, bounds in enumerate(np.array_split(np.arange(len(cdc)), 4)):
        cdc.iloc[bounds].to_parquet(landing / f"batch-{i:04d}.parquet", index=False)

    print(f"catalog   {catalog.catalog_path()}")
    print(f"fraud_raw.transactions   {len(tx):,} rows at v{8 if args.drift else 7}")
    print(f"policy_raw.claims        {len(cdc):,} CDC rows in 4 batches under {landing}")
    if args.drift:
        print("\nfraud_raw.transactions is now at v8; the descriptor pins v7.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
