-- $model_version comes from the step's `params` block in the descriptor, bound
-- as a query parameter rather than pasted into the text. It lands on the
-- lineage event too, inside the provenance facet, which is what lets someone
-- ask six months later which model version produced a given score.
SELECT
    tx_id,
    account_id,
    iban,
    amount_eur,
    currency,
    booked_at,
    merchant_id,
    $model_version                                          AS model_version,
    CAST(booked_at AS DATE)                                 AS scored_date,
    -- Stand-in for the real model: deterministic in tx_id so a rerun of the
    -- same batch scores identically, and shaped so the distributional
    -- expectation in the suite has something to be right or wrong about.
    round(
        least(
            0.999,
            0.35 * (abs(hash(tx_id)) % 1000) / 1000.0
          + 0.55 * least(1.0, amount_eur / 5000.0)
        ),
        4
    )                                                       AS fraud_score
FROM frame;
