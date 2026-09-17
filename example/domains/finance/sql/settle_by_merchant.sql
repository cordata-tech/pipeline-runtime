-- Domain-owned. Settled volume per merchant per booking day — the figure the
-- finance domain reconciles payouts against.
SELECT
    merchant_id,
    CAST(booked_at AS DATE)      AS settlement_date,
    count(*)                     AS tx_count,
    round(sum(amount_eur), 2)    AS amount_eur
FROM frame
WHERE status = 'settled'
GROUP BY merchant_id, CAST(booked_at AS DATE);
