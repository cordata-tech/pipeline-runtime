-- The reader has already collapsed the CDC log to one row per claim_id — that
-- is DMS mechanics and belongs in the platform's `read_dms_landing`, not here.
-- What is left is the domain's own notion of a duplicate: the same claim
-- submitted twice under different ids, which only the policy team can define.
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY policy_id, claimant_iban, amount_eur, CAST(reported_at AS DATE)
            ORDER BY reported_at, claim_id
        ) AS submission_rank
    FROM frame
    WHERE status <> 'void'
)
SELECT
    claim_id,
    policy_id,
    claimant_iban,
    amount_eur,
    reported_at,
    status,
    CAST(reported_at AS DATE) AS ingest_date
FROM ranked
WHERE submission_rank = 1;
