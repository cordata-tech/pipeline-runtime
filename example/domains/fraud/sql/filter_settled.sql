-- Domain-owned. The only executable thing in the descriptor is SQL, referenced
-- by path — this is the part that genuinely differs between pipelines and
-- genuinely belongs to the domain.
--
-- The frame arriving from the previous stage is always registered as `frame`.
SELECT *
FROM frame
WHERE status = 'settled';
