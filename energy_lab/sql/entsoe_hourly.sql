-- Step 2 (transform): parsed ENTSO-E intervals  ->  one value per (zone, hour).
--
-- Input:  `intervals` (zone, ts_utc, resolution_min, value), from ingest/entsoe_xml.py.
--         Resolution is 60 min until the end of 2024 and 15 min from 1 January 2025.
--         The same hour can occasionally be published at both resolutions.
-- Output: zone, ts_utc (start of the UTC hour), value (hourly mean), resolution_min,
--         n_intervals, complete (all intervals of the hour present).
--
-- For each hour we keep only the finest resolution available, so an hour is never
-- averaged from a mix of hourly and quarter-hourly values.

WITH per_resolution AS (
    SELECT
        zone,
        date_trunc('hour', ts_utc) AS ts_utc,
        resolution_min,
        avg(value)                 AS value,
        count(*)                   AS n_intervals
    FROM intervals
    WHERE resolution_min <= 60
    GROUP BY ALL
)
SELECT
    zone,
    ts_utc,
    CAST(value AS DOUBLE)              AS value,
    CAST(resolution_min AS SMALLINT)   AS resolution_min,
    CAST(n_intervals AS SMALLINT)      AS n_intervals,
    n_intervals = 60 // resolution_min AS complete
FROM per_resolution
QUALIFY row_number() OVER (PARTITION BY zone, ts_utc ORDER BY resolution_min) = 1
ORDER BY zone, ts_utc
