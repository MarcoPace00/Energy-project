-- Step 2 (transform): raw Open-Meteo JSON  ->  one tidy hourly table.
--
-- Input:  every file matching $raw_glob, e.g. raw/open_meteo/zone=*/month=*/data.json
--         hive_partitioning = true turns the folder names "zone=NORD" and "month=2026-10"
--         into columns, so the zone code never has to be stored inside the file.
-- Output: one row per (zone, hour), typed columns, UTC timestamps.
--
-- Each JSON file holds parallel lists: hourly.time[i] belongs with hourly.temperature_2m[i].
-- Several unnest() calls in the same SELECT walk those lists side by side, turning
-- one row per file into one row per hour.

WITH unpacked AS (
    SELECT
        zone,
        unnest(hourly.time)                AS ts_text,
        unnest(hourly.temperature_2m)      AS temperature_c,
        unnest(hourly.wind_speed_100m)     AS wind_speed_100m_kmh,
        unnest(hourly.shortwave_radiation) AS solar_radiation_wm2,
        unnest(hourly.cloud_cover)         AS cloud_cover_pct
    FROM read_json($raw_glob, hive_partitioning = true, union_by_name = true)
)
SELECT
    zone,
    CAST(ts_text AS TIMESTAMP)              AS ts_utc,
    CAST(temperature_c AS FLOAT)            AS temperature_c,
    CAST(wind_speed_100m_kmh AS FLOAT)      AS wind_speed_100m_kmh,
    CAST(solar_radiation_wm2 AS FLOAT)      AS solar_radiation_wm2,
    CAST(cloud_cover_pct AS FLOAT)          AS cloud_cover_pct
FROM unpacked
-- the archive returns empty values for the last few days until they are published
WHERE temperature_c IS NOT NULL
ORDER BY zone, ts_utc
