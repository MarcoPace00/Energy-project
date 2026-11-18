-- Step 2 (transform): raw Open-Meteo JSON  ->  one tidy hourly table.
--
-- Input:  every file matching $raw_glob, e.g. raw/open_meteo/zone=*/month=*/data.json
--         hive_partitioning = true turns the folder names "zone=NORD" and "month=2026-10"
--         into columns, so the zone code never has to be stored inside the file.
-- Output: one row per (zone, hour), typed columns, UTC timestamps.
--
-- Time labels follow the power-market convention (ENTSO-E, day-ahead auction):
-- a row at 13:00 describes the hour 13:00-14:00 (interval-beginning).
-- Open-Meteo labels its hourly *averages* by the end of the hour instead: its
-- shortwave_radiation at 13:00 is the mean over 12:00-13:00. So radiation is moved one
-- hour back (the value labelled 14:00 goes to the 13:00 row). Temperature, wind and
-- cloud cover are instant readings at the labelled time and are not moved.
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
, typed AS (
    SELECT
        zone,
        CAST(ts_text AS TIMESTAMP)              AS ts_utc,
        CAST(temperature_c AS FLOAT)            AS temperature_c,
        CAST(wind_speed_100m_kmh AS FLOAT)      AS wind_speed_100m_kmh,
        CAST(solar_radiation_wm2 AS FLOAT)      AS solar_radiation_wm2,
        CAST(cloud_cover_pct AS FLOAT)          AS cloud_cover_pct
    FROM unpacked
)
SELECT
    t.zone,
    t.ts_utc,
    t.temperature_c,
    t.wind_speed_100m_kmh,
    nxt.solar_radiation_wm2,  -- mean over [ts_utc, ts_utc + 1 h), labelled ts_utc + 1 h by Open-Meteo
    t.cloud_cover_pct
FROM typed t
JOIN typed nxt ON nxt.zone = t.zone AND nxt.ts_utc = t.ts_utc + INTERVAL 1 HOUR
-- the archive returns empty values for the last few days until they are published;
-- the newest hour has no "next" row yet and drops out through the join
WHERE t.temperature_c IS NOT NULL AND nxt.solar_radiation_wm2 IS NOT NULL
ORDER BY t.zone, t.ts_utc
