-- Database schema for the RUCO oil tracker.
-- PostgreSQL runs this file automatically the FIRST time the database is created
-- (it is mounted into /docker-entrypoint-initdb.d/ by docker-compose.yml).

-- 1) Reference table: one row per fryer, including its oil volume (used for ESG)
CREATE TABLE fryers (
    fryer_id     TEXT PRIMARY KEY,
    fryer_name   TEXT NOT NULL,
    temperature  INTEGER NOT NULL,      -- frying temperature in °C
    oil_litres   DOUBLE PRECISION NOT NULL   -- litres of oil in the fryer (ESG assumption)
);

INSERT INTO fryers VALUES
    ('F160', 'Bakery',          160, 10),
    ('F175', 'Cafe',            175,  8),
    ('F190', 'College Canteen', 190, 25),
    ('F205', 'Restaurant',      205, 20),
    ('F220', 'Street Stall',    220, 15);

-- 2) Every cleaned reading written by Spark (one row per fryer per 0.1 frying hour)
CREATE TABLE readings (
    id                      BIGSERIAL PRIMARY KEY,
    run_id                  BIGINT NOT NULL,          -- which Spark run wrote it (start time, epoch seconds)
    ts                      TIMESTAMPTZ NOT NULL,     -- when the sensor reading was sent
    fryer_id                TEXT NOT NULL REFERENCES fryers(fryer_id),
    fryer_name              TEXT,
    temperature             INTEGER,
    data_hour               DOUBLE PRECISION,         -- frying hours since the oil was changed
    tpc_raw                 DOUBLE PRECISION,         -- as received (may be an outlier or NULL)
    tpc_clean               DOUBLE PRECISION,         -- outlier removed / missing value filled
    tpc_rolling             DOUBLE PRECISION,         -- rolling average over 1 frying hour
    acid_value              DOUBLE PRECISION,
    carbonyl_value          DOUBLE PRECISION,
    z_score                 DOUBLE PRECISION,
    is_outlier              BOOLEAN,
    was_missing             BOOLEAN,
    status                  TEXT,                     -- WARMING_UP / OK / WARNING / DISCARD
    live_slope              DOUBLE PRECISION,
    rate_per_hour           DOUBLE PRECISION,         -- degradation rate used (% TPC per hour)
    rate_source             TEXT,                     -- 'model' or 'live'
    hours_left              DOUBLE PRECISION,
    predicted_discard_hour  DOUBLE PRECISION,
    extrapolated            BOOLEAN                   -- prediction beyond the 30 lab hours
);
CREATE INDEX readings_run_idx ON readings (run_id, fryer_id);

-- 3) Discard alerts (one per fryer per oil cycle)
CREATE TABLE alerts (
    id           BIGSERIAL PRIMARY KEY,
    run_id       BIGINT NOT NULL,
    ts           TIMESTAMPTZ NOT NULL,
    fryer_id     TEXT NOT NULL REFERENCES fryers(fryer_id),
    fryer_name   TEXT,
    data_hour    DOUBLE PRECISION,
    tpc_rolling  DOUBLE PRECISION,
    message      TEXT
);

-- 4) ESG impact of every alert: the whole fryer of oil goes to RUCO for biodiesel.
--    Assumptions: 1 L used oil -> 0.85 L biodiesel; 2.2 kg CO2 avoided per L biodiesel.
CREATE VIEW alert_impact AS
SELECT a.id,
       a.run_id,
       a.ts,
       a.fryer_id,
       a.fryer_name,
       a.data_hour,
       a.tpc_rolling,
       a.message,
       f.oil_litres,
       f.oil_litres * 0.85          AS biodiesel_litres,
       f.oil_litres * 0.85 * 2.2    AS co2_avoided_kg
FROM alerts a
JOIN fryers f ON f.fryer_id = a.fryer_id;
