-- Serving-layer schema.
--
-- Deliberately split into three groups, because in a Lambda architecture the
-- serving layer is where the two views meet and the reader must be able to tell
-- them apart:
--
--   rt_*     speed-layer views  -- approximate, seconds old, overwritten continuously
--   daily_*  batch-layer views  -- authoritative, one simulated day behind, recomputed
--   *_audit  provenance         -- which run produced which numbers
--
-- Every table has a natural primary key so both layers can write idempotently
-- (INSERT ... ON CONFLICT), which is what makes their at-least-once delivery safe.

-- =====================================================================
-- Speed layer
-- =====================================================================

-- Latest known state of each vehicle. One row per vehicle, continuously updated.
CREATE TABLE IF NOT EXISTS rt_vehicle_state (
    vehicle_id        TEXT PRIMARY KEY,
    driver_id         TEXT,
    trip_id           TEXT,
    status            TEXT NOT NULL,
    zone              TEXT,
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    speed_kmh         DOUBLE PRECISION,
    fare              DOUBLE PRECISION,
    last_event_time   TIMESTAMPTZ NOT NULL,   -- simulated clock
    idle_since        TIMESTAMPTZ,            -- simulated clock; NULL while working
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Windowed fleet metrics per operating zone. Window bounds are on the simulated
-- event-time axis, so a "1 hour" window means one simulated hour.
CREATE TABLE IF NOT EXISTS rt_zone_metrics (
    window_start      TIMESTAMPTZ NOT NULL,
    window_end        TIMESTAMPTZ NOT NULL,
    zone              TEXT NOT NULL,
    events            BIGINT      NOT NULL,
    active_vehicles   INTEGER     NOT NULL,
    total_vehicles    INTEGER     NOT NULL,
    idle_events       BIGINT      NOT NULL,
    idle_ratio        DOUBLE PRECISION NOT NULL,
    trips             INTEGER     NOT NULL,
    earnings          DOUBLE PRECISION NOT NULL,
    avg_speed_kmh     DOUBLE PRECISION,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);

CREATE INDEX IF NOT EXISTS idx_rt_zone_metrics_window ON rt_zone_metrics (window_start DESC);

-- Threshold alerts raised by the speed layer.
-- The unique key is (type, vehicle, idle_since): one alert per idle spell, not
-- one per micro-batch, so a vehicle parked for hours does not produce a flood.
CREATE TABLE IF NOT EXISTS rt_alerts (
    alert_id          BIGSERIAL PRIMARY KEY,
    alert_type        TEXT NOT NULL,
    vehicle_id        TEXT NOT NULL,
    severity          TEXT NOT NULL,
    message           TEXT NOT NULL,
    zone              TEXT,
    idle_since        TIMESTAMPTZ,
    sim_detected_at   TIMESTAMPTZ NOT NULL,
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged      BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT uq_alert_spell UNIQUE (alert_type, vehicle_id, idle_since)
);

CREATE INDEX IF NOT EXISTS idx_rt_alerts_recent ON rt_alerts (detected_at DESC);

-- =====================================================================
-- Batch layer
-- =====================================================================

-- The authoritative per-vehicle daily reconciliation: telemetry-derived revenue
-- and distance, joined to the fuel partner's reported costs.
CREATE TABLE IF NOT EXISTS daily_vehicle_profitability (
    sim_date               DATE NOT NULL,
    vehicle_id             TEXT NOT NULL,
    driver_id              TEXT,
    trips                  INTEGER NOT NULL,
    revenue                DOUBLE PRECISION NOT NULL,
    gps_distance_km        DOUBLE PRECISION NOT NULL,
    reported_distance_km   DOUBLE PRECISION,
    distance_variance_pct  DOUBLE PRECISION,
    fuel_cost              DOUBLE PRECISION,
    maintenance_cost       DOUBLE PRECISION,
    total_cost             DOUBLE PRECISION,
    profit                 DOUBLE PRECISION,
    margin_pct             DOUBLE PRECISION,
    revenue_per_km         DOUBLE PRECISION,
    utilisation_pct        DOUBLE PRECISION,
    service_flag           TEXT,
    expense_data_present   BOOLEAN NOT NULL,
    distance_disputed      BOOLEAN NOT NULL,
    unprofitable           BOOLEAN NOT NULL,
    run_id                 TEXT NOT NULL,
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, vehicle_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_profit_date ON daily_vehicle_profitability (sim_date DESC);

-- Fleet-level roll-up of the same run, so the dashboard does not re-aggregate.
CREATE TABLE IF NOT EXISTS daily_fleet_summary (
    sim_date               DATE PRIMARY KEY,
    vehicles               INTEGER NOT NULL,
    trips                  INTEGER NOT NULL,
    revenue                DOUBLE PRECISION NOT NULL,
    total_cost             DOUBLE PRECISION NOT NULL,
    profit                 DOUBLE PRECISION NOT NULL,
    margin_pct             DOUBLE PRECISION,
    unprofitable_vehicles  INTEGER NOT NULL,
    disputed_vehicles      INTEGER NOT NULL,
    missing_expense_rows   INTEGER NOT NULL,
    run_id                 TEXT NOT NULL,
    computed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =====================================================================
-- Provenance
-- =====================================================================

CREATE TABLE IF NOT EXISTS batch_run_audit (
    run_id            TEXT PRIMARY KEY,
    sim_date          DATE NOT NULL,
    status            TEXT NOT NULL,
    telemetry_rows    BIGINT,
    clean_rows        BIGINT,
    quarantined_rows  BIGINT,
    expense_rows      BIGINT,
    vehicles_out      INTEGER,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    duration_seconds  DOUBLE PRECISION,
    notes             TEXT
);

CREATE INDEX IF NOT EXISTS idx_batch_run_date ON batch_run_audit (sim_date DESC, started_at DESC);
