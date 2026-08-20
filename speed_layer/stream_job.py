"""Lambda SPEED LAYER -- Spark Structured Streaming.

Reads the telemetry topic and does four things, split across two streaming
queries so that a slow sink cannot stall the others' watermarks:

  Query "ingest-and-state"  (trigger: 10s)
    1. appends every parsed event -- clean *and* rejected -- to the Parquet
       master dataset, partitioned by simulated date. This is the immutable
       record the batch layer recomputes from; it must not be filtered here,
       or the batch layer could never revise a decision made in the stream.
    2. republishes rejected events to the quarantine topic with their violation
       reasons attached.
    3. upserts each vehicle's latest state into Postgres.
    4. raises threshold alerts for vehicles idle beyond the configured limit.

  Query "zone-windows"      (trigger: 15s)
    5. tumbling window aggregation per operating zone -- active vehicles, idle
       ratio, trips, earnings -- upserted into Postgres for the live dashboard.

Approximation is deliberate here. `approx_count_distinct` (HyperLogLog) is used
for vehicle and trip counts because exact `countDistinct` is not supported in a
streaming aggregation, and the speed layer's job is a fast approximate answer.
The batch layer recomputes the same quantities exactly from the master dataset.
That gap between the two is the whole reason this project is Lambda and not a
single stream.

Run:
    spark-submit /opt/project/speed_layer/stream_job.py
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Summary, start_http_server
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

from common.config import SETTINGS
from common.db import connect
from common.geo import zone_for
from common.logging_setup import configure
from common.schemas import TELEMETRY_DDL, TELEMETRY_FIELDS
from common.validation import telemetry_violations

log = configure("speed-layer", "process", SETTINGS.log_level)

EVENTS = Counter("speed_events_total", "Events processed by the speed layer", ["outcome"])
VIOLATIONS = Counter("speed_violations_total", "Validation rule hits", ["rule"])
BATCHES = Counter("speed_batches_total", "Micro-batches completed", ["query"])
BATCH_SECONDS = Summary("speed_batch_duration_seconds", "Micro-batch wall time", ["query"])
ROWS_WRITTEN = Counter("speed_rows_written_total", "Rows written downstream", ["sink"])
ALERTS = Counter("speed_alerts_raised_total", "Threshold alerts raised", ["alert_type"])
LAST_BATCH = Gauge("speed_last_batch_unixtime", "Wall-clock time the last micro-batch finished", ["query"])
LAST_EVENT_LAG = Gauge("speed_ingest_lag_seconds", "Real seconds between event ingest and processing")
FAILURES = Counter("speed_batch_failures_total", "Micro-batches that raised", ["query"])


# --- UDFs -----------------------------------------------------------------
# Both wrap functions in common/ so the pipeline and the unit tests run the same
# code. A Python UDF costs a serialisation hop per row; at this volume that is
# irrelevant, and the report records what would replace it at scale.

@F.udf(returnType=ArrayType(StringType()))
def violations_udf(row):
    return telemetry_violations(row.asDict() if row is not None else {})


@F.udf(returnType=StringType())
def zone_udf(lat, lon):
    return zone_for(lat, lon)


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # 12 vehicles at a few events per second is a tiny stream; the default
        # 200 shuffle partitions would create 200 near-empty tasks per batch.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )


def read_telemetry(spark: SparkSession, starting_offsets: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", SETTINGS.bootstrap_servers)
        .option("subscribe", SETTINGS.telemetry_topic)
        .option("startingOffsets", starting_offsets)
        # Bounds how much backlog one micro-batch may swallow after a restart,
        # so recovery is gradual instead of one enormous batch.
        .option("maxOffsetsPerTrigger", 20000)
        .load()
    )

    parsed = (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_time"),
            F.from_json(F.col("value").cast("string"), TELEMETRY_DDL).alias("e"),
        )
        .select("kafka_key", "kafka_partition", "kafka_offset", "kafka_time", "e.*")
    )

    # Validate, then enrich. Enrichment happens after validation so that a
    # rejected row never gets a zone it does not deserve.
    flagged = parsed.withColumn("violations", violations_udf(F.struct(*TELEMETRY_FIELDS)))

    return (
        flagged
        .withColumn("is_valid", F.size("violations") == 0)
        # Never let a null partition key silently create a __HIVE_DEFAULT_PARTITION__
        # directory the batch layer would then fail to find.
        .withColumn("sim_date", F.coalesce(F.col("sim_date"), F.lit("unknown")))
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("ingest_ts", F.to_timestamp("ingest_time"))
        .withColumn("zone", F.when(F.col("is_valid"), zone_udf(F.col("lat"), F.col("lon"))).otherwise(F.lit(None)))
        .withColumn("is_active", F.col("status").isin("enroute", "on_trip"))
        .withColumn("processed_at", F.current_timestamp())
    )


# --- Query 1: master dataset, quarantine, vehicle state, alerts -----------

def with_retries(operation, attempts: int, on_error, label: str):
    """Retry a micro-batch's side effects before failing the query.

    A dropped Postgres connection is the common failure here and it recovers on
    the next attempt. Only when the budget is spent does the exception escape --
    at which point the query stops *without* committing its offsets, so nothing
    is lost and a restart reprocesses the batch.
    """
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as exc:  # noqa: BLE001 - deliberately broad, then re-raised
            last = exc
            log.warning("micro_batch_attempt_failed", extra={
                "query": label, "attempt": attempt, "of": attempts, "error": str(exc),
            })
            on_error()
            if attempt < attempts:
                time.sleep(min(5.0, 0.5 * 2 ** attempt))
    raise last  # type: ignore[misc]


def write_master_dataset(batch: DataFrame, lake_path: str, total: int) -> None:
    """Append the batch verbatim to the immutable master dataset."""
    (
        batch.drop("is_valid", "is_active")
        .write.mode("append")
        .partitionBy("sim_date")
        .parquet(lake_path)
    )
    ROWS_WRITTEN.labels(sink="lake_parquet").inc(total)


def publish_quarantine(batch: DataFrame) -> int:
    """Rejected events go to their own topic, with the reasons attached."""
    rejected = batch.filter(~F.col("is_valid"))
    count = rejected.count()
    if count:
        (
            rejected.select(
                F.coalesce(F.col("kafka_key"), F.lit("unknown")).alias("key"),
                F.to_json(F.struct(
                    # event_id first: the quarantine record must be joinable back
                    # to the exact source event that produced it.
                    "event_id",
                    "vehicle_id", "trip_id", "event_time", "ingest_time", "status",
                    "lat", "lon", "speed_kmh", "fare", "fare_increment",
                    "violations", "kafka_partition", "kafka_offset",
                )).alias("value"),
            )
            .write.format("kafka")
            .option("kafka.bootstrap.servers", SETTINGS.bootstrap_servers)
            .option("topic", SETTINGS.quarantine_topic)
            .save()
        )
        ROWS_WRITTEN.labels(sink="quarantine_topic").inc(count)
    return count


VEHICLE_STATE_SQL = """
INSERT INTO rt_vehicle_state
    (vehicle_id, driver_id, trip_id, status, zone, lat, lon, speed_kmh, fare,
     last_event_time, idle_since, updated_at)
VALUES %s
ON CONFLICT (vehicle_id) DO UPDATE SET
    driver_id       = EXCLUDED.driver_id,
    trip_id         = EXCLUDED.trip_id,
    status          = EXCLUDED.status,
    zone            = EXCLUDED.zone,
    lat             = EXCLUDED.lat,
    lon             = EXCLUDED.lon,
    speed_kmh       = EXCLUDED.speed_kmh,
    fare            = EXCLUDED.fare,
    last_event_time = EXCLUDED.last_event_time,
    -- An idle spell keeps its original start time until the vehicle moves,
    -- so "idle for 45 minutes" measures the spell, not the micro-batch.
    idle_since = CASE
        WHEN EXCLUDED.status <> 'idle' THEN NULL
        WHEN rt_vehicle_state.status = 'idle' AND rt_vehicle_state.idle_since IS NOT NULL
            THEN rt_vehicle_state.idle_since
        ELSE EXCLUDED.last_event_time
    END,
    updated_at = now()
-- Out-of-order or replayed events must not rewind the live view.
WHERE EXCLUDED.last_event_time >= rt_vehicle_state.last_event_time
"""

IDLE_ALERT_SQL = """
INSERT INTO rt_alerts
    (alert_type, vehicle_id, severity, message, zone, idle_since, sim_detected_at)
SELECT
    'VEHICLE_IDLE_TOO_LONG',
    vehicle_id,
    CASE WHEN EXTRACT(EPOCH FROM (last_event_time - idle_since)) / 60 >= %(threshold)s * 2
         THEN 'critical' ELSE 'warning' END,
    'Vehicle ' || vehicle_id || ' idle for ' ||
        ROUND((EXTRACT(EPOCH FROM (last_event_time - idle_since)) / 60)::numeric, 1) ||
        ' simulated minutes in ' || COALESCE(zone, 'unknown zone'),
    zone,
    idle_since,
    last_event_time
FROM rt_vehicle_state
WHERE status = 'idle'
  AND idle_since IS NOT NULL
  AND EXTRACT(EPOCH FROM (last_event_time - idle_since)) / 60 >= %(threshold)s
ON CONFLICT ON CONSTRAINT uq_alert_spell DO NOTHING
RETURNING vehicle_id
"""


def upsert_vehicle_state(batch: DataFrame, conn) -> int:
    """Keep only the newest event per vehicle in this batch, then upsert."""
    newest = (
        batch.filter(F.col("is_valid"))
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("vehicle_id").orderBy(F.col("event_ts").desc(), F.col("seq").desc())
        ))
        .filter(F.col("rn") == 1)
        .select("vehicle_id", "driver_id", "trip_id", "status", "zone",
                "lat", "lon", "speed_kmh", "fare", "event_ts")
    )

    rows = []
    now = datetime.now(tz=timezone.utc)
    for r in newest.collect():
        idle_since = r["event_ts"] if r["status"] == "idle" else None
        rows.append((r["vehicle_id"], r["driver_id"], r["trip_id"], r["status"], r["zone"],
                     r["lat"], r["lon"], r["speed_kmh"], r["fare"], r["event_ts"], idle_since, now))

    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, VEHICLE_STATE_SQL, rows)
    conn.commit()
    ROWS_WRITTEN.labels(sink="rt_vehicle_state").inc(len(rows))
    return len(rows)


def raise_idle_alerts(conn, threshold_minutes: int) -> list[str]:
    """One alert per idle spell; the unique constraint suppresses repeats."""
    with conn.cursor() as cur:
        cur.execute(IDLE_ALERT_SQL, {"threshold": threshold_minutes})
        raised = [row[0] for row in cur.fetchall()]
    conn.commit()
    for vehicle_id in raised:
        ALERTS.labels(alert_type="VEHICLE_IDLE_TOO_LONG").inc()
        log.warning("alert_raised", extra={
            "alert_type": "VEHICLE_IDLE_TOO_LONG",
            "vehicle_id": vehicle_id,
            "threshold_sim_minutes": threshold_minutes,
        })
    return raised


def reconnect(conn_holder: dict) -> None:
    try:
        conn_holder["conn"].close()
    except Exception:  # noqa: BLE001 - already broken, nothing to salvage
        pass
    conn_holder["conn"] = connect()


def make_ingest_handler(lake_path: str, conn_holder: dict):
    def handle(batch: DataFrame, batch_id: int) -> None:
        started = time.time()
        try:
            batch.persist()
            total = batch.count()
            if total == 0:
                return

            valid = batch.filter(F.col("is_valid")).count()
            EVENTS.labels(outcome="clean").inc(valid)
            EVENTS.labels(outcome="quarantined").inc(total - valid)

            for rule, count in (
                batch.filter(~F.col("is_valid"))
                .select(F.explode("violations").alias("rule"))
                .groupBy("rule").count().collect()
            ):
                VIOLATIONS.labels(rule=rule).inc(count)

            def sink() -> tuple[int, int, list[str]]:
                write_master_dataset(batch, lake_path, total)
                quarantined = publish_quarantine(batch)
                conn = conn_holder["conn"]
                vehicles = upsert_vehicle_state(batch, conn)
                alerts = raise_idle_alerts(conn, SETTINGS.idle_alert_minutes)
                return quarantined, vehicles, alerts

            quarantined, vehicles, alerts = with_retries(
                sink, attempts=3, on_error=lambda: reconnect(conn_holder), label="ingest-and-state"
            )

            lag = batch.select(
                F.max(F.unix_timestamp(F.col("processed_at")) - F.unix_timestamp(F.col("ingest_ts")))
            ).collect()[0][0]
            if lag is not None:
                LAST_EVENT_LAG.set(float(lag))

            log.info("micro_batch_complete", extra={
                "query": "ingest-and-state",
                "batch_id": batch_id,
                "events": total,
                "clean": valid,
                "quarantined": quarantined,
                "vehicles_updated": vehicles,
                "alerts_raised": len(alerts),
                "ingest_lag_seconds": lag,
                "duration_seconds": round(time.time() - started, 3),
            })
            BATCHES.labels(query="ingest-and-state").inc()
            ROWS_WRITTEN.labels(sink="rt_alerts").inc(len(alerts))
        except Exception:
            FAILURES.labels(query="ingest-and-state").inc()
            log.exception("micro_batch_failed", extra={"query": "ingest-and-state", "batch_id": batch_id})
            raise
        finally:
            batch.unpersist()
            BATCH_SECONDS.labels(query="ingest-and-state").observe(time.time() - started)
            LAST_BATCH.labels(query="ingest-and-state").set(time.time())

    return handle


# --- Query 2: windowed zone metrics ---------------------------------------

ZONE_METRICS_SQL = """
INSERT INTO rt_zone_metrics
    (window_start, window_end, zone, events, active_vehicles, total_vehicles,
     idle_events, idle_ratio, trips, earnings, avg_speed_kmh, updated_at)
VALUES %s
ON CONFLICT (window_start, zone) DO UPDATE SET
    window_end      = EXCLUDED.window_end,
    events          = EXCLUDED.events,
    active_vehicles = EXCLUDED.active_vehicles,
    total_vehicles  = EXCLUDED.total_vehicles,
    idle_events     = EXCLUDED.idle_events,
    idle_ratio      = EXCLUDED.idle_ratio,
    trips           = EXCLUDED.trips,
    earnings        = EXCLUDED.earnings,
    avg_speed_kmh   = EXCLUDED.avg_speed_kmh,
    updated_at      = now()
"""


def zone_windows(stream: DataFrame) -> DataFrame:
    """Tumbling windows over *simulated* event time, per operating zone."""
    return (
        stream.filter(F.col("is_valid"))
        # Lateness is expressed in simulated minutes: at 288x compression the
        # default 30 simulated minutes is ~6 real seconds of tolerance.
        .withWatermark("event_ts", f"{SETTINGS.watermark_minutes} minutes")
        .groupBy(F.window("event_ts", f"{SETTINGS.window_minutes} minutes"), F.col("zone"))
        .agg(
            F.count("*").alias("events"),
            F.approx_count_distinct(F.when(F.col("is_active"), F.col("vehicle_id"))).alias("active_vehicles"),
            F.approx_count_distinct("vehicle_id").alias("total_vehicles"),
            F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)).alias("idle_events"),
            F.approx_count_distinct("trip_id").alias("trips"),
            F.sum("fare_increment").alias("earnings"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("zone"),
            "events", "active_vehicles", "total_vehicles", "idle_events",
            (F.col("idle_events") / F.col("events")).alias("idle_ratio"),
            "trips",
            F.round(F.col("earnings"), 2).alias("earnings"),
            F.round(F.col("avg_speed_kmh"), 2).alias("avg_speed_kmh"),
        )
    )


def make_window_handler(conn_holder: dict):
    def handle(batch: DataFrame, batch_id: int) -> None:
        started = time.time()
        try:
            rows = [
                (r["window_start"], r["window_end"], r["zone"], r["events"], r["active_vehicles"],
                 r["total_vehicles"], r["idle_events"], float(r["idle_ratio"] or 0.0), r["trips"],
                 float(r["earnings"] or 0.0), r["avg_speed_kmh"], datetime.now(tz=timezone.utc))
                for r in batch.collect()
            ]
            if not rows:
                return

            def sink() -> None:
                conn = conn_holder["conn"]
                with conn.cursor() as cur:
                    execute_values(cur, ZONE_METRICS_SQL, rows)
                conn.commit()

            with_retries(sink, attempts=3, on_error=lambda: reconnect(conn_holder), label="zone-windows")

            ROWS_WRITTEN.labels(sink="rt_zone_metrics").inc(len(rows))
            BATCHES.labels(query="zone-windows").inc()
            log.info("micro_batch_complete", extra={
                "query": "zone-windows",
                "batch_id": batch_id,
                "windows_upserted": len(rows),
                "zones": sorted({r[2] for r in rows if r[2]}),
                "duration_seconds": round(time.time() - started, 3),
            })
        except Exception:
            FAILURES.labels(query="zone-windows").inc()
            log.exception("micro_batch_failed", extra={"query": "zone-windows", "batch_id": batch_id})
            raise
        finally:
            BATCH_SECONDS.labels(query="zone-windows").observe(time.time() - started)
            LAST_BATCH.labels(query="zone-windows").set(time.time())

    return handle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lambda speed layer")
    parser.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                        help="'earliest' replays the retained log -- the Lambda recomputation escape hatch")
    parser.add_argument("--metrics-port", type=int, default=SETTINGS.metrics_port_speed)
    args = parser.parse_args(argv)

    start_http_server(args.metrics_port)

    lake_path = f"{SETTINGS.data_dir}/lake/telemetry"
    checkpoint_root = f"{SETTINGS.data_dir}/checkpoints"

    spark = build_spark("fleet-speed-layer")
    spark.sparkContext.setLogLevel("WARN")

    stream = read_telemetry(spark, args.starting_offsets)
    conn_holder = {"conn": connect()}

    log.info("speed_layer_started", extra={
        "topic": SETTINGS.telemetry_topic,
        "lake_path": lake_path,
        "window_sim_minutes": SETTINGS.window_minutes,
        "watermark_sim_minutes": SETTINGS.watermark_minutes,
        "idle_alert_sim_minutes": SETTINGS.idle_alert_minutes,
        "starting_offsets": args.starting_offsets,
    })

    ingest_query = (
        stream.writeStream
        .foreachBatch(make_ingest_handler(lake_path, conn_holder))
        .option("checkpointLocation", f"{checkpoint_root}/ingest_and_state")
        .trigger(processingTime="10 seconds")
        .queryName("ingest-and-state")
        .start()
    )

    window_query = (
        zone_windows(stream).writeStream
        .outputMode("update")
        .foreachBatch(make_window_handler(conn_holder))
        .option("checkpointLocation", f"{checkpoint_root}/zone_windows")
        .trigger(processingTime="15 seconds")
        .queryName("zone-windows")
        .start()
    )

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for q in (ingest_query, window_query):
            try:
                q.stop()
            except Exception:
                pass
        log.info("speed_layer_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
