"""Lambda BATCH LAYER -- authoritative daily profitability reconciliation.

Runs once per simulated day, orchestrated by Airflow. Recomputes the day's
numbers from scratch out of the immutable Parquet master dataset and joins them
to the fuel partner's expense file:

    master dataset (Parquet, one sim day)      landing/expenses_<date>.csv
                |                                        |
      filter to validated events                  read with an explicit schema
                |                                        |
      per trip: final cumulative fare                     |
      per vehicle: trips, revenue,                        |
                   GPS distance (haversine over           |
                   consecutive fixes), utilisation        |
                \_______________________  ______________/
                                        \/
                        left join on vehicle_id (the partner's file
                        is incomplete by design -- missing rows must
                        not delete vehicles from the report)
                                        |
              revenue - (fuel + maintenance) = profit, margin, variance
                                        |
        Postgres (idempotent upsert) + CSV + standalone HTML report

Why this exists at all, given the speed layer already reports earnings: the
speed layer sums fare *increments* inside a watermarked window using
HyperLogLog counts, so late events and approximation make it directionally right
but not bankable. This layer reads the complete day after the fact, counts
exactly, and is allowed to disagree. That disagreement is the Lambda bargain,
and it is quantified in the report.

Re-running the same simulated date is safe and expected: every write is an
upsert keyed on (sim_date, vehicle_id), so a corrected re-run replaces the day's
numbers rather than doubling them.

Run:
    spark-submit /opt/project/batch_layer/daily_reconciliation.py --sim-date 2026-08-01
"""
from __future__ import annotations

import argparse
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.config import SETTINGS
from common.db import connect, upsert
from common.logging_setup import configure
from common.schemas import EXPENSE_DDL

# Absolute imports, not relative: spark-submit runs this file as __main__, so the
# module has no package context. PYTHONPATH=/opt/project makes these resolve.
from batch_layer.profitability import (
    DISTANCE_DISPUTE_PCT,
    LOW_MARGIN_FRACTION,
    MIN_DISTANCE_FOR_VARIANCE_KM,
    summarise,
)
from batch_layer.report_render import write_csv, write_html

log = configure("batch-layer", "process", SETTINGS.log_level)

# Two consecutive GPS fixes further apart than this are a bad fix, not a
# journey; the segment is dropped rather than inflating the day's distance.
MAX_SEGMENT_KM = 25.0

EARTH_DIAMETER_KM = 2 * 6371.0088


class BatchFailure(RuntimeError):
    """Raised when the run cannot produce a trustworthy report."""


# --- Spark helpers --------------------------------------------------------

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance as a Spark column expression.

    Same formula as common.geo.haversine_km, which the unit tests pin; expressed
    natively here so the whole day's distance calculation stays inside the JVM
    instead of paying a Python round trip per GPS fix.
    """
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2)
    )
    return F.lit(EARTH_DIAMETER_KM) * F.asin(F.sqrt(a))


def read_master_dataset(spark: SparkSession, lake_path: str, sim_date: str) -> DataFrame:
    """Read one simulated day out of the append-only master dataset."""
    partition = Path(lake_path) / f"sim_date={sim_date}"
    if not partition.exists():
        raise BatchFailure(
            f"no telemetry in the master dataset for {sim_date} (expected {partition}); "
            "is the speed layer running?"
        )
    # Read the partition directory directly and re-attach the partition column:
    # cheaper than scanning the whole lake and filtering, and it fails loudly
    # rather than silently returning zero rows for a date that never landed.
    return spark.read.parquet(str(partition)).withColumn("sim_date", F.lit(sim_date))


def read_expenses(spark: SparkSession, path: Path) -> DataFrame:
    """Read the partner's file with an explicit schema.

    Never inferSchema on an external feed: a day where every maintenance_cost
    happens to be a whole number would silently arrive as integers and change
    the arithmetic downstream.
    """
    if not path.exists():
        raise BatchFailure(f"expense file missing: {path}")
    return (
        spark.read.option("header", True)
        .schema(EXPENSE_DDL)
        .csv(str(path))
        .drop("sim_date")           # the filename is authoritative for the date
        .dropDuplicates(["vehicle_id"])
    )


def per_vehicle_revenue(clean: DataFrame) -> DataFrame:
    """Revenue = the final cumulative fare of every trip the vehicle completed.

    The producer emits a running fare per trip, so the last value seen for a
    trip_id is what the passenger paid. Taking max() rather than the last event
    also makes the result independent of event order -- important, because the
    master dataset is a union of micro-batches with no global ordering.
    """
    trips = (
        clean.filter(F.col("trip_id").isNotNull())
        .groupBy("vehicle_id", "trip_id")
        .agg(F.max("fare").alias("trip_revenue"))
    )
    return trips.groupBy("vehicle_id").agg(
        F.count("trip_id").alias("trips"),
        F.round(F.sum("trip_revenue"), 2).alias("revenue"),
    )


def per_vehicle_movement(clean: DataFrame) -> DataFrame:
    """Distance actually covered, plus how much of the day was spent working."""
    ordered = Window.partitionBy("vehicle_id").orderBy("event_ts", "seq")

    segments = (
        clean.withColumn("prev_lat", F.lag("lat").over(ordered))
        .withColumn("prev_lon", F.lag("lon").over(ordered))
        .withColumn(
            "segment_km",
            F.when(
                F.col("prev_lat").isNotNull(),
                haversine_km(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")),
            ).otherwise(F.lit(0.0)),
        )
        # Discard implausible jumps instead of letting one bad fix dominate.
        .withColumn(
            "segment_km",
            F.when(F.col("segment_km") <= MAX_SEGMENT_KM, F.col("segment_km")).otherwise(F.lit(0.0)),
        )
    )

    return segments.groupBy("vehicle_id").agg(
        F.max("driver_id").alias("driver_id"),
        F.round(F.sum("segment_km"), 3).alias("gps_distance_km"),
        F.count("*").alias("events"),
        F.round(
            F.sum(F.when(F.col("status") != "idle", 1).otherwise(0)) / F.count("*") * 100, 2
        ).alias("utilisation_pct"),
    )


def reconcile(movement: DataFrame, revenue: DataFrame, expenses: DataFrame, sim_date: str,
              run_id: str) -> DataFrame:
    """Join the three sources and compute the economics.

    Left joins throughout: a vehicle that drove but was not invoiced, and a
    vehicle that was invoiced but never moved, are both real situations the
    report has to show rather than quietly drop.
    """
    joined = (
        movement.join(revenue, "vehicle_id", "left")
        .join(expenses, "vehicle_id", "left")
        .fillna({"trips": 0, "revenue": 0.0})
    )

    total_cost = F.coalesce(F.col("fuel_cost"), F.lit(0.0)) + F.coalesce(F.col("maintenance_cost"), F.lit(0.0))
    profit = F.col("revenue") - total_cost
    variance = F.when(
        F.col("distance_covered").isNotNull() & (F.col("gps_distance_km") >= MIN_DISTANCE_FOR_VARIANCE_KM),
        (F.col("distance_covered") - F.col("gps_distance_km")) / F.col("gps_distance_km") * 100,
    )
    margin = F.when(F.col("revenue") > 0, profit / F.col("revenue") * 100)

    return (
        joined.withColumn("total_cost", F.round(total_cost, 2))
        .withColumn("profit", F.round(profit, 2))
        .withColumn("margin_pct", F.round(margin, 2))
        .withColumn("distance_variance_pct", F.round(variance, 2))
        .withColumn(
            "revenue_per_km",
            F.when(F.col("gps_distance_km") > 0, F.round(F.col("revenue") / F.col("gps_distance_km"), 2)),
        )
        .withColumn("expense_data_present", F.col("fuel_cost").isNotNull())
        .withColumn(
            "distance_disputed",
            F.coalesce(F.abs(F.col("distance_variance_pct")) > F.lit(DISTANCE_DISPUTE_PCT), F.lit(False)),
        )
        .withColumn(
            # margin_pct is NULL when there was no revenue; coalesce keeps the
            # column NOT NULL-safe, and no-revenue days are unprofitable anyway.
            "unprofitable",
            F.coalesce(
                (F.col("profit") <= 0) | (F.col("margin_pct") < F.lit(LOW_MARGIN_FRACTION * 100)),
                F.lit(True),
            ),
        )
        .withColumnRenamed("distance_covered", "reported_distance_km")
        .withColumn("sim_date", F.lit(sim_date))
        .withColumn("run_id", F.lit(run_id))
        .select(
            "sim_date", "vehicle_id", "driver_id", "trips", "revenue", "gps_distance_km",
            "reported_distance_km", "distance_variance_pct", "fuel_cost", "maintenance_cost",
            "total_cost", "profit", "margin_pct", "revenue_per_km", "utilisation_pct",
            "service_flag", "expense_data_present", "distance_disputed", "unprofitable", "run_id",
        )
    )


# --- Persistence ----------------------------------------------------------

PROFIT_COLUMNS = [
    "sim_date", "vehicle_id", "driver_id", "trips", "revenue", "gps_distance_km",
    "reported_distance_km", "distance_variance_pct", "fuel_cost", "maintenance_cost",
    "total_cost", "profit", "margin_pct", "revenue_per_km", "utilisation_pct",
    "service_flag", "expense_data_present", "distance_disputed", "unprofitable", "run_id",
]

SUMMARY_COLUMNS = [
    "sim_date", "vehicles", "trips", "revenue", "total_cost", "profit", "margin_pct",
    "unprofitable_vehicles", "disputed_vehicles", "missing_expense_rows", "run_id",
]


def persist(rows: list[dict], summary: dict, sim_date: str, run_id: str, conn) -> None:
    upsert(
        "daily_vehicle_profitability",
        PROFIT_COLUMNS,
        [tuple(r[c] for c in PROFIT_COLUMNS) for r in rows],
        conflict_columns=["sim_date", "vehicle_id"],
        conn=conn,
    )
    upsert(
        "daily_fleet_summary",
        SUMMARY_COLUMNS,
        [tuple([sim_date] + [summary[c] for c in SUMMARY_COLUMNS[1:-1]] + [run_id])],
        conflict_columns=["sim_date"],
        conn=conn,
    )


def start_audit(run_id: str, sim_date: str, conn) -> None:
    upsert(
        "batch_run_audit",
        ["run_id", "sim_date", "status", "started_at"],
        [(run_id, sim_date, "running", datetime.now(tz=timezone.utc))],
        conflict_columns=["run_id"],
        conn=conn,
    )


def finish_audit(run_id: str, sim_date: str, status: str, started: float, counts: dict,
                 notes: str, conn) -> None:
    upsert(
        "batch_run_audit",
        ["run_id", "sim_date", "status", "telemetry_rows", "clean_rows", "quarantined_rows",
         "expense_rows", "vehicles_out", "started_at", "finished_at", "duration_seconds", "notes"],
        [(
            run_id, sim_date, status,
            counts.get("telemetry_rows"), counts.get("clean_rows"), counts.get("quarantined_rows"),
            counts.get("expense_rows"), counts.get("vehicles_out"),
            datetime.fromtimestamp(started, tz=timezone.utc), datetime.now(tz=timezone.utc),
            round(time.time() - started, 3), notes[:2000],
        )],
        conflict_columns=["run_id"],
        conn=conn,
    )


def push_metrics(sim_date: str, summary: dict, counts: dict, duration: float, status: str) -> None:
    """Batch jobs are too short-lived for Prometheus to scrape, so they push.

    Grouped by sim_date so a re-run of one day replaces that day's sample
    instead of appending a second, conflicting one.
    """
    registry = CollectorRegistry()
    gauges = {
        "fleet_batch_duration_seconds": duration,
        "fleet_batch_telemetry_rows": counts.get("telemetry_rows", 0),
        "fleet_batch_clean_rows": counts.get("clean_rows", 0),
        "fleet_batch_quarantined_rows": counts.get("quarantined_rows", 0),
        "fleet_batch_expense_rows": counts.get("expense_rows", 0),
        "fleet_batch_vehicles_out": counts.get("vehicles_out", 0),
        "fleet_batch_revenue": summary.get("revenue", 0),
        "fleet_batch_profit": summary.get("profit", 0),
        "fleet_batch_unprofitable_vehicles": summary.get("unprofitable_vehicles", 0),
        "fleet_batch_disputed_vehicles": summary.get("disputed_vehicles", 0),
        "fleet_batch_missing_expense_rows": summary.get("missing_expense_rows", 0),
        "fleet_batch_last_success_unixtime": time.time() if status == "success" else 0,
    }
    for name, value in gauges.items():
        Gauge(name, name.replace("_", " "), registry=registry).set(value or 0)
    Counter("fleet_batch_runs", "Batch runs", ["status"], registry=registry).labels(status=status).inc()

    try:
        push_to_gateway(
            SETTINGS.pushgateway_url.replace("http://", ""),
            job="fleet_daily_batch",
            grouping_key={"sim_date": sim_date},
            registry=registry,
        )
    except Exception as exc:  # noqa: BLE001 - telemetry must never fail the job
        log.warning("pushgateway_unavailable", extra={"error": str(exc)})


# --- Entry point ----------------------------------------------------------

def run(sim_date: str, run_id: str) -> dict:
    started = time.time()
    lake_path = f"{SETTINGS.data_dir}/lake/telemetry"
    expense_path = Path(SETTINGS.data_dir) / "landing" / "expenses" / f"expenses_{sim_date}.csv"
    reports_dir = Path(SETTINGS.data_dir) / "reports"

    conn = connect()
    start_audit(run_id, sim_date, conn)

    spark = build_spark(f"fleet-batch-{sim_date}")
    spark.sparkContext.setLogLevel("WARN")
    counts: dict = {}

    log.info("batch_run_started", extra={
        "run_id": run_id, "sim_date": sim_date,
        "lake_path": lake_path, "expense_path": str(expense_path),
    })

    try:
        telemetry = read_master_dataset(spark, lake_path, sim_date).cache()
        counts["telemetry_rows"] = telemetry.count()

        clean = telemetry.filter(F.size("violations") == 0).cache()
        counts["clean_rows"] = clean.count()
        counts["quarantined_rows"] = counts["telemetry_rows"] - counts["clean_rows"]
        if counts["clean_rows"] == 0:
            raise BatchFailure(f"every telemetry row for {sim_date} failed validation")

        expenses = read_expenses(spark, expense_path).cache()
        counts["expense_rows"] = expenses.count()

        result = reconcile(
            per_vehicle_movement(clean), per_vehicle_revenue(clean), expenses, sim_date, run_id
        ).orderBy("profit")

        rows = [r.asDict() for r in result.collect()]
        counts["vehicles_out"] = len(rows)
        if not rows:
            raise BatchFailure(f"reconciliation produced no vehicles for {sim_date}")

        summary = summarise(rows)
        generated_at = datetime.now(tz=timezone.utc).isoformat(timespec="seconds")

        persist(rows, summary, sim_date, run_id, conn)
        csv_path = write_csv(rows, reports_dir / f"daily_profitability_{sim_date}.csv")
        html_path = write_html(sim_date, rows, summary, run_id, generated_at,
                               reports_dir / f"daily_profitability_{sim_date}.html")

        duration = time.time() - started
        finish_audit(run_id, sim_date, "success", started, counts,
                     f"csv={csv_path.name}; html={html_path.name}", conn)
        push_metrics(sim_date, summary, counts, duration, "success")

        log.info("batch_run_succeeded", extra={
            "run_id": run_id, "sim_date": sim_date, "duration_seconds": round(duration, 2),
            "csv": str(csv_path), "html": str(html_path), **counts, **summary,
        })
        return {"status": "success", "sim_date": sim_date, "run_id": run_id,
                "summary": summary, "counts": counts,
                "csv": str(csv_path), "html": str(html_path)}

    except Exception as exc:
        finish_audit(run_id, sim_date, "failed", started, counts, str(exc), conn)
        push_metrics(sim_date, {}, counts, time.time() - started, "failed")
        log.exception("batch_run_failed", extra={"run_id": run_id, "sim_date": sim_date})
        raise
    finally:
        spark.stop()
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily fleet profitability reconciliation")
    parser.add_argument("--sim-date", required=True, help="simulated date to reconcile, YYYY-MM-DD")
    parser.add_argument("--run-id", default=None, help="defaults to a generated id")
    args = parser.parse_args(argv)

    run_id = args.run_id or f"{args.sim_date}-{uuid.uuid4().hex[:8]}"
    run(args.sim_date, run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
