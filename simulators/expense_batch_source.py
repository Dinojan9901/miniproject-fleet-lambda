"""Daily-batch source: the fuel partner / garage expense drop.

Once per *simulated* day (5 real minutes at the default compression) this writes
one CSV of per-vehicle running costs into the landing directory, exactly as an
external partner SFTP-ing an end-of-day extract would.

Two properties matter for the pipeline downstream:

1. **Atomic publication.** The file is written to `.tmp` and then `os.replace`d
   into place. Airflow's sensor therefore never sees a half-written file --
   rename within a filesystem is atomic, a streaming write is not.

2. **Realistic imperfection.** Some vehicles are missing from the file (the
   partner did not report them) and some report a distance that disagrees
   sharply with the GPS track. The batch layer has to survive both, and the
   reconciliation report exists precisely to surface the second.

Costs are derived from the same shared fleet roster the telemetry simulator
uses, so the two feeds describe the same vehicles -- see common/fleet.py.

Usage:
    python -m simulators.expense_batch_source
    python -m simulators.expense_batch_source --emit-now      # don't wait for the boundary
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import signal
import sys
import time
from datetime import timedelta
from pathlib import Path

from prometheus_client import Counter, Gauge, start_http_server

from common.config import SETTINGS
from common.fleet import build_fleet
from common.logging_setup import configure
from common.schemas import EXPENSE_COLUMNS
from common.simclock import SimClock

log = configure("expense-batch-source", "ingest", SETTINGS.log_level)

FILES = Counter("fleet_expense_files_written_total", "Daily expense files published")
ROWS = Counter("fleet_expense_rows_written_total", "Expense rows published")
ANOMALIES = Counter("fleet_expense_anomalies_total", "Injected data problems", ["kind"])
LAST_FILE = Gauge("fleet_expense_last_file_unixtime", "Wall-clock time of the last published file")
LAST_FILE_SIM_DAY = Gauge("fleet_expense_last_file_sim_day", "Simulated day index of the last file")

FUEL_PRICE_PER_LITRE = 366.0     # LKR, order-of-magnitude realistic
SERVICE_PROBABILITY = 0.15
MISSING_ROW_PROBABILITY = 0.05
DISTANCE_DISPUTE_PROBABILITY = 0.10

_running = True


def _stop(*_args) -> None:
    global _running
    _running = False


def build_rows(sim_date: str, fleet_size: int, rng: random.Random) -> list[dict]:
    """One row per vehicle -- minus the ones the partner failed to report."""
    rows: list[dict] = []
    for vehicle in build_fleet(fleet_size):
        if rng.random() < MISSING_ROW_PROBABILITY:
            ANOMALIES.labels(kind="missing_vehicle_row").inc()
            log.warning("expense_row_omitted", extra={"vehicle_id": vehicle.vehicle_id, "sim_date": sim_date})
            continue

        # Distance the partner claims: normally close to the vehicle's baseline
        # duty, occasionally wildly off (a disputed odometer reading).
        distance = rng.gauss(vehicle.baseline_km_per_day, vehicle.baseline_km_per_day * 0.08)
        distance = max(5.0, distance)
        if rng.random() < DISTANCE_DISPUTE_PROBABILITY:
            factor = rng.choice([0.45, 1.6, 1.85])
            distance *= factor
            ANOMALIES.labels(kind="distance_dispute").inc()
            log.warning("expense_distance_disputed", extra={
                "vehicle_id": vehicle.vehicle_id, "sim_date": sim_date, "factor": factor,
            })

        litres = distance / vehicle.fuel_efficiency_km_per_l
        fuel_cost = litres * FUEL_PRICE_PER_LITRE * rng.uniform(0.96, 1.06)

        serviced = rng.random() < SERVICE_PROBABILITY
        maintenance_cost = rng.uniform(3500, 22000) if serviced else rng.uniform(150, 900)

        rows.append({
            "vehicle_id": vehicle.vehicle_id,
            "fuel_cost": round(fuel_cost, 2),
            "maintenance_cost": round(maintenance_cost, 2),
            "distance_covered": round(distance, 2),
            "service_flag": "Y" if serviced else "N",
            "sim_date": sim_date,
        })
    return rows


def write_file(landing_dir: Path, sim_date: str, rows: list[dict]) -> Path:
    """Publish atomically: write a temp file, then rename it into place."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    final_path = landing_dir / f"expenses_{sim_date}.csv"
    tmp_path = landing_dir / f".expenses_{sim_date}.csv.tmp"

    with tmp_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=EXPENSE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, final_path)   # atomic within the same filesystem
    return final_path


def publish(landing_dir: Path, sim_date: str, fleet_size: int, rng: random.Random, day_index: int) -> Path:
    rows = build_rows(sim_date, fleet_size, rng)
    path = write_file(landing_dir, sim_date, rows)

    FILES.inc()
    ROWS.inc(len(rows))
    LAST_FILE.set(time.time())
    LAST_FILE_SIM_DAY.set(day_index)

    log.info("expense_file_published", extra={
        "path": str(path),
        "sim_date": sim_date,
        "rows": len(rows),
        "total_fuel_cost": round(sum(r["fuel_cost"] for r in rows), 2),
        "total_maintenance_cost": round(sum(r["maintenance_cost"] for r in rows), 2),
        "serviced_vehicles": sum(1 for r in rows if r["service_flag"] == "Y"),
    })
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simulated daily vehicle-expense file drop")
    parser.add_argument("--fleet-size", type=int, default=SETTINGS.fleet_size)
    parser.add_argument("--emit-now", action="store_true",
                        help="publish a file for the current simulated day immediately, then resume the schedule")
    parser.add_argument("--max-files", type=int, default=0, help="stop after N files (0 = forever)")
    parser.add_argument("--metrics-port", type=int, default=SETTINGS.metrics_port_batch_source)
    parser.add_argument("--seed", type=int, default=11)
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    start_http_server(args.metrics_port)
    clock = SimClock.shared(SETTINGS.data_dir, SETTINGS.sim_day_seconds, SETTINGS.sim_epoch_date)
    landing_dir = Path(SETTINGS.data_dir) / "landing" / "expenses"
    rng = random.Random(args.seed)

    log.info("batch_source_started", extra={
        "landing_dir": str(landing_dir),
        "fleet_size": args.fleet_size,
        "seconds_per_sim_day": SETTINGS.sim_day_seconds,
        **clock.describe(),
    })

    published = 0
    if args.emit_now:
        publish(landing_dir, clock.sim_date().isoformat(), args.fleet_size, rng, clock.day_index())
        published += 1

    while _running:
        # Sleep in short slices so Ctrl-C / SIGTERM is honoured promptly.
        wait = clock.real_seconds_until_next_day()
        log.info("waiting_for_sim_day_boundary", extra={
            "real_seconds_remaining": round(wait, 1), **clock.describe(),
        })
        deadline = time.time() + wait
        while _running and time.time() < deadline:
            time.sleep(min(1.0, deadline - time.time()))
        if not _running:
            break

        # The day that just ended is the one being invoiced.
        completed_day = clock.day_index() - 1
        sim_date = (clock.now() - timedelta(days=1)).date().isoformat()
        publish(landing_dir, sim_date, args.fleet_size, rng, completed_day)
        published += 1

        if args.max_files and published >= args.max_files:
            log.info("max_files_reached", extra={"files": published})
            break

    log.info("batch_source_stopped", extra={"files_published": published})
    return 0


if __name__ == "__main__":
    sys.exit(main())
