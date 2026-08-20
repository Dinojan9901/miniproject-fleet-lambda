"""Streaming source: simulated GPS/telemetry events from the fleet, into Kafka.

Each vehicle runs a small state machine -- idle -> enroute -> on_trip -> idle --
and emits one event per tick with its position, speed, status and accrued fare.
Event timestamps come from the shared simulated clock, so one real second of
wall time produces ~288 simulated seconds of telemetry.

A configurable fraction of events is deliberately malformed (null position,
negative speed, unknown status, fare accruing while idle). Real telemetry from
cheap in-vehicle units is not clean, and the speed layer's validation and
quarantine path needs something to reject.

Usage:
    python -m simulators.telemetry_producer
    python -m simulators.telemetry_producer --anomaly-rate 0.2 --interval 0.5
"""
from __future__ import annotations

import argparse
import json
import math
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from common.config import SETTINGS
from common.fleet import Vehicle, build_fleet
from common.geo import LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, clamp_to_area
from common.logging_setup import configure
from common.simclock import SimClock

log = configure("telemetry-producer", "ingest", SETTINGS.log_level)

EVENTS = Counter("fleet_telemetry_events_total", "Telemetry events published", ["status"])
ANOMALIES = Counter("fleet_telemetry_anomalies_total", "Deliberately malformed events", ["kind"])
ERRORS = Counter("fleet_telemetry_produce_errors_total", "Kafka delivery failures")
LAST_EVENT = Gauge("fleet_telemetry_last_event_unixtime", "Wall-clock time of the last published event")
SIM_DAY = Gauge("fleet_sim_day_index", "Simulated day index currently being produced")
ACTIVE = Gauge("fleet_simulated_vehicles", "Vehicles in the simulated fleet")

# Fare model (LKR), roughly calibrated to a Colombo ride-hail tariff.
FARE_FLAGFALL = 120.0
FARE_PER_KM = 65.0
FARE_PER_MINUTE = 3.5

ANOMALY_KINDS = ["null_position", "negative_speed", "unknown_status", "missing_vehicle_id", "fare_while_idle"]

_running = True


def _stop(*_args) -> None:
    global _running
    _running = False


@dataclass
class VehicleState:
    """Runtime state of one simulated vehicle. Sim-time fields are in seconds."""
    vehicle: Vehicle
    lat: float
    lon: float
    heading_deg: float
    status: str = "idle"
    trip_id: str | None = None
    fare: float = 0.0
    fare_increment: float = 0.0
    speed_kmh: float = 0.0
    seq: int = 0
    state_ends_at: float = 0.0          # simulated unix seconds
    idle_since: float | None = None
    trip_speed_kmh: float = 20.0
    trips_completed: int = 0
    rng: random.Random = field(default_factory=random.Random)

    def plan_duration(self, mean_minutes: float, spread: float = 0.35) -> float:
        """Simulated seconds for the next state, jittered around a mean."""
        minutes = max(1.0, self.rng.gauss(mean_minutes, mean_minutes * spread))
        return minutes * 60.0


def initial_state(vehicle: Vehicle, rng: random.Random, sim_now: float) -> VehicleState:
    # Average trip speed is derived from the vehicle's own baseline so that a
    # simulated day of driving covers roughly baseline_km_per_day. That is what
    # keeps GPS-derived distance comparable to the distance the fuel partner
    # reports in the daily file, and makes the reconciliation variance meaningful.
    hours_on_duty = max(1.0, vehicle.duty_cycle * 24.0)
    trip_speed = vehicle.baseline_km_per_day / hours_on_duty

    state = VehicleState(
        vehicle=vehicle,
        lat=rng.uniform(LAT_MIN, LAT_MAX),
        lon=rng.uniform(LON_MIN, LON_MAX),
        heading_deg=rng.uniform(0, 360),
        trip_speed_kmh=trip_speed,
        rng=random.Random(rng.random()),
    )
    state.state_ends_at = sim_now + state.plan_duration(12.0)
    state.idle_since = sim_now
    return state


def advance(state: VehicleState, sim_now: float, sim_dt: float, idle_offender: bool) -> None:
    """Move the vehicle and run its state machine forward by `sim_dt` seconds."""
    v = state.vehicle

    # --- movement ---------------------------------------------------------
    speed = 0.0
    if state.status in ("enroute", "on_trip"):
        speed = max(0.0, state.rng.gauss(state.trip_speed_kmh, state.trip_speed_kmh * 0.3))
        km = speed * (sim_dt / 3600.0)
        # Wander: small heading changes produce a plausible road-like track.
        state.heading_deg = (state.heading_deg + state.rng.gauss(0, 25)) % 360
        rad = math.radians(state.heading_deg)
        dlat = (km * math.cos(rad)) / 111.32
        dlon = (km * math.sin(rad)) / (111.32 * math.cos(math.radians(state.lat)))
        lat, lon = state.lat + dlat, state.lon + dlon
        if not (LAT_MIN < lat < LAT_MAX and LON_MIN < lon < LON_MAX):
            state.heading_deg = (state.heading_deg + 180) % 360   # reflect at the boundary
        state.lat, state.lon = clamp_to_area(lat, lon)
    state.speed_kmh = speed

    # --- fare -------------------------------------------------------------
    increment = 0.0
    if state.status == "on_trip":
        increment = FARE_PER_KM * speed * (sim_dt / 3600.0) + FARE_PER_MINUTE * (sim_dt / 60.0)
        state.fare += increment
    state.fare_increment = round(increment, 2)

    # --- transitions ------------------------------------------------------
    if sim_now < state.state_ends_at:
        return

    if state.status == "idle":
        if idle_offender:
            # One vehicle is left parked far longer than the alert threshold, so
            # the idle alert rule has something real to fire on during a demo.
            state.state_ends_at = sim_now + state.plan_duration(180.0, 0.1)
            return
        state.status = "enroute"
        state.trip_id = f"TRIP-{v.vehicle_id.split('-')[1]}-{int(sim_now)}"
        state.fare = FARE_FLAGFALL
        state.idle_since = None
        state.state_ends_at = sim_now + state.plan_duration(5.0)
    elif state.status == "enroute":
        state.status = "on_trip"
        state.state_ends_at = sim_now + state.plan_duration(18.0)
    else:  # on_trip -> idle
        state.status = "idle"
        state.trips_completed += 1
        state.trip_id = None
        state.fare = 0.0
        # The fare accrued earlier in this tick belongs to a trip that has now
        # ended. Leaving it set would emit an idle event carrying a fare
        # increment -- which is exactly what the fare_while_idle rule rejects,
        # so the producer would be manufacturing its own quarantine traffic.
        state.fare_increment = 0.0
        state.idle_since = sim_now
        # Idle long enough to hit the vehicle's target duty cycle.
        busy = 23.0
        idle_mean = busy * (1 - v.duty_cycle) / max(v.duty_cycle, 0.05)
        state.state_ends_at = sim_now + state.plan_duration(idle_mean)


def build_event(state: VehicleState, sim_now_dt: datetime, sim_date: str) -> dict:
    state.seq += 1
    return {
        # Minted here and never rewritten: the correlation id that lets one
        # event be traced from this line to the Parquet lake.
        "event_id": uuid.uuid4().hex,
        "trip_id": state.trip_id,
        "driver_id": state.vehicle.driver_id,
        "vehicle_id": state.vehicle.vehicle_id,
        "lat": round(state.lat, 6),
        "lon": round(state.lon, 6),
        "speed_kmh": round(state.speed_kmh, 2),
        "status": state.status,
        "fare": round(state.fare, 2),
        "fare_increment": state.fare_increment,
        "event_time": sim_now_dt.isoformat(timespec="milliseconds"),
        "ingest_time": datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds"),
        "sim_date": sim_date,
        "seq": state.seq,
    }


def corrupt(event: dict, rng: random.Random) -> tuple[dict, str]:
    """Damage an event the way a flaky in-vehicle unit would."""
    kind = rng.choice(ANOMALY_KINDS)
    broken = dict(event)
    if kind == "null_position":
        broken["lat"] = None
        broken["lon"] = None
    elif kind == "negative_speed":
        broken["speed_kmh"] = -abs(broken["speed_kmh"] or 1.0) - 1.0
    elif kind == "unknown_status":
        broken["status"] = "unknown"
    elif kind == "missing_vehicle_id":
        broken["vehicle_id"] = None
    else:  # fare_while_idle
        broken["status"] = "idle"
        broken["fare_increment"] = round(abs(rng.uniform(10, 90)), 2)
    return broken, kind


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Simulated fleet telemetry -> Kafka")
    parser.add_argument("--interval", type=float, default=SETTINGS.event_interval_seconds,
                        help="real seconds between emission rounds")
    parser.add_argument("--fleet-size", type=int, default=SETTINGS.fleet_size)
    parser.add_argument("--anomaly-rate", type=float, default=SETTINGS.anomaly_rate)
    parser.add_argument("--idle-offenders", type=int, default=1,
                        help="vehicles that park for an abnormally long time, to trigger idle alerts")
    parser.add_argument("--max-sim-days", type=float, default=0.0, help="stop after N simulated days (0 = forever)")
    parser.add_argument("--metrics-port", type=int, default=SETTINGS.metrics_port_producer)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    start_http_server(args.metrics_port)
    clock = SimClock.shared(SETTINGS.data_dir, SETTINGS.sim_day_seconds, SETTINGS.sim_epoch_date)
    rng = random.Random(args.seed)

    fleet = build_fleet(args.fleet_size)
    ACTIVE.set(len(fleet))
    sim_start = clock.now().timestamp()
    states = {v.vehicle_id: initial_state(v, rng, sim_start) for v in fleet}
    offenders = {v.vehicle_id for v in fleet[: max(0, args.idle_offenders)]}

    producer = Producer({
        "bootstrap.servers": SETTINGS.bootstrap_servers,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
        "client.id": "telemetry-producer",
    })

    def on_delivery(err, msg) -> None:
        if err is not None:
            ERRORS.inc()
            log.error("kafka_delivery_failed", extra={"error": str(err)})

    log.info("producer_started", extra={
        "topic": SETTINGS.telemetry_topic,
        "fleet_size": len(fleet),
        "interval_seconds": args.interval,
        "anomaly_rate": args.anomaly_rate,
        "idle_offenders": sorted(offenders),
        **clock.describe(),
    })

    sim_dt = args.interval * clock.scale
    emitted = 0
    last_heartbeat = time.time()

    try:
        while _running:
            sim_now_dt = clock.now()
            sim_now = sim_now_dt.timestamp()
            sim_date = sim_now_dt.date().isoformat()
            SIM_DAY.set(clock.day_index())

            if args.max_sim_days and (sim_now - sim_start) >= args.max_sim_days * 86400:
                log.info("max_sim_days_reached", extra={"max_sim_days": args.max_sim_days})
                break

            for vehicle_id, state in states.items():
                advance(state, sim_now, sim_dt, vehicle_id in offenders)
                event = build_event(state, sim_now_dt, sim_date)

                if rng.random() < args.anomaly_rate:
                    event, kind = corrupt(event, rng)
                    ANOMALIES.labels(kind=kind).inc()
                    log.warning("anomalous_event_emitted", extra={"kind": kind, "vehicle_id": vehicle_id})

                producer.produce(
                    topic=SETTINGS.telemetry_topic,
                    # Keyed by vehicle so one vehicle's events stay ordered
                    # within a partition -- the speed layer's "latest state per
                    # vehicle" logic depends on that ordering.
                    key=(vehicle_id or "unknown").encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    on_delivery=on_delivery,
                )
                EVENTS.labels(status=event.get("status") or "unknown").inc()
                emitted += 1

            LAST_EVENT.set(time.time())
            producer.poll(0)

            if time.time() - last_heartbeat >= 10:
                snapshot = {
                    "emitted": emitted,
                    "on_trip": sum(1 for s in states.values() if s.status == "on_trip"),
                    "enroute": sum(1 for s in states.values() if s.status == "enroute"),
                    "idle": sum(1 for s in states.values() if s.status == "idle"),
                    "trips_completed": sum(s.trips_completed for s in states.values()),
                    **clock.describe(),
                }
                log.info("producer_heartbeat", extra=snapshot)
                last_heartbeat = time.time()

            time.sleep(args.interval)
    finally:
        outstanding = producer.flush(15)
        log.info("producer_stopped", extra={"emitted": emitted, "unflushed": outstanding})

    return 0


if __name__ == "__main__":
    sys.exit(main())
