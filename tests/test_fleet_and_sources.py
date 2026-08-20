"""The shared roster, and the two simulated sources that depend on it agreeing."""
import random

import pytest

from common.fleet import build_fleet, fleet_index
from common.schemas import EXPENSE_COLUMNS, TELEMETRY_FIELDS
from simulators.expense_batch_source import build_rows, write_file
from simulators.telemetry_producer import advance, build_event, corrupt, initial_state
from common.simclock import SimClock
from common.validation import is_clean, telemetry_violations


def test_roster_is_deterministic():
    """Both sources build the roster independently; they must get the same fleet."""
    assert build_fleet(12) == build_fleet(12)
    assert [v.vehicle_id for v in build_fleet(3)] == ["FLEET-001", "FLEET-002", "FLEET-003"]


def test_roster_grows_without_renaming_existing_vehicles():
    small = build_fleet(5)
    large = build_fleet(20)
    assert large[:5] == small


def test_fleet_index_is_keyed_by_id():
    index = fleet_index(4)
    assert set(index) == {"FLEET-001", "FLEET-002", "FLEET-003", "FLEET-004"}
    assert index["FLEET-002"].driver_id == "DRV-002"


# --- telemetry simulator --------------------------------------------------

def test_generated_events_match_the_declared_schema_and_pass_validation():
    clock = SimClock.fixed(real_anchor=0.0)
    rng = random.Random(1)
    state = initial_state(build_fleet(1)[0], rng, clock.now(0.0).timestamp())

    for tick in range(60):
        sim_now = clock.now(tick * 2.0).timestamp()
        advance(state, sim_now, 576.0, idle_offender=False)
        event = build_event(state, clock.now(tick * 2.0), "2026-08-01")

        assert set(event) == set(TELEMETRY_FIELDS)
        assert telemetry_violations(event) == [], event


def test_the_state_machine_visits_every_status():
    clock = SimClock.fixed(real_anchor=0.0)
    rng = random.Random(3)
    state = initial_state(build_fleet(1)[0], rng, 0.0)

    seen = set()
    for tick in range(400):
        advance(state, tick * 576.0, 576.0, idle_offender=False)
        seen.add(state.status)
    assert seen == {"idle", "enroute", "on_trip"}


def test_an_idle_offender_stays_parked():
    """The idle-alert demo depends on one vehicle never picking up a fare."""
    rng = random.Random(5)
    state = initial_state(build_fleet(1)[0], rng, 0.0)
    for tick in range(200):
        advance(state, tick * 576.0, 576.0, idle_offender=True)
    assert state.status == "idle"
    assert state.trips_completed == 0


def test_fare_only_accrues_on_a_trip():
    rng = random.Random(9)
    state = initial_state(build_fleet(1)[0], rng, 0.0)
    for tick in range(300):
        advance(state, tick * 576.0, 576.0, idle_offender=False)
        if state.status != "on_trip":
            assert state.fare_increment == 0.0


def test_corrupted_events_are_always_rejected():
    """Every injected anomaly must actually be caught -- otherwise the quarantine
    path would look healthy while dirty data flowed into the master dataset."""
    clock = SimClock.fixed(real_anchor=0.0)
    rng = random.Random(2)
    state = initial_state(build_fleet(1)[0], rng, 0.0)

    kinds = set()
    for tick in range(200):
        advance(state, tick * 576.0, 576.0, idle_offender=False)
        event = build_event(state, clock.now(tick * 2.0), "2026-08-01")
        broken, kind = corrupt(event, rng)
        kinds.add(kind)
        assert not is_clean(broken), (kind, broken)

    assert len(kinds) == 5   # every anomaly kind exercised


# --- expense simulator ----------------------------------------------------

def test_expense_rows_have_the_declared_columns():
    rows = build_rows("2026-08-01", fleet_size=12, rng=random.Random(4))
    assert rows
    for row in rows:
        assert set(row) == set(EXPENSE_COLUMNS)
        assert row["fuel_cost"] > 0
        assert row["maintenance_cost"] > 0
        assert row["distance_covered"] > 0
        assert row["service_flag"] in {"Y", "N"}


def test_expense_rows_reference_the_shared_roster():
    rows = build_rows("2026-08-01", fleet_size=12, rng=random.Random(6))
    roster = set(fleet_index(12))
    assert {r["vehicle_id"] for r in rows} <= roster


def test_some_vehicles_are_missing_by_design():
    """The partner file is incomplete on purpose; the batch layer must cope."""
    missing_days = 0
    for seed in range(40):
        rows = build_rows("2026-08-01", fleet_size=12, rng=random.Random(seed))
        if len(rows) < 12:
            missing_days += 1
    assert missing_days > 0


def test_file_is_published_atomically(tmp_path):
    """No .tmp file may survive, or Airflow's sensor could read a partial file."""
    rows = build_rows("2026-08-01", fleet_size=6, rng=random.Random(8))
    path = write_file(tmp_path, "2026-08-01", rows)

    assert path.name == "expenses_2026-08-01.csv"
    assert list(tmp_path.glob("*.tmp")) == []
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert lines[0].split(",") == EXPENSE_COLUMNS
    assert len(lines) == len(rows) + 1
