"""Telemetry validation rules -- the gate between the stream and the master dataset.

These run the exact function the Spark UDF wraps, so a rule that passes here is
the rule the pipeline enforces.
"""
import pytest

from common.validation import MAX_PLAUSIBLE_SPEED_KMH, is_clean, telemetry_violations


def good_event(**overrides) -> dict:
    event = {
        "trip_id": "TRIP-001-1",
        "driver_id": "DRV-001",
        "vehicle_id": "FLEET-001",
        "lat": 6.92,
        "lon": 79.86,
        "speed_kmh": 32.5,
        "status": "on_trip",
        "fare": 480.0,
        "fare_increment": 12.5,
        "event_time": "2026-08-01T09:15:00.000+00:00",
        "ingest_time": "2026-08-01T09:15:00.100+00:00",
        "sim_date": "2026-08-01",
        "seq": 42,
    }
    event.update(overrides)
    return event


def test_a_well_formed_event_passes():
    assert telemetry_violations(good_event()) == []
    assert is_clean(good_event())


def test_idle_event_with_no_trip_is_clean():
    """Idle vehicles have no trip_id and no fare movement -- that is normal."""
    assert is_clean(good_event(status="idle", trip_id=None, fare=0.0, fare_increment=0.0))


@pytest.mark.parametrize("overrides, expected", [
    ({"vehicle_id": None}, "missing_vehicle_id"),
    ({"vehicle_id": ""}, "missing_vehicle_id"),
    ({"event_time": None}, "missing_event_time"),
    ({"lat": None}, "missing_position"),
    ({"lon": None}, "missing_position"),
    ({"lat": 91.0}, "impossible_position"),
    ({"lon": -181.0}, "impossible_position"),
    ({"speed_kmh": None}, "missing_speed"),
    ({"speed_kmh": -3.0}, "negative_speed"),
    ({"speed_kmh": MAX_PLAUSIBLE_SPEED_KMH + 1}, "implausible_speed"),
    ({"status": "unknown"}, "unknown_status"),
    ({"status": None}, "unknown_status"),
    ({"fare": None}, "missing_fare"),
    ({"fare": -1.0}, "negative_fare"),
    ({"fare": 1e9}, "implausible_fare"),
    ({"fare_increment": -5.0}, "negative_fare_increment"),
])
def test_each_rule_fires_on_its_own_defect(overrides, expected):
    assert expected in telemetry_violations(good_event(**overrides))


def test_fare_cannot_accrue_while_idle():
    """The cross-field rule: a parked vehicle billing a passenger is impossible."""
    violations = telemetry_violations(good_event(status="idle", fare_increment=45.0))
    assert "fare_while_idle" in violations


def test_a_valid_speed_at_the_boundary_is_accepted():
    assert is_clean(good_event(speed_kmh=MAX_PLAUSIBLE_SPEED_KMH))
    assert is_clean(good_event(speed_kmh=0.0, status="idle", fare_increment=0.0))


def test_multiple_defects_are_all_reported():
    """The quarantine record names every rule broken, not just the first."""
    violations = telemetry_violations(good_event(vehicle_id=None, lat=None, speed_kmh=-1.0))
    assert {"missing_vehicle_id", "missing_position", "negative_speed"} <= set(violations)


def test_an_empty_record_does_not_raise():
    """Unparseable JSON reaches the UDF as an all-null row; it must be rejected,
    not crash the micro-batch."""
    violations = telemetry_violations({})
    assert "missing_vehicle_id" in violations
    assert len(violations) >= 4
