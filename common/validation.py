"""Data-quality rules for incoming telemetry.

One function, used in exactly one place in the pipeline (wrapped as a Spark UDF
in the speed layer) and tested directly in pure Python. Keeping the rules here
rather than as SQL inside the streaming job means the tests exercise the same
code the pipeline runs, and the quarantine record can name the precise rule that
rejected a row.

The rules separate *impossible* from merely *unusual*:
  - impossible  -> quarantine (a null vehicle_id cannot be attributed to anyone)
  - unusual     -> keep, and let the business layer judge (a fix outside the
                   operating area becomes zone OUT-OF-AREA, not a rejection)
"""
from __future__ import annotations

from .schemas import VALID_STATUSES

MAX_PLAUSIBLE_SPEED_KMH = 200.0
MAX_PLAUSIBLE_FARE = 100_000.0


def telemetry_violations(record: dict) -> list[str]:
    """Return the names of every rule the record breaks. Empty list = clean."""
    problems: list[str] = []

    if not record.get("vehicle_id"):
        problems.append("missing_vehicle_id")
    if not record.get("event_time"):
        problems.append("missing_event_time")

    lat, lon = record.get("lat"), record.get("lon")
    if lat is None or lon is None:
        problems.append("missing_position")
    else:
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            problems.append("impossible_position")

    speed = record.get("speed_kmh")
    if speed is None:
        problems.append("missing_speed")
    elif speed < 0:
        problems.append("negative_speed")
    elif speed > MAX_PLAUSIBLE_SPEED_KMH:
        problems.append("implausible_speed")

    status = record.get("status")
    if status not in VALID_STATUSES:
        problems.append("unknown_status")

    fare = record.get("fare")
    if fare is None:
        problems.append("missing_fare")
    elif fare < 0:
        problems.append("negative_fare")
    elif fare > MAX_PLAUSIBLE_FARE:
        problems.append("implausible_fare")

    increment = record.get("fare_increment")
    if increment is not None and increment < 0:
        problems.append("negative_fare_increment")

    # A fare cannot accrue while the vehicle is not carrying a passenger.
    if status == "idle" and (increment or 0) > 0:
        problems.append("fare_while_idle")

    return problems


def is_clean(record: dict) -> bool:
    return not telemetry_violations(record)
