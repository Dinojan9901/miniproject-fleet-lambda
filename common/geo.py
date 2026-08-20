"""Geography: the operating area, its zones, and distance between fixes.

The operating area is a bounding box over Colombo, divided into a 3x2 grid of
operating zones. Zone assignment is the enrichment step in the speed layer: raw
telemetry carries only lat/lon, and the business question is asked *by zone*.

This module is the single source of truth for that mapping -- the Spark job
wraps `zone_for` in a UDF rather than reimplementing the arithmetic in SQL, so
the unit tests and the pipeline can never drift apart. (Trade-off noted in the
report: a Python UDF costs a serialisation hop per row, and at real volume this
would become a broadcast join against a zone lookup table or a native
expression.)
"""
from __future__ import annotations

import math

# Bounding box of the operating area.
LAT_MIN, LAT_MAX = 6.85, 6.99
LON_MIN, LON_MAX = 79.83, 79.93

ZONE_ROWS = 2
ZONE_COLS = 3

# [row][col], row 0 = southern band.
ZONE_NAMES = [
    ["Z1-Dehiwala", "Z2-Nugegoda", "Z3-Battaramulla"],
    ["Z4-Fort", "Z5-Borella", "Z6-Rajagiriya"],
]

OUT_OF_AREA = "OUT-OF-AREA"

EARTH_RADIUS_KM = 6371.0088


def zone_for(lat: float | None, lon: float | None) -> str:
    """Map a GPS fix to an operating zone, or OUT-OF-AREA.

    Returns OUT-OF-AREA rather than raising: a fix outside the box is a
    legitimate business observation (a driver left the city), not a data error.
    Fixes that are *impossible* (null, out of global range) are rejected earlier,
    by validation.
    """
    if lat is None or lon is None:
        return OUT_OF_AREA
    if not (LAT_MIN <= lat < LAT_MAX and LON_MIN <= lon < LON_MAX):
        return OUT_OF_AREA

    row = int((lat - LAT_MIN) / (LAT_MAX - LAT_MIN) * ZONE_ROWS)
    col = int((lon - LON_MIN) / (LON_MAX - LON_MIN) * ZONE_COLS)
    row = min(row, ZONE_ROWS - 1)
    col = min(col, ZONE_COLS - 1)
    return ZONE_NAMES[row][col]


def all_zones() -> list[str]:
    return [name for row in ZONE_NAMES for name in row]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two fixes, in kilometres.

    Used by the batch layer to derive each vehicle's distance travelled from
    consecutive GPS fixes, which is then reconciled against the distance the
    fuel partner claims in the daily expense file.
    """
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def clamp_to_area(lat: float, lon: float) -> tuple[float, float]:
    """Keep a simulated vehicle inside the operating area."""
    return (
        min(max(lat, LAT_MIN + 1e-4), LAT_MAX - 1e-4),
        min(max(lon, LON_MIN + 1e-4), LON_MAX - 1e-4),
    )
