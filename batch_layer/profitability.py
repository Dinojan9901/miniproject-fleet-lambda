"""The economics of the daily reconciliation, and the thresholds that classify it.

The Spark job expresses these same formulas as column expressions (idiomatic
Spark, no per-row Python), while the report renderer and the unit tests call the
functions here. The *thresholds* are imported by both, so a policy change --
"unprofitable now means margin below 15%" -- happens in exactly one place and
cannot drift between the pipeline and the report.
"""
from __future__ import annotations

# A vehicle is flagged when its margin falls below this fraction of revenue.
LOW_MARGIN_FRACTION = 0.10

# Distance disagreement between the GPS track and the partner's invoice that is
# large enough to be a billing dispute rather than GPS noise.
DISTANCE_DISPUTE_PCT = 20.0

# Below this, a GPS distance is too small to compute a meaningful variance on.
MIN_DISTANCE_FOR_VARIANCE_KM = 1.0


def total_cost(fuel_cost: float | None, maintenance_cost: float | None) -> float:
    return (fuel_cost or 0.0) + (maintenance_cost or 0.0)


def profit(revenue: float, cost: float) -> float:
    return revenue - cost


def margin_pct(revenue: float, profit_value: float) -> float | None:
    """Profit as a percentage of revenue. Undefined when there was no revenue."""
    if revenue <= 0:
        return None
    return profit_value / revenue * 100.0


def revenue_per_km(revenue: float, distance_km: float) -> float | None:
    if distance_km <= 0:
        return None
    return revenue / distance_km


def distance_variance_pct(gps_km: float, reported_km: float | None) -> float | None:
    """How far the partner's claimed distance sits from the GPS-derived one.

    Positive means the partner claims more distance than the vehicle actually
    covered -- the direction that costs the operator money.
    """
    if reported_km is None or gps_km < MIN_DISTANCE_FOR_VARIANCE_KM:
        return None
    return (reported_km - gps_km) / gps_km * 100.0


def is_disputed(variance_pct: float | None) -> bool:
    return variance_pct is not None and abs(variance_pct) > DISTANCE_DISPUTE_PCT


def is_unprofitable(revenue: float, profit_value: float) -> bool:
    """Losing money outright, or earning too thin a margin to be worth the asset."""
    if profit_value <= 0:
        return True
    margin = margin_pct(revenue, profit_value)
    return margin is not None and margin < LOW_MARGIN_FRACTION * 100.0


def summarise(rows: list[dict]) -> dict:
    """Fleet-level roll-up of per-vehicle rows, for the summary table and API."""
    revenue = sum(r["revenue"] for r in rows)
    cost = sum(r["total_cost"] or 0.0 for r in rows)
    fleet_profit = profit(revenue, cost)
    return {
        "vehicles": len(rows),
        "trips": sum(r["trips"] for r in rows),
        "revenue": round(revenue, 2),
        "total_cost": round(cost, 2),
        "profit": round(fleet_profit, 2),
        "margin_pct": round(margin_pct(revenue, fleet_profit), 2) if revenue > 0 else None,
        "unprofitable_vehicles": sum(1 for r in rows if r["unprofitable"]),
        "disputed_vehicles": sum(1 for r in rows if r["distance_disputed"]),
        "missing_expense_rows": sum(1 for r in rows if not r["expense_data_present"]),
    }
