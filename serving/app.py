"""Lambda SERVING LAYER -- one read API over both views, plus the live dashboard.

The defining job of a serving layer in Lambda is not "expose the database". It
is to present the speed view and the batch view together while keeping them
distinguishable, so a reader always knows whether a number is *fresh* or
*correct*. Every response here that mixes the two labels which layer produced
what, and /api/lambda/compare/{sim_date} deliberately puts the two answers side
by side and reports the gap.

    /                                live dashboard (self-contained HTML)
    /health                          liveness + database reachability
    /metrics                         Prometheus exposition
    /api/fleet/live                  speed view: fleet state right now
    /api/fleet/zones                 speed view: recent windowed zone metrics
    /api/fleet/vehicles              speed view: per-vehicle current state
    /api/fleet/vehicles/{id}         MERGED: live state + last batch verdict
    /api/alerts                      speed view: threshold alerts
    /api/reports                     batch view: which days are reconciled
    /api/reports/daily/{sim_date}    batch view: authoritative day
    /api/reports/daily/{d}/html      batch view: the rendered report file
    /api/lambda/compare/{sim_date}   speed vs batch, and the difference
    /api/pipeline/status             recent batch runs + freshness of each layer
"""
from __future__ import annotations

import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from common.config import SETTINGS
from common.logging_setup import configure

from .db import close_pool, fetch, fetch_one

log = configure("serving-api", "serve", SETTINGS.log_level)

REQUESTS = Counter("api_requests_total", "API requests", ["method", "endpoint", "status"])
LATENCY = Histogram("api_request_duration_seconds", "API request duration", ["endpoint"])
DB_ERRORS = Counter("api_db_errors_total", "Database errors while serving")
SPEED_FRESHNESS = Gauge("api_speed_view_age_seconds", "Age of the newest speed-layer write")
BATCH_FRESHNESS = Gauge("api_batch_view_age_seconds", "Age of the newest successful batch run")

DASHBOARD = Path(__file__).parent / "templates" / "dashboard.html"
REPORTS_DIR = Path(SETTINGS.data_dir) / "reports"

app = FastAPI(
    title="Fleet Operations API",
    description="Lambda serving layer for the ride-hailing fleet pipeline",
    version="1.0.0",
)


@app.middleware("http")
async def observe(request: Request, call_next):
    """Every request is counted, timed and logged with a correlation-friendly shape."""
    started = time.time()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    except Exception:
        log.exception("request_failed", extra={"path": request.url.path})
        raise
    finally:
        duration = time.time() - started
        # Read the matched route *after* the request has been handled: the router
        # populates scope["route"] downstream of this middleware. Labelling with
        # the route template ("/api/reports/daily/{sim_date}") rather than the
        # concrete path keeps the metric's cardinality bounded -- labelling by
        # raw path would mint a new Prometheus time series per simulated date.
        route = request.scope.get("route")
        endpoint = getattr(route, "path", None) or request.url.path
        REQUESTS.labels(request.method, endpoint, str(status)).inc()
        LATENCY.labels(endpoint).observe(duration)
        if duration > 1.0:
            log.warning("slow_request", extra={"path": request.url.path,
                                               "endpoint": endpoint,
                                               "duration_seconds": round(duration, 3)})


@app.on_event("shutdown")
def _shutdown() -> None:
    close_pool()


# --- operational endpoints ------------------------------------------------

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard() -> HTMLResponse:
    return HTMLResponse(DASHBOARD.read_text(encoding="utf-8"))


@app.get("/health")
def health() -> dict:
    """Liveness plus a real dependency check -- an API that cannot read is not healthy."""
    try:
        fetch_one("SELECT 1 AS ok")
        return {"status": "healthy", "database": "reachable"}
    except Exception as exc:  # noqa: BLE001
        DB_ERRORS.inc()
        log.error("health_check_failed", extra={"error": str(exc)})
        raise HTTPException(status_code=503, detail=f"database unreachable: {exc}")


@app.get("/metrics", include_in_schema=False)
def metrics() -> Response:
    _refresh_freshness_gauges()
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _refresh_freshness_gauges() -> None:
    """Turn "when did each layer last write?" into a scrapeable number.

    This is the metric the staleness alerts fire on: a pipeline that has quietly
    stopped looks identical to a healthy one from the outside unless something
    watches the age of its output.
    """
    try:
        speed = fetch_one("SELECT EXTRACT(EPOCH FROM (now() - MAX(updated_at))) AS age FROM rt_vehicle_state")
        if speed and speed["age"] is not None:
            SPEED_FRESHNESS.set(float(speed["age"]))
        batch = fetch_one(
            "SELECT EXTRACT(EPOCH FROM (now() - MAX(finished_at))) AS age "
            "FROM batch_run_audit WHERE status = 'success'"
        )
        if batch and batch["age"] is not None:
            BATCH_FRESHNESS.set(float(batch["age"]))
    except Exception as exc:  # noqa: BLE001 - metrics must not break the endpoint
        DB_ERRORS.inc()
        log.warning("freshness_gauges_failed", extra={"error": str(exc)})


# --- speed view -----------------------------------------------------------

@app.get("/api/fleet/live")
def fleet_live() -> dict:
    """Fleet state as of the last micro-batch: seconds old, approximate."""
    totals = fetch_one("""
        SELECT COUNT(*)                                             AS vehicles,
               COUNT(*) FILTER (WHERE status = 'on_trip')           AS on_trip,
               COUNT(*) FILTER (WHERE status = 'enroute')           AS enroute,
               COUNT(*) FILTER (WHERE status = 'idle')              AS idle,
               ROUND(AVG(speed_kmh)::numeric, 2)                    AS avg_speed_kmh,
               MAX(last_event_time)                                 AS latest_event_time,
               EXTRACT(EPOCH FROM (now() - MAX(updated_at)))        AS view_age_seconds
        FROM rt_vehicle_state
    """) or {}

    window = fetch("""
        SELECT zone, events, active_vehicles, idle_ratio, trips, earnings, avg_speed_kmh,
               window_start, window_end
        FROM rt_zone_metrics
        WHERE window_start = (SELECT MAX(window_start) FROM rt_zone_metrics)
        ORDER BY zone
    """)

    open_alerts = fetch_one(
        "SELECT COUNT(*) AS open_alerts FROM rt_alerts WHERE acknowledged = FALSE"
    ) or {}

    vehicles = totals.get("vehicles") or 0
    active = (totals.get("on_trip") or 0) + (totals.get("enroute") or 0)
    return {
        "view": "speed",
        "accuracy": "approximate",
        "fleet": {
            **totals,
            "utilisation_pct": round(active / vehicles * 100, 1) if vehicles else None,
        },
        "latest_window": {
            "window_start": window[0]["window_start"] if window else None,
            "window_end": window[0]["window_end"] if window else None,
            "earnings": round(sum(z["earnings"] or 0 for z in window), 2) if window else 0.0,
            "trips": sum(z["trips"] or 0 for z in window) if window else 0,
            "zones": window,
        },
        "open_alerts": open_alerts.get("open_alerts", 0),
    }


@app.get("/api/fleet/zones")
def fleet_zones(windows: int = 6) -> dict:
    rows = fetch("""
        SELECT window_start, window_end, zone, events, active_vehicles, total_vehicles,
               idle_events, ROUND(idle_ratio::numeric, 3) AS idle_ratio, trips,
               earnings, avg_speed_kmh
        FROM rt_zone_metrics
        WHERE window_start >= (
            SELECT COALESCE(MIN(window_start), now())
            FROM (SELECT DISTINCT window_start FROM rt_zone_metrics
                  ORDER BY window_start DESC LIMIT %s) recent
        )
        ORDER BY window_start DESC, zone
    """, (max(1, min(windows, 48)),))
    return {"view": "speed", "accuracy": "approximate", "windows": windows, "rows": rows}


@app.get("/api/fleet/vehicles")
def fleet_vehicles() -> dict:
    rows = fetch("""
        SELECT vehicle_id, driver_id, trip_id, status, zone, lat, lon, speed_kmh, fare,
               last_event_time, idle_since,
               CASE WHEN idle_since IS NOT NULL
                    THEN ROUND((EXTRACT(EPOCH FROM (last_event_time - idle_since)) / 60)::numeric, 1)
               END AS idle_minutes
        FROM rt_vehicle_state
        ORDER BY vehicle_id
    """)
    return {"view": "speed", "accuracy": "approximate", "vehicles": rows}


@app.get("/api/alerts")
def alerts(limit: int = 25, only_open: bool = False) -> dict:
    rows = fetch(f"""
        SELECT alert_id, alert_type, vehicle_id, severity, message, zone,
               idle_since, sim_detected_at, detected_at, acknowledged
        FROM rt_alerts
        {"WHERE acknowledged = FALSE" if only_open else ""}
        ORDER BY detected_at DESC
        LIMIT %s
    """, (max(1, min(limit, 200)),))
    return {"view": "speed", "alerts": rows}


# --- batch view -----------------------------------------------------------

@app.get("/api/reports")
def reports() -> dict:
    rows = fetch("""
        SELECT sim_date, vehicles, trips, revenue, total_cost, profit, margin_pct,
               unprofitable_vehicles, disputed_vehicles, missing_expense_rows,
               run_id, computed_at
        FROM daily_fleet_summary
        ORDER BY sim_date DESC
    """)
    return {"view": "batch", "accuracy": "authoritative", "days": rows}


@app.get("/api/reports/daily/{sim_date}")
def daily_report(sim_date: str) -> dict:
    summary = fetch_one("""
        SELECT sim_date, vehicles, trips, revenue, total_cost, profit, margin_pct,
               unprofitable_vehicles, disputed_vehicles, missing_expense_rows,
               run_id, computed_at
        FROM daily_fleet_summary WHERE sim_date = %s
    """, (sim_date,))
    if summary is None:
        raise HTTPException(status_code=404,
                            detail=f"no batch reconciliation for {sim_date}; it may not have run yet")

    vehicles = fetch("""
        SELECT vehicle_id, driver_id, trips, revenue, gps_distance_km, reported_distance_km,
               distance_variance_pct, fuel_cost, maintenance_cost, total_cost, profit,
               margin_pct, revenue_per_km, utilisation_pct, service_flag,
               expense_data_present, distance_disputed, unprofitable
        FROM daily_vehicle_profitability
        WHERE sim_date = %s
        ORDER BY profit ASC
    """, (sim_date,))
    return {"view": "batch", "accuracy": "authoritative", "summary": summary, "vehicles": vehicles}


@app.get("/api/reports/daily/{sim_date}/html", include_in_schema=False)
def daily_report_html(sim_date: str) -> FileResponse:
    path = REPORTS_DIR / f"daily_profitability_{sim_date}.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"no rendered report for {sim_date}")
    return FileResponse(path, media_type="text/html")


# --- the merge ------------------------------------------------------------

@app.get("/api/fleet/vehicles/{vehicle_id}")
def vehicle_detail(vehicle_id: str) -> dict:
    """Where the two layers meet: live state next to the last authoritative day."""
    live = fetch_one("""
        SELECT vehicle_id, driver_id, trip_id, status, zone, lat, lon, speed_kmh, fare,
               last_event_time, idle_since, updated_at
        FROM rt_vehicle_state WHERE vehicle_id = %s
    """, (vehicle_id,))
    latest_batch = fetch_one("""
        SELECT sim_date, trips, revenue, gps_distance_km, reported_distance_km,
               distance_variance_pct, total_cost, profit, margin_pct, utilisation_pct,
               unprofitable, distance_disputed, expense_data_present, computed_at
        FROM daily_vehicle_profitability
        WHERE vehicle_id = %s
        ORDER BY sim_date DESC LIMIT 1
    """, (vehicle_id,))
    history = fetch("""
        SELECT sim_date, trips, revenue, total_cost, profit, margin_pct
        FROM daily_vehicle_profitability
        WHERE vehicle_id = %s
        ORDER BY sim_date DESC LIMIT 7
    """, (vehicle_id,))

    if live is None and latest_batch is None:
        raise HTTPException(status_code=404, detail=f"unknown vehicle {vehicle_id}")

    return {
        "vehicle_id": vehicle_id,
        "speed_view": {"accuracy": "approximate", "seconds_old": _age(live, "updated_at"), "state": live},
        "batch_view": {"accuracy": "authoritative", "latest_day": latest_batch, "history": history},
    }


@app.get("/api/lambda/compare/{sim_date}")
def compare_views(sim_date: str) -> dict:
    """Quantify the speed/batch gap for one simulated day.

    Speed-layer earnings are a watermarked sum of fare *increments* over
    windows; batch revenue is the exact final fare of every completed trip.
    They should be close and are not expected to be equal -- this endpoint is
    what turns that claim into a number during the demo.
    """
    speed = fetch_one("""
        SELECT COALESCE(SUM(earnings), 0)  AS speed_earnings,
               COALESCE(SUM(trips), 0)     AS speed_trips,
               COUNT(DISTINCT window_start) AS windows
        FROM rt_zone_metrics
        WHERE window_start::date = %s
    """, (sim_date,)) or {}
    batch = fetch_one("""
        SELECT revenue AS batch_revenue, trips AS batch_trips, computed_at
        FROM daily_fleet_summary WHERE sim_date = %s
    """, (sim_date,))

    if batch is None:
        raise HTTPException(status_code=404, detail=f"no batch run for {sim_date} to compare against")

    speed_earnings = float(speed.get("speed_earnings") or 0.0)
    batch_revenue = float(batch["batch_revenue"] or 0.0)
    delta = speed_earnings - batch_revenue
    return {
        "sim_date": sim_date,
        "speed_view": {"earnings": round(speed_earnings, 2), "trips": speed.get("speed_trips"),
                       "windows": speed.get("windows"), "accuracy": "approximate"},
        "batch_view": {"revenue": round(batch_revenue, 2), "trips": batch.get("batch_trips"),
                       "accuracy": "authoritative", "computed_at": batch.get("computed_at")},
        "difference": {
            "absolute": round(delta, 2),
            "pct_of_batch": round(delta / batch_revenue * 100, 2) if batch_revenue else None,
            "note": "speed layer sums windowed fare increments under a watermark; "
                    "batch layer sums the final fare of each completed trip from the master dataset",
        },
    }


@app.get("/api/pipeline/status")
def pipeline_status() -> dict:
    runs = fetch("""
        SELECT run_id, sim_date, status, telemetry_rows, clean_rows, quarantined_rows,
               expense_rows, vehicles_out, started_at, finished_at, duration_seconds, notes
        FROM batch_run_audit ORDER BY started_at DESC LIMIT 10
    """)
    speed = fetch_one("""
        SELECT MAX(updated_at) AS last_write,
               EXTRACT(EPOCH FROM (now() - MAX(updated_at))) AS age_seconds,
               COUNT(*) AS vehicles
        FROM rt_vehicle_state
    """) or {}
    windows = fetch_one("""
        SELECT MAX(window_end) AS latest_window_end, COUNT(*) AS window_rows
        FROM rt_zone_metrics
    """) or {}
    return {
        "speed_layer": {**speed, **windows},
        "batch_layer": {"recent_runs": runs,
                        "last_success": next((r for r in runs if r["status"] == "success"), None)},
    }


def _age(row: dict | None, column: str) -> float | None:
    if not row or row.get(column) is None:
        return None
    value = row[column]
    return round(time.time() - value.timestamp(), 1)
