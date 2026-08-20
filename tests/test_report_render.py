"""The rendered daily report -- the artefact a fleet manager actually opens."""
import csv

import pytest

from batch_layer.profitability import summarise
from batch_layer.report_render import CSV_COLUMNS, render_html, write_csv, write_html


def row(**overrides) -> dict:
    base = {
        "sim_date": "2026-08-01", "vehicle_id": "FLEET-001", "driver_id": "DRV-001",
        "trips": 9, "revenue": 14500.0, "gps_distance_km": 152.4,
        "reported_distance_km": 158.0, "distance_variance_pct": 3.7,
        "fuel_cost": 4200.0, "maintenance_cost": 600.0, "total_cost": 4800.0,
        "profit": 9700.0, "margin_pct": 66.9, "revenue_per_km": 95.1,
        "utilisation_pct": 61.0, "service_flag": "N", "expense_data_present": True,
        "distance_disputed": False, "unprofitable": False,
    }
    base.update(overrides)
    return base


@pytest.fixture
def rows():
    return [
        row(),
        row(vehicle_id="FLEET-002", profit=-1200.0, revenue=3000.0, total_cost=4200.0,
            margin_pct=-40.0, unprofitable=True),
        row(vehicle_id="FLEET-003", distance_variance_pct=64.0, distance_disputed=True),
        row(vehicle_id="FLEET-004", fuel_cost=None, maintenance_cost=None, total_cost=0.0,
            reported_distance_km=None, distance_variance_pct=None, expense_data_present=False),
    ]


def test_csv_has_a_header_and_one_row_per_vehicle(tmp_path, rows):
    path = write_csv(rows, tmp_path / "report.csv")
    with path.open(encoding="utf-8") as fh:
        parsed = list(csv.DictReader(fh))
    assert list(parsed[0]) == CSV_COLUMNS
    assert len(parsed) == 4
    assert {r["vehicle_id"] for r in parsed} == {f"FLEET-00{i}" for i in range(1, 5)}


def test_html_is_self_contained(tmp_path, rows):
    """No external CSS, fonts or scripts -- the file has to open from disk."""
    html = render_html("2026-08-01", rows, summarise(rows), "run-1", "2026-08-01T00:05:00Z")
    for forbidden in ("http://", "https://", "<script", "cdn."):
        assert forbidden not in html


def test_html_sorts_worst_first(rows):
    html = render_html("2026-08-01", rows, summarise(rows), "run-1", "2026-08-01T00:05:00Z")
    assert html.index("FLEET-002") < html.index("FLEET-001")


def test_html_flags_every_exception(rows):
    html = render_html("2026-08-01", rows, summarise(rows), "run-1", "2026-08-01T00:05:00Z")
    assert "unprofitable" in html
    assert "distance disputed" in html
    assert "no expense data" in html


def test_missing_values_render_as_a_dash_not_none(rows):
    html = render_html("2026-08-01", rows, summarise(rows), "run-1", "2026-08-01T00:05:00Z")
    assert "None" not in html
    assert "&ndash;" in html


def test_summary_cards_show_the_fleet_totals(rows):
    summary = summarise(rows)
    html = render_html("2026-08-01", rows, summary, "run-1", "2026-08-01T00:05:00Z")
    assert f"{summary['revenue']:,.0f}" in html
    assert str(summary["unprofitable_vehicles"]) in html


def test_write_html_creates_missing_directories(tmp_path, rows):
    path = write_html("2026-08-01", rows, summarise(rows), "run-1", "2026-08-01T00:05:00Z",
                      tmp_path / "nested" / "deeper" / "report.html")
    assert path.exists()
    assert "<!DOCTYPE html>" in path.read_text(encoding="utf-8")
