"""Render the daily reconciliation into the artefacts a human actually reads.

Two outputs, deliberately: a CSV for anyone who wants to pivot the numbers
themselves, and a self-contained HTML page (no external assets, so it opens from
disk or from the API) that answers the business question directly -- which
vehicles stopped being worth running yesterday.

Pure functions over plain dictionaries: no Spark, no database, so the layout is
unit-testable and the Spark job stays about data.
"""
from __future__ import annotations

import csv
import html
from pathlib import Path

from .profitability import DISTANCE_DISPUTE_PCT, LOW_MARGIN_FRACTION

CSV_COLUMNS = [
    "sim_date", "vehicle_id", "driver_id", "trips", "revenue", "gps_distance_km",
    "reported_distance_km", "distance_variance_pct", "fuel_cost", "maintenance_cost",
    "total_cost", "profit", "margin_pct", "revenue_per_km", "utilisation_pct",
    "service_flag", "expense_data_present", "distance_disputed", "unprofitable",
]


def write_csv(rows: list[dict], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def _fmt(value, digits: int = 2, dash: str = "&ndash;") -> str:
    if value is None:
        return dash
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return html.escape(str(value))


def _row_html(r: dict) -> str:
    classes = []
    if r["unprofitable"]:
        classes.append("unprofitable")
    if r["distance_disputed"]:
        classes.append("disputed")
    if not r["expense_data_present"]:
        classes.append("nodata")

    flags = []
    if r["unprofitable"]:
        flags.append('<span class="tag tag-loss">unprofitable</span>')
    if r["distance_disputed"]:
        flags.append('<span class="tag tag-dispute">distance disputed</span>')
    if not r["expense_data_present"]:
        flags.append('<span class="tag tag-missing">no expense data</span>')
    if r.get("service_flag") == "Y":
        flags.append('<span class="tag tag-service">serviced</span>')

    return f"""      <tr class="{' '.join(classes)}">
        <td class="id">{html.escape(r['vehicle_id'])}</td>
        <td>{_fmt(r['trips'], 0)}</td>
        <td class="num">{_fmt(r['revenue'])}</td>
        <td class="num">{_fmt(r['gps_distance_km'])}</td>
        <td class="num">{_fmt(r['reported_distance_km'])}</td>
        <td class="num">{_fmt(r['distance_variance_pct'], 1)}</td>
        <td class="num">{_fmt(r['total_cost'])}</td>
        <td class="num {'neg' if (r['profit'] or 0) < 0 else 'pos'}">{_fmt(r['profit'])}</td>
        <td class="num">{_fmt(r['margin_pct'], 1)}</td>
        <td class="num">{_fmt(r['utilisation_pct'], 1)}</td>
        <td class="flags">{' '.join(flags)}</td>
      </tr>"""


def render_html(sim_date: str, rows: list[dict], summary: dict, run_id: str, generated_at: str) -> str:
    """A standalone HTML report -- no external CSS, fonts or scripts."""
    ordered = sorted(rows, key=lambda r: (r["profit"] if r["profit"] is not None else 0))
    body_rows = "\n".join(_row_html(r) for r in ordered)

    margin = summary["margin_pct"]
    verdict_class = "pos" if (summary["profit"] or 0) > 0 else "neg"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Fleet profitability &ndash; {html.escape(sim_date)}</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: "Segoe UI", system-ui, sans-serif; margin: 2rem auto; max-width: 1180px;
          padding: 0 1rem; background: #fbfbfd; color: #16202c; }}
  h1 {{ font-size: 1.5rem; margin-bottom: .2rem; }}
  .sub {{ color: #5b6b7d; font-size: .88rem; margin-bottom: 1.5rem; }}
  .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: .75rem;
            margin-bottom: 1.6rem; }}
  .card {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 10px; padding: .85rem 1rem; }}
  .card .label {{ font-size: .72rem; text-transform: uppercase; letter-spacing: .06em; color: #6b7c90; }}
  .card .value {{ font-size: 1.35rem; font-weight: 600; margin-top: .25rem; }}
  table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e2e8f0;
           border-radius: 10px; overflow: hidden; font-size: .87rem; }}
  th, td {{ padding: .5rem .6rem; text-align: left; border-bottom: 1px solid #eef2f6; }}
  th {{ background: #f3f6fa; font-size: .74rem; text-transform: uppercase; letter-spacing: .04em;
        color: #52637a; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.id {{ font-weight: 600; }}
  tr.unprofitable {{ background: #fff6f6; }}
  .pos {{ color: #16794a; }}
  .neg {{ color: #b4232c; font-weight: 600; }}
  .tag {{ display: inline-block; font-size: .68rem; padding: .1rem .4rem; border-radius: 4px;
          margin-right: .25rem; white-space: nowrap; }}
  .tag-loss {{ background: #fde2e2; color: #98202a; }}
  .tag-dispute {{ background: #fdf0d5; color: #8a5a00; }}
  .tag-missing {{ background: #e8ecf1; color: #4d5b6b; }}
  .tag-service {{ background: #e2f0fb; color: #1f5f8b; }}
  footer {{ margin-top: 1.5rem; font-size: .76rem; color: #6b7c90; line-height: 1.5; }}
  @media (prefers-color-scheme: dark) {{
    body {{ background: #10151c; color: #e6edf5; }}
    .card, table {{ background: #172029; border-color: #263241; }}
    th {{ background: #1d2833; color: #9fb2c6; }}
    th, td {{ border-color: #22303d; }}
    tr.unprofitable {{ background: #241a1c; }}
    .sub, footer, .card .label {{ color: #93a5b8; }}
  }}
</style>
</head>
<body>
  <h1>Daily fleet profitability reconciliation</h1>
  <div class="sub">
    Simulated date <strong>{html.escape(sim_date)}</strong> &middot;
    batch run <code>{html.escape(run_id)}</code> &middot;
    generated {html.escape(generated_at)} &middot; amounts in LKR
  </div>

  <div class="cards">
    <div class="card"><div class="label">Vehicles</div><div class="value">{summary['vehicles']}</div></div>
    <div class="card"><div class="label">Trips</div><div class="value">{summary['trips']:,}</div></div>
    <div class="card"><div class="label">Revenue</div><div class="value">{summary['revenue']:,.0f}</div></div>
    <div class="card"><div class="label">Cost</div><div class="value">{summary['total_cost']:,.0f}</div></div>
    <div class="card"><div class="label">Profit</div>
      <div class="value {verdict_class}">{summary['profit']:,.0f}</div></div>
    <div class="card"><div class="label">Margin</div>
      <div class="value">{_fmt(margin, 1)}%</div></div>
    <div class="card"><div class="label">Unprofitable</div>
      <div class="value">{summary['unprofitable_vehicles']}</div></div>
    <div class="card"><div class="label">Disputed distance</div>
      <div class="value">{summary['disputed_vehicles']}</div></div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Vehicle</th><th>Trips</th><th>Revenue</th><th>GPS km</th><th>Billed km</th>
        <th>Variance %</th><th>Cost</th><th>Profit</th><th>Margin %</th><th>Utilisation %</th><th>Flags</th>
      </tr>
    </thead>
    <tbody>
{body_rows}
    </tbody>
  </table>

  <footer>
    Sorted worst-first by profit. A vehicle is flagged <em>unprofitable</em> when it lost money or
    earned a margin below {LOW_MARGIN_FRACTION * 100:.0f}% of revenue; <em>distance disputed</em>
    when the partner's billed distance differs from the GPS-derived distance by more than
    {DISTANCE_DISPUTE_PCT:.0f}%. Revenue is the batch layer's authoritative figure, taken as the
    final cumulative fare of each completed trip in the master dataset &ndash; not the speed layer's
    running approximation.
  </footer>
</body>
</html>
"""


def write_html(sim_date: str, rows: list[dict], summary: dict, run_id: str,
               generated_at: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_html(sim_date, rows, summary, run_id, generated_at), encoding="utf-8")
    return path
