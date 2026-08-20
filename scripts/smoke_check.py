"""End-to-end smoke check: is every layer of the pipeline actually alive?

Run it from the host after `docker compose up -d`. It answers, in order, the
questions a demonstrator gets asked:

    is data arriving?          -> Kafka topic offsets are advancing
    is the speed layer live?   -> rt_vehicle_state was written seconds ago
    is the lake filling?       -> the API reports master-dataset partitions
    has the batch layer run?   -> a successful batch_run_audit row exists
    are the two views merged?  -> /api/lambda/compare returns a difference
    is monitoring up?          -> Prometheus has targets and rules loaded

Exit code 0 means everything that *should* be ready at this point is ready.
Components that legitimately need more simulated time are reported as PENDING
rather than failures.

    python scripts/smoke_check.py
    python scripts/smoke_check.py --api http://localhost:8000
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

OK, PENDING, FAIL = "PASS", "PENDING", "FAIL"


def get(url: str, timeout: float = 10.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def check(label: str, fn) -> tuple[str, str, str]:
    try:
        status, detail = fn()
    except urllib.error.URLError as exc:
        return FAIL, label, f"unreachable: {exc.reason}"
    except Exception as exc:  # noqa: BLE001
        return FAIL, label, f"{type(exc).__name__}: {exc}"
    return status, label, detail


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke-check the fleet pipeline")
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--prometheus", default="http://localhost:9090")
    args = parser.parse_args(argv)

    results = []

    results.append(check("serving API healthy", lambda: (
        (OK, "database reachable") if get(f"{args.api}/health")["status"] == "healthy"
        else (FAIL, "unhealthy"))))

    def speed_layer():
        live = get(f"{args.api}/api/fleet/live")
        fleet = live["fleet"]
        age = fleet.get("view_age_seconds")
        if not fleet.get("vehicles"):
            return PENDING, "no vehicle state yet -- the speed layer needs ~15s of stream"
        if age is not None and age > 120:
            return FAIL, f"speed view is {age:.0f}s stale"
        return OK, (f"{fleet['vehicles']} vehicles tracked "
                    f"({fleet['on_trip']} on trip, {fleet['idle']} idle), "
                    f"{age:.0f}s old" if age is not None else "fresh")

    results.append(check("speed layer writing state", speed_layer))

    def windows():
        zones = get(f"{args.api}/api/fleet/zones?windows=3")
        if not zones["rows"]:
            return PENDING, "no closed windows yet -- one simulated hour is ~12.5s"
        distinct = {r["window_start"] for r in zones["rows"]}
        earnings = sum(r["earnings"] or 0 for r in zones["rows"])
        return OK, f"{len(distinct)} window(s), {len(zones['rows'])} zone rows, {earnings:,.0f} LKR"

    results.append(check("windowed aggregation", windows))

    def alerts():
        data = get(f"{args.api}/api/alerts?limit=5")
        if not data["alerts"]:
            return PENDING, "no alerts yet -- the idle threshold needs ~45 simulated minutes"
        return OK, f"{len(data['alerts'])} alert(s), newest: {data['alerts'][0]['message'][:60]}"

    results.append(check("threshold alerts", alerts))

    def batch():
        status = get(f"{args.api}/api/pipeline/status")
        last = status["batch_layer"]["last_success"]
        if last is None:
            runs = status["batch_layer"]["recent_runs"]
            if runs:
                return FAIL, f"latest run {runs[0]['run_id']} is {runs[0]['status']}: {runs[0]['notes']}"
            return PENDING, "no batch run yet -- the first lands after one simulated day (~5 min)"
        return OK, (f"{last['sim_date']} reconciled in {last['duration_seconds']:.1f}s, "
                    f"{last['clean_rows']} clean rows -> {last['vehicles_out']} vehicles")

    results.append(check("batch layer reconciliation", batch))

    def merge():
        days = get(f"{args.api}/api/reports")["days"]
        if not days:
            return PENDING, "no reconciled day to compare yet"
        cmp = get(f"{args.api}/api/lambda/compare/{days[0]['sim_date']}")
        return OK, (f"speed {cmp['speed_view']['earnings']:,.0f} vs "
                    f"batch {cmp['batch_view']['revenue']:,.0f} "
                    f"({cmp['difference']['pct_of_batch']}% apart)")

    results.append(check("speed/batch comparison", merge))

    def monitoring():
        targets = get(f"{args.prometheus}/api/v1/targets")["data"]["activeTargets"]
        up = [t for t in targets if t["health"] == "up"]
        rules = get(f"{args.prometheus}/api/v1/rules")["data"]["groups"]
        total_rules = sum(len(g["rules"]) for g in rules)
        if len(up) < len(targets):
            down = ", ".join(sorted({t["labels"]["job"] for t in targets if t["health"] != "up"}))
            return PENDING, f"{len(up)}/{len(targets)} targets up ({total_rules} rules); down: {down}"
        return OK, f"{len(up)}/{len(targets)} targets up, {total_rules} alert rules loaded"

    results.append(check("monitoring", monitoring))

    width = max(len(label) for _, label, _ in results)
    print("\nFleet pipeline smoke check")
    print("=" * (width + 46))
    for status, label, detail in results:
        print(f"  [{status:<7}] {label.ljust(width)}  {detail}")
    print("=" * (width + 46))

    failures = [r for r in results if r[0] == FAIL]
    pending = [r for r in results if r[0] == PENDING]
    if failures:
        print(f"\n{len(failures)} check(s) FAILED.")
        return 1
    if pending:
        print(f"\nAll healthy; {len(pending)} check(s) still warming up. "
              "Re-run in a minute or two.")
    else:
        print("\nEverything is up.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
