"""Airflow DAG: the nightly (here, per-simulated-day) batch reconciliation.

Schedule: every 5 real minutes, because 5 real minutes is one simulated day.
The DAG is deliberately *not* driven by Airflow's logical date. It discovers
which simulated day is ready by looking for an expense file that has no
completion marker, which means:

  - a run that fails and is retried picks up the same day again;
  - a day whose file arrives late is still processed, just later;
  - re-running an already-processed day is possible on purpose (clear the
    marker) and safe, because every write in the batch layer is an upsert.

Task flow:

    find_pending_day -> wait_for_master_dataset -> run_batch_layer
                                                        |
                                     validate_output <--+
                                            |
                                     mark_processed -> announce

`validate_output` is a real gate, not decoration: if the batch layer wrote
implausible numbers the day is left unmarked and the failure is visible, rather
than a bad report quietly reaching the dashboard.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

DATA_DIR = os.getenv("DATA_DIR", "/data")
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/project")

LANDING_DIR = Path(DATA_DIR) / "landing" / "expenses"
PROCESSED_DIR = Path(DATA_DIR) / "landing" / "_processed"
LAKE_DIR = Path(DATA_DIR) / "lake" / "telemetry"
REPORTS_DIR = Path(DATA_DIR) / "reports"

DEFAULT_ARGS = {
    "owner": "fleet-data-eng",
    "depends_on_past": False,
    # Transient failures here are Postgres or the JVM still coming up; both
    # clear on a retry. Backoff keeps a genuinely broken run from hammering.
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=8),
}


with DAG(
    dag_id="fleet_daily_batch",
    description="Daily fleet profitability reconciliation (Lambda batch layer)",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="*/5 * * * *",     # one simulated day
    catchup=False,              # yesterday's missed slots are not useful; the
                                # file-marker scan already finds unprocessed days
    max_active_runs=1,          # one reconciliation at a time: they share tables
    tags=["lambda", "batch-layer", "fleet"],
    doc_md=__doc__,
) as dag:

    @task
    def find_pending_day() -> str:
        """Oldest expense file with no completion marker."""
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        if not LANDING_DIR.exists():
            raise AirflowSkipException(f"no landing directory yet at {LANDING_DIR}")

        pending = sorted(
            path for path in LANDING_DIR.glob("expenses_*.csv")
            if not (PROCESSED_DIR / f"{path.stem}.done").exists()
        )
        if not pending:
            # Nothing to do is a normal outcome on most 5-minute ticks.
            raise AirflowSkipException("no unprocessed expense files")

        sim_date = pending[0].stem.replace("expenses_", "")
        print(f"selected sim_date={sim_date} from {pending[0]} ({len(pending)} pending)")
        return sim_date

    def _master_dataset_ready(sim_date: str) -> bool:
        partition = LAKE_DIR / f"sim_date={sim_date}"
        ready = partition.exists() and any(partition.glob("*.parquet"))
        print(f"master dataset for {sim_date}: {'ready' if ready else 'not yet'} ({partition})")
        return ready

    wait_for_master_dataset = PythonSensor(
        task_id="wait_for_master_dataset",
        python_callable=_master_dataset_ready,
        op_kwargs={"sim_date": "{{ ti.xcom_pull(task_ids='find_pending_day') }}"},
        poke_interval=15,
        timeout=60 * 4,
        # Release the worker slot between pokes instead of holding it idle.
        mode="reschedule",
    )

    run_batch_layer = BashOperator(
        task_id="run_batch_layer",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"PYTHONPATH={PROJECT_DIR} spark-submit "
            "--master 'local[2]' --driver-memory 1g "
            "--conf spark.ui.enabled=false "
            "--conf spark.sql.adaptive.enabled=true "
            f"{PROJECT_DIR}/batch_layer/daily_reconciliation.py "
            "--sim-date {{ ti.xcom_pull(task_ids='find_pending_day') }} "
            "--run-id {{ run_id | replace(':', '-') | replace('+', '-') }}"
        ),
    )

    @task
    def validate_output(sim_date: str) -> dict:
        """Quality gate on what the batch layer actually wrote."""
        import sys
        sys.path.insert(0, PROJECT_DIR)
        from common.db import query_dicts   # imported late: Airflow parses this file often

        audit = query_dicts(
            "SELECT status, vehicles_out, clean_rows, notes FROM batch_run_audit "
            "WHERE sim_date = %s ORDER BY started_at DESC LIMIT 1", (sim_date,),
        )
        if not audit:
            raise AirflowFailException(f"no audit row for {sim_date} -- did the job run?")
        if audit[0]["status"] != "success":
            raise AirflowFailException(f"batch run for {sim_date} reported {audit[0]['status']}: {audit[0]['notes']}")

        summary = query_dicts(
            "SELECT vehicles, trips, revenue, total_cost, profit, unprofitable_vehicles "
            "FROM daily_fleet_summary WHERE sim_date = %s", (sim_date,),
        )
        if not summary:
            raise AirflowFailException(f"no fleet summary written for {sim_date}")
        s = summary[0]

        problems = []
        if s["vehicles"] <= 0:
            problems.append("no vehicles in the reconciliation")
        if s["revenue"] is None or s["revenue"] < 0:
            problems.append(f"implausible revenue {s['revenue']}")
        if s["total_cost"] is None or s["total_cost"] < 0:
            problems.append(f"implausible cost {s['total_cost']}")

        nulls = query_dicts(
            "SELECT COUNT(*) AS n FROM daily_vehicle_profitability "
            "WHERE sim_date = %s AND (profit IS NULL OR revenue IS NULL)", (sim_date,),
        )[0]["n"]
        if nulls:
            problems.append(f"{nulls} vehicle rows have null revenue or profit")

        report = REPORTS_DIR / f"daily_profitability_{sim_date}.html"
        if not report.exists():
            problems.append(f"rendered report missing at {report}")

        if problems:
            raise AirflowFailException(f"validation failed for {sim_date}: " + "; ".join(problems))

        print(f"validation passed for {sim_date}: {s}")
        return {"sim_date": sim_date, **{k: float(v) if v is not None else None
                                         for k, v in s.items()}}

    @task
    def mark_processed(sim_date: str) -> str:
        """Record completion so the next run moves on to the following day."""
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        marker = PROCESSED_DIR / f"expenses_{sim_date}.done"
        marker.write_text(
            f"completed_at={datetime.utcnow().isoformat()}Z\nmarker_id={uuid.uuid4().hex}\n",
            encoding="utf-8",
        )
        print(f"marked {sim_date} processed -> {marker}")
        return str(marker)

    @task
    def announce(validated: dict) -> None:
        sim_date = validated["sim_date"]
        print(
            f"reconciliation complete for {sim_date}: "
            f"{validated['vehicles']:.0f} vehicles, {validated['trips']:.0f} trips, "
            f"revenue {validated['revenue']:,.2f}, profit {validated['profit']:,.2f}, "
            f"{validated['unprofitable_vehicles']:.0f} vehicle(s) flagged unprofitable. "
            f"Report: /api/reports/daily/{sim_date}/html"
        )

    sim_date = find_pending_day()
    validated = validate_output(sim_date)

    sim_date >> wait_for_master_dataset >> run_batch_layer >> validated
    validated >> mark_processed(sim_date) >> announce(validated)
