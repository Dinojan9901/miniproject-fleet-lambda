# The Fleet Project, Explained Simply

This guide explains the project as if you are new to data engineering. It starts with how to run it, then explains what the pieces do and how to read what they produce.

## 1. How to run the project

### Before you start

- Install and start **Docker Desktop**.
- Make sure Docker Desktop has enough memory (the project README recommends about 8 GB).
- Open PowerShell in the project folder:

```powershell
cd "C:\Users\H.S.Thishon\OneDrive\Desktop\BigData_Project\miniproject-fleet-lambda"
```

### Start the containers

The first command builds the project’s own Docker images. The next starts the services in the background:

```powershell
docker compose build
docker compose up -d
```

The first build may take several minutes because Docker needs to download software and assemble the project images. If a download times out, retry the build when the internet connection is stable.

Check whether the services are running:

```powershell
docker compose ps
```

Wait until the main services show as `Up` or `healthy`. Some data takes time to appear because the simulated clock moves faster than real time:

- Vehicle status usually appears in about 15 seconds.
- The first live summary takes about 30 seconds.
- One simulated day takes about 5 real minutes, so the first daily report needs a batch run after a day of simulated data is available.

### Open the project

| Open this address | What you see |
|---|---|
| <http://localhost:18000> | Fleet dashboard |
| <http://localhost:18000/docs> | API endpoints and interactive API page |
| <http://localhost:8088> | Airflow, which runs the daily batch job; login `admin`, password `admin` |
| <http://localhost:4040> | Spark’s page showing streaming work |
| <http://localhost:3000> | Grafana monitoring dashboard |
| <http://localhost:9090> | Prometheus metrics and alerts |

The API is published on **port 18000** on your computer. Inside Docker, the API listens on port 8000. So when using a browser on your computer, use `localhost:18000`.

### Check that the pipeline is working

From PowerShell, look at the simulated vehicle feed and Spark processing:

```powershell
docker compose logs --tail 15 telemetry-producer
docker compose logs --tail 15 speed-layer
```

In the producer log, `producer_heartbeat` means it is sending events. In the speed-layer log, `micro_batch_complete` means Spark has processed a group of events. `WARNING` lines about unusual events are expected: the simulator intentionally creates some bad sample records to demonstrate data checks.

You can also run the project’s health checker from a Python environment where the project requirements are installed:

```powershell
python scripts/smoke_check.py --api http://localhost:18000
```

`PASS` means a check is working. `PENDING` means it is waiting for enough simulated data. For example, a daily check cannot pass before there is a completed daily batch.

### Get the daily report

The report is created by the batch layer after Airflow finishes its daily job. In Airflow, open the `fleet_daily_batch` DAG and check that its tasks are green/successful. Then open the report for a date that has been processed. For the run discussed while this guide was prepared, that date was `2026-09-06`:

<http://localhost:18000/api/reports/daily/2026-09-06/html>

Change the date only to a simulated date that has actually been processed. If the page says `no rendered report`, the batch has not produced the HTML report for that date yet, or the date is not the one processed.

To start a batch run manually from PowerShell:

```powershell
docker compose exec airflow-scheduler airflow dags trigger fleet_daily_batch
```

This asks Airflow to run the DAG. It does not mean the run has already finished. In the Airflow page, find the new run and wait for the tasks to finish. The normal task path is:

`find_pending_day` → `wait_for_master_dataset` → `run_batch_layer` → `validate_output` → `mark_processed` → `announce`

If a task turns red, open that task and read its **Logs** tab. Compose now runs a `data-init` service before Airflow to create `/data/landing/_processed` and `/data/reports` with the required write permissions. If those directories still show a permission error on an older running stack, recreate the initializer with `docker compose up -d --force-recreate data-init airflow-init`, then run `docker compose up -d` again. A task failure means the report may not exist yet.

### Stop the project safely

```powershell
docker compose down
```

This stops and removes the containers but keeps the named data volumes. Avoid `docker compose down -v` unless you intentionally want to erase the saved data, reports, database contents, and simulated clock state.

## 2. What problem does the project solve?

Imagine a taxi company with 12 cars. The company wants to answer two questions:

1. **What is happening right now?** How many cars are working, where are they, and how much are they earning?
2. **How did each car do for the whole day?** After fuel and repair bills arrive, did each car earn enough to cover its costs?

The first answer should be fast, even if it is a close estimate. The second answer can wait, but should use the whole day and the costs. One calculation cannot serve both needs equally well, so the project uses two paths. This design is called a **Lambda architecture**.

## 3. The project as a mailroom

Think of the system as a busy mailroom:

1. **The vehicles write postcards.** The telemetry simulator sends small messages about a vehicle’s location, speed, status, and fare.
2. **Kafka sorts and holds the postcards.** It is like a set of labeled mail trays. The messages go into topics such as `fleet.telemetry`.
3. **Spark reads the live mail.** It checks each message, updates the latest vehicle picture, totals short time windows, and raises alerts.
4. **The project saves the original messages.** It stores them in a Parquet data lake so the whole day can be checked again later.
5. **The expense simulator sends a daily bill file.** It makes a CSV with fuel and maintenance costs for each vehicle.
6. **Airflow calls the daily worker.** Airflow is like a manager’s checklist: it waits for the required files, starts the batch calculation, checks its results, and marks the day complete.
7. **The batch worker calculates the careful answer.** It combines the full day of saved vehicle messages with the expense CSV.
8. **PostgreSQL stores answers for the website.** The API reads these stored answers and shows them on the dashboard or in a report.
9. **Prometheus and Grafana watch the system.** They show whether messages and calculations are continuing to arrive.

## 4. The two answers: fast and careful

### Speed layer: “What is happening now?”

The speed layer uses **Spark Structured Streaming**. It reads new Kafka events repeatedly in small groups called micro-batches. It checks the event, keeps the newest state for each vehicle, calculates live summaries, and looks for long-idle vehicles.

Because the answer must be quick, it uses time windows and allows some late events to be missed. Counts can be approximate. This is useful for a dispatcher who needs a fresh picture now.

### Batch layer: “What really happened that day?”

The batch layer reads the saved events for a complete simulated day and joins them to that day’s expenses. It can calculate revenue, distance, costs, profit, and margin with all available daily data.

Airflow runs and tracks this work. Its DAG is a sequence of steps. A successful final step means the daily result passed validation and was marked as processed.

### Why can the two totals be different?

They count different things. The live speed result adds fare increments in time windows and may not include late events. The daily batch result uses the final fare for completed trips in the full day. Therefore, the comparison endpoint shows a difference instead of pretending the answers must match.

During one project run, the comparison for `2026-09-06` showed speed earnings of `45,419.41` and batch revenue of `76,024.04`, a difference of `-30,604.63` (`-40.26%` of batch revenue). Those are results from that particular run and date, not fixed values for every run.

## 5. What are the main tools?

| Tool | Grade-school explanation |
|---|---|
| **Docker Compose** | Starts the project’s separate computers (containers) together and connects them. |
| **Kafka** | Holds and delivers the stream of vehicle messages. |
| **Spark** | Quickly sorts and calculates lots of incoming messages, and can also calculate a full day’s result. |
| **Airflow** | Starts the daily job, checks its steps, and records success or failure. |
| **Parquet data lake** | A saved collection of the original events, organized so the batch worker can read them later. |
| **PostgreSQL** | A database that keeps the latest vehicle information and daily results for the API. |
| **FastAPI** | The web doorway that serves the dashboard, reports, and data endpoints. |
| **Prometheus and Grafana** | Tools that collect health measurements and display them as charts. |

## 6. What is a simulated clock?

Waiting a real 24 hours for a daily file would make a classroom demo very slow. The project speeds up its pretend calendar: **about 5 real minutes equal one simulated day**. The simulated calendar uses **Sri Lankan time (`Asia/Colombo`, UTC+05:30)**, so each simulated day starts at midnight in Sri Lanka. Service logs, Airflow's schedule and UI, dashboard timestamps, and database timestamp display also use Sri Lankan time. The containers share a saved clock anchor so they agree about the simulated date. The computers still measure the same real moments; timestamps are just shown with the Sri Lankan offset.

For example, a log might say the real time is 3:30 PM while `sim_now` says it is 9:00 AM on a simulated date. These are two different clocks: one is your computer’s clock, and the other belongs to the pretend fleet world.

## 7. What do strange-looking logs mean?

- `producer_heartbeat`: the simulator is alive and sending events.
- `anomalous_event_emitted`: the simulator purposely made an unusual event for testing.
- `quarantined`: Spark rejected an event that failed a data rule and saved it separately for inspection.
- `micro_batch_complete`: Spark finished one small group of streaming events. `events` is the number examined; `clean` is how many passed; `quarantined` is how many were rejected.
- `expense_file_published`: the daily source saved an expense CSV.
- `PENDING` from the smoke check: the service may be healthy but has not gathered enough data for that check yet.
- Red/failed Airflow task: that step encountered an error. Read its task log to see the reason before expecting a report.

## 8. Useful commands

Show running services:

```powershell
docker compose ps
```

Show recent logs:

```powershell
docker compose logs --tail 30 telemetry-producer
docker compose logs --tail 30 speed-layer
docker compose logs --tail 30 airflow-scheduler
```

List Kafka topics:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

See the expense CSV files inside Docker:

```powershell
docker compose exec expense-source ls -la /data/landing/expenses
```

See whether the API is answering:

```powershell
curl.exe http://localhost:18000/health
```

## 9. One-sentence summary

The project makes pretend taxi data, sends it through Kafka, uses Spark for a quick live view and a careful whole-day calculation, lets Airflow manage the daily work, saves the answers, and displays them through a dashboard and reports.
