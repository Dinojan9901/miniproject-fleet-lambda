# Fleet Operations Lambda Pipeline — Two-Person Demo Script

**Target length:** 9 minutes (about 4 minutes 30 seconds per presenter)  
**Demo mode:** The project is already running. This demo checks that it is ready, then shows the working system. It does not rebuild or reset anything.

## Before presenting

Open these browser tabs before the demo:

| Tab | What to show |
|---|---|
| `http://localhost:18000` | Fleet dashboard: live fleet, recent windows, batch results and comparison |
| `http://localhost:4040` | Spark application UI for the speed layer |
| `http://localhost:8088` | Airflow, DAG `fleet_daily_batch` |
| `http://localhost:3000` | Grafana dashboard, **Fleet pipeline health** |
| `http://localhost:18000/api/reports` | Available daily report dates |

Airflow login, if it asks: `admin` / `admin`.

Keep Terminal 1 open at the project folder. Do not run `docker compose down -v`: it deletes the saved simulation data and database.

## 1. Quick readiness check (about 35 seconds)

Run these read-only checks before starting the presentation:

```powershell
docker compose ps
curl.exe http://localhost:18000/health
curl.exe http://localhost:18000/api/reports
```

Check that the main containers say `Up`, the health response says `healthy`, and `/api/reports` lists at least one completed simulated date. Use one of those dates later when opening a report or comparison. If a date is not listed, wait for the batch run to finish before recording.

The project is already running for the demo. Do not run a fresh reset. If it was stopped, start it with `docker compose up -d` and wait for the health check to pass.

## 2. Speaking plan

### Presenter A - 0:00 to 4:30

#### 0:00-0:35 - Project introduction and readiness

**Show:** Terminal 1 with the checks from Section 1. Point out that the containers are up, the API reports healthy with its database reachable, and at least one daily report is available.

**Say:**

> "We are demonstrating a data pipeline for a ride-hailing fleet. It helps operators follow vehicle activity as it happens and review each vehicle earnings and costs at the end of the day. The services are running, the API can reach its database, and a daily report is ready. First, I will show how the live vehicle data flows through the system. Then Presenter B will show how the daily report is produced."

#### 0:35-1:25 - Project purpose and system overview

**Show:** Fleet dashboard at `http://localhost:18000`.

**Say:**

> "The project uses simulated vehicles so we can see how the pipeline works. Kafka receives their updates, and Spark processes the live stream. The dashboard uses those results to show the current fleet. A separate daily process combines the complete vehicle data with fuel and maintenance costs to calculate each vehicle profit or loss."

#### 1:25-2:35 - Follow the live fleet view

**Show:** On the dashboard, point to vehicle counts and statuses, recent zone and time-window figures, and alerts.

**Say:**

> "Here we can see which vehicles are active or idle, where they are operating, and how activity and earnings change over recent time windows. These figures update as new vehicle messages arrive, so they give operators a current view of the fleet."

> "Spark also checks incoming events. Invalid messages are sent to a quarantine topic for inspection, and rules can raise alerts, for example when a vehicle stays idle too long."

#### 2:35-3:30 - Show Spark processing the stream

**Show:** Spark UI at `http://localhost:4040`. Point to **Structured Streaming** or recent completed jobs and micro-batches.

**Say:**

> "This is the Spark job behind the live view. It reads vehicle messages from Kafka and processes them in small groups called micro-batches. The job updates the vehicle state and recent summaries, while handling invalid events and alerts."

#### 3:30-4:30 - Confirm the stream and hand off

**Show:** Terminal 1. Run:

```powershell
docker compose logs --tail 5 telemetry-producer
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

**Say:**

> "The producer log shows the simulated vehicle messages and occasional test anomalies. Kafka has separate topics for normal telemetry, quarantined events and alerts. This confirms the live path we just saw."

> "I will hand over to Presenter B to follow the daily path: Airflow waits for the full-day data and expense file, runs and checks the batch job, and makes the daily report."
### Presenter B — 4:30 to 9:00

#### 4:30–5:25 — Daily expense files and simulated time

**Show:** Terminal 1. Run:

```powershell
docker compose logs --tail 5 expense-source
docker compose exec expense-source ls -la /data/landing/expenses
```

**Say:**

> “This service creates one expense CSV for each simulated day. It includes fuel and maintenance costs. These files are saved in the shared data volume so the batch job can read them.”

> “The simulated calendar is set to Sri Lankan time. A simulated day takes about five real minutes, so we can demonstrate a daily process without waiting a full day.”

#### 5:25–6:45 — Airflow daily batch

**Show:** Airflow at `http://localhost:8088`. Open `fleet_daily_batch`, then select a successful recent run. Show the task graph/grid and the green task states.

**Say:**

> “Airflow coordinates the daily work. First, it looks for an expense file that has not been processed. Then it waits until Spark has written that date’s full telemetry data. The batch job calculates the day again from the full data and joins it to the expenses.”

> “Airflow validates the output before marking the date as processed. The final announce step records that the run completed. A green run means every required step succeeded.”

If needed, point to these tasks in order:

```text
find_pending_day → wait_for_master_dataset → run_batch_layer
→ validate_output → mark_processed → announce
```

#### 6:45–7:45 — Daily report

**Show:** First show `http://localhost:18000/api/reports`. Choose a date in its response, then open:

```text
http://localhost:18000/api/reports/daily/<date>/html
```

Replace `<date>` with a date returned by `/api/reports`, for example `2026-08-17` if it is listed.

**Say:**

> “This is the saved daily report for a completed simulated date. It shows each vehicle’s revenue, expenses, profit, distance and flags. The batch result is authoritative for this daily reconciliation because it uses the complete telemetry data and the expense file.”

#### 7:45–8:35 — Compare the two views

**Show:** Open `http://localhost:18000/api/lambda/compare/<date>` using the same date. Point to `speed_view`, `batch_view` and `difference`.

**Say:**

> “This comparison puts the fast live estimate beside the daily result. They can differ because the live layer sums fare increments, while the batch layer uses final fares for completed trips. In this project, the trip-start flagfall is included in the final fare but is not emitted as a fare increment, so it is one measured reason the live earnings are lower.”

> “The live trip count can also count the same trip in more than one window or zone group. The batch trip count is the completed-trip count for that date.”

#### 8:35–9:00 — Monitoring and close

**Show:** Grafana at `http://localhost:3000`, dashboard **Fleet pipeline health**. Point to event rate/freshness, quarantine, batch and API panels.

**Say:**

> “Grafana helps us see whether the data is arriving, whether Spark is processing it, and whether the batch and API are healthy. Together, Kafka, Spark, Airflow, the database, the API and monitoring make the complete pipeline visible. Thank you.”

## If a page looks empty during the demo

- **No daily report:** Check `/api/reports` for an available date. Airflow must show a successful `announce` task for that date.
- **Airflow run is still running:** Wait for its tasks to finish, then refresh `/api/reports`.
- **No live vehicles or windows:** Give the stream a little time to warm up, then refresh the fleet dashboard.
- **Grafana asks for a dashboard:** Open **Fleet pipeline health** from the dashboards list.

Do not delete volumes or reset the simulated clock during the presentation.
