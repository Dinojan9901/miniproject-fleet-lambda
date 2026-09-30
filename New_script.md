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
| `http://localhost:9090/alerts` | Prometheus alert rules (Presenter B) |
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

> "We are demonstrating a data pipeline for a ride-hailing fleet. It helps operators follow vehicle activity as it happens and review each vehicle earnings and costs at the end of the day. The services are running, the API can reach its database, and create the daily report. First, I will show how the live vehicle data flows through the system. Then Dinojan will show how the daily report is produced."

#### 0:35-1:25 - Project purpose and system overview

**Show:** Fleet dashboard at `http://localhost:18000`.

**Say:**

> "The project uses simulated vehicles so we can see how the pipeline works. Simulated vehicle data flows through Kafka to Spark Structured Streaming, which updates the live fleet data and saves the full telemetry history. Each day, Airflow coordinates a Spark batch job that combines that history with fuel and maintenance costs to create daily results and a report. The API brings the live and daily results to the fleet dashboard. Prometheus and Grafana monitor the pipeline’s health."

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

> "I will hand over to Dinojan to follow the daily path: Airflow waits for the full-day data and expense file, runs and checks the batch job, and makes the daily report."
### Presenter B - 4:30 to 9:00

**Before recording, Presenter B should:**

- Open `http://localhost:18000/api/reports` and pick one date from the list. Use this same date for the report and the comparison.
- In Airflow, find one green (successful) run of `fleet_daily_batch` and keep it open.
- Open `http://localhost:9090/alerts` in a tab.

#### 4:30-5:15 - Daily expense files and simulated time

**Show:** Terminal 1. Run:

```powershell
docker compose logs --tail 5 expense-source
docker compose exec expense-source ls -la /data/landing/expenses
```

**Say:**

> "Thank you. Now I will show the daily part. This service makes one expense file for each simulated day. The file has the fuel cost and the maintenance cost of each vehicle."

> "Look at these log lines. Every service logs structured JSON. This makes the logs easy to search."

> "Our time is compressed. One simulated day takes five real minutes. The calendar uses Sri Lankan time. So we can show a daily job without waiting a full day."

#### 5:15-6:30 - Airflow daily batch

**Show:** Airflow at `http://localhost:8088`. Open `fleet_daily_batch`, then select the green run. Show the task graph or grid.

**Say:**

> "Airflow runs the daily job. It does these steps in order."

> "First, it finds an expense file that is not processed yet. Second, it checks that the vehicle data for that day is saved. Third, it runs the Spark batch job. This job calculates the whole day again and joins it with the expenses."

> "Fourth, it checks the results. If the numbers look wrong, the run fails and the day is not marked as done. Only after this check, the day is marked as finished. Here Green means the step passed."



If needed, point to these tasks in order:

```text
find_pending_day -> wait_for_master_dataset -> run_batch_layer
-> validate_output -> mark_processed -> announce
```

#### 6:30-7:25 - Daily report

**Show:** First show `http://localhost:18000/api/reports`. Then open the report for the date you picked:

```text
http://localhost:18000/api/reports/daily/<date>/html
```

Replace `<date>` with the date you picked from `/api/reports`.

**Say:**

> "This is the daily report for one simulated day. Each row is one vehicle. We can see its revenue, its cost, its profit and its distance."

> "The report also gives warnings. A vehicle is flagged when its profit is too low. It is also flagged when the billed distance does not match the GPS distance."

> "This report is the final, correct answer for the day, because it uses the full day's data and the expense file."

#### 7:25-8:15 - Compare the two views

**Show:** Open `http://localhost:18000/api/lambda/compare/<date>` with the same date. Point to `speed_view`, `batch_view` and `difference`.

**Say:**

> "This is a Lambda architecture. It has two layers. The speed layer gives fast answers that are close, but not exact. The batch layer gives the correct answer later. This page shows both side by side."

> "The two numbers are different, and we know why. Every trip starts with a fixed charge of 120 rupees. The batch layer counts it, because it uses the final fare. The speed layer adds up only the fare increases, so it misses this starting charge. That is the main reason the speed number is lower."

> "The trip counts are also different. The speed layer can count one trip more than once, when the trip crosses two time windows or two zones. The batch layer counts each finished trip only once."

#### 8:15-9:00 - Monitoring and close

**Show:** First the Prometheus alert rules at `http://localhost:9090/alerts` (about 15 seconds). Then Grafana at `http://localhost:3000`, dashboard **Fleet pipeline health**.

**Say (on the Prometheus page):**

> "Last, monitoring. Prometheus has fifteen alert rules. Most of them check how old the data is, not how many errors there are. This is because a stopped pipeline gives no errors. The numbers just stop changing."

**Say (on the Grafana page):**

> "Grafana shows the health of the pipeline. We can see the data coming in, Spark processing it, the daily batch, and the API."

> "So we showed the full pipeline: Kafka, Spark, Airflow, the database, the API and monitoring. Thank you."

## If a page looks empty during the demo

- **No daily report:** Check `/api/reports` for an available date. Airflow must show a successful `announce` task for that date.
- **Airflow run is still running:** Wait for its tasks to finish, then refresh `/api/reports`.
- **No live vehicles or windows:** Give the stream a little time to warm up, then refresh the fleet dashboard.
- **Grafana asks for a dashboard:** Open **Fleet pipeline health** from the dashboards list.

Do not delete volumes or reset the simulated clock during the presentation.
