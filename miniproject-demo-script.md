# Mini-Project Demo Script — Fleet Operations Lambda Pipeline

**EC8203 Applied Big Data Engineering — Mini Project (25%)**
Target length: **~8:30** (the brief allows 5–10 minutes)

> **Note:** this script reflects the stack as run during demo preparation. Do a
> short readiness check before recording, especially the latest Airflow batch.

---

## Prep (off camera)

```powershell
# From the project root. Build the two local app images before starting.
docker compose build
docker compose up -d
docker compose ps

# Check that Kafka topics were created (one-shot init containers exit on success).
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list

# Check the API and monitoring. The API's host port is 18000.
python scripts/smoke_check.py --api http://localhost:18000

# Optional: run the project tests inside the already-built app container.
docker compose exec api python -m pytest
```

The DAG is scheduled every five minutes; to trigger a run immediately:

```powershell
docker compose exec airflow-scheduler airflow dags trigger fleet_daily_batch
```

`queued` means Airflow accepted the run and is waiting to schedule it. In the
Grid/Graph view, follow that run through `announce`; old failed runs stay red in
history even after a later run succeeds.

Allow time for warm-up. Live vehicle state and streaming windows appear first;
the first daily expense reconciliation needs a matching telemetry day and can
take at least five real minutes on a fresh run. Record only after Airflow shows
a successful `announce` task and `/api/reports` lists a date.

The `data-init` one-shot service prepares the shared folders and Airflow write
permissions automatically. On a stack that was already running before this
service was added, recreate the initializers once:

```powershell
docker compose up -d --force-recreate data-init kafka-init airflow-init
docker compose up -d
```

### Windows to prepare

| Window | Purpose |
|---|---|
| Terminal 1 | Commands |
| Terminal 2 | Spare, for log tails |
| Browser tab 1 | <http://localhost:18000> — dashboard |
| Browser tab 2 | <http://localhost:18000/docs> — API docs |
| Browser tab 3 | <http://localhost:8088> — Airflow (admin / admin, if initialized) |
| Browser tab 4 | <http://localhost:3000> — Grafana |
| Browser tab 5 | <http://localhost:9090/alerts> — Prometheus |

Preload every tab. Grafana and Airflow are slow on first load.

### Requirements

- Docker Desktop with **~8 GB RAM** allocated (15 services, including one-shot initializers)
- Free ports: 18000, 8088, 3000, 9090, 9091, 4040, 5432, 29092
- Kafka and ZooKeeper use `confluentinc/cp-*:7.6.1`; Kafka is single-broker

---

## 0:00 – 0:45 · Use case and architecture

**Show:** the README architecture diagram

> "Ride-hailing fleet operations. Two sources: continuous GPS telemetry, and a
> vehicle expense file that arrives once a day from the fuel partner. The business
> question has two halves — what is fleet utilisation right now, and which vehicles
> became unprofitable once yesterday's costs are applied."

> "Those halves have different requirements. Dispatch needs an answer in seconds and
> can tolerate approximation. Finance needs an exact figure that can be restated if
> the partner reissues an invoice. That is why this is **Lambda**, not Kappa."

---

## 0:45 – 1:15 · Simulated clock

**Show:** Terminal 1

```powershell
docker compose exec api cat /data/_sim_anchor.json
```

> "Time is compressed 288 times. One simulated day is five real minutes, so a
> simulated hour passes every 12.5 seconds. Windowing, watermarks and the Airflow
> schedule are all expressed in simulated time, so they keep their real-world
> meaning — only the wall-clock rate changes."

---

## 1:15 – 2:15 · Ingestion, both sources

**Show:** Terminal 1

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker compose logs --tail 15 telemetry-producer
docker compose logs --tail 10 expense-source
docker compose exec expense-source ls -la /data/landing/expenses
```

> "Twelve vehicles emitting telemetry into Kafka — three partitions keyed by vehicle,
> so each vehicle's events stay ordered. Around 4% are deliberately malformed,
> because real telemetry from cheap in-vehicle units is not clean."

> "Separately, the expense source drops one CSV per simulated day. It is published
> atomically — written to a temp name, then renamed — so Airflow's sensor can never
> read a half-written file."

---

## 2:15 – 3:30 · Speed layer

**Show:** Terminal 1

```powershell
docker compose logs --tail 20 speed-layer
```

> "Spark Structured Streaming, two queries. Each micro-batch reports events
> processed, how many passed validation, how many were quarantined, vehicles
> updated, and alerts raised."

**Show:** <http://localhost:4040> — the Spark UI, both streaming queries

Use the **Structured Streaming** tab for query progress. Repeated
`KafkaDataConsumer` interrupt warnings can appear; if `micro_batch_complete`
continues, the stream is processing. The producer's `anomalous_event_emitted`
warnings are deliberate test data, not producer failures.

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic fleet.telemetry.quarantine --max-messages 2
```

> "Rejected events are not dropped. They go to a quarantine topic with the exact
> rules they broke, and they are still written to the Parquet master dataset — so
> the batch layer can revise that decision later if a rule turns out to be wrong."

---

## 3:30 – 4:30 · Live dashboard, the speed view

**Show:** <http://localhost:18000>

> "Utilisation, earnings per simulated hour, the per-zone breakdown, and idle alerts
> appearing as vehicles cross the threshold."

> "Everything in the top half is the speed view — seconds old, and explicitly
> labelled approximate. Every API response says which layer produced it."

---

## 4:30 – 5:45 · Batch layer, via Airflow

**Show:** <http://localhost:8088> → DAG `fleet_daily_batch`

> "Runs every five real minutes — one simulated day. The DAG looks for an
> unprocessed expense file with a matching Parquet telemetry partition. This lets
> it move past old expense-only files left from startup, while keeping those CSVs
> available. Writes are idempotent, so a completed day can be rerun safely."

**Show:** the task graph

```
find_pending_day → wait_for_master_dataset → run_batch_layer → validate_output → mark_processed → announce
```

> "`validate_output` is a real quality gate. If the numbers are implausible — no
> vehicles, negative revenue, null profits, or a missing HTML report — the day
> stays unmarked and the run fails rather than publishing a bad result."

**Show:** a successful run. `announce` green means the batch, output validation,
and processed marker all completed. Older failed runs remain red in history.

---

## 5:45 – 6:30 · The reconciliation report

**Show:** `http://localhost:18000/api/reports` first; copy a `sim_date` from the
response, then open `http://localhost:18000/api/reports/daily/<sim_date>/html`.

> "Per vehicle: revenue derived from the telemetry, GPS-derived distance computed
> with haversine over consecutive fixes, the distance the partner billed, and the
> variance between them. Then cost, profit and margin."

> "Vehicles are flagged unprofitable below a 10% margin, or distance-disputed when
> the billed distance differs from the GPS track by more than 20%."

---

## 6:30 – 7:15 · The Lambda gap

**Show:** Terminal 1

```powershell
$simDate = "2026-09-06"  # replace with a date listed by /api/reports
curl.exe "http://localhost:18000/api/lambda/compare/$simDate"
```

> "This is the architectural argument turned into a number. The speed layer sums
> windowed fare increments under a watermark, using approximate counts. The batch
> layer sums completed-trip revenue from the master dataset. The difference
> quantifies the trade-off; trip counts are not directly comparable because speed
> counts are aggregated across zones and windows."

---

## 7:15 – 8:15 · Observability

**Show:** <http://localhost:3000> → dashboard *Fleet pipeline health*

> "Ingestion rate and freshness, quarantine ratio, micro-batch duration, ingest lag,
> batch rows and reconciled profit, API rate and p95 latency, and the age of each of
> the two Lambda views."

**Break it on purpose:**

```powershell
docker compose stop telemetry-producer
```

**Show:** <http://localhost:9090/alerts>

> "Within a minute, `NoTelemetryIngested` and `TelemetryProducerDown` fire. Most of
> the fifteen rules alert on the **age of the output** rather than on error counts —
> because that is how data pipelines actually fail. No errors, no exceptions, just
> numbers that quietly stop moving."

```powershell
docker compose start telemetry-producer
```

---

## 8:15 – 8:30 · Close

> "Fifteen alert rules, structured JSON logs across every stage, correlation IDs
> that follow one event from the producer into the Parquet lake, and 76 unit tests.
> The code and the written report are on GitHub."

---

## Cheat sheet

| Time | Window | Action |
|---|---|---|
| 0:00 | README | Architecture diagram |
| 0:45 | Terminal | `docker compose exec api cat /data/_sim_anchor.json` |
| 1:15 | Terminal | Kafka topics, producer and expense logs |
| 2:15 | Terminal, Spark UI | `docker compose logs speed-layer`, then port 4040 |
| 3:30 | Browser :18000 | Live dashboard |
| 4:30 | Browser :8088 | Airflow DAG |
| 5:45 | Browser :18000 | `/api/reports/daily/<sim_date>/html` |
| 6:30 | Terminal | `/api/lambda/compare/<date>` |
| 7:15 | Browser :3000, :9090 | Grafana, stop producer, show alerts |
| 8:15 | — | Close |

---

## If the batch is not ready

- Check `docker compose ps` and `docker compose logs --tail 50 airflow-scheduler`.
- In Airflow, a successful run has all six tasks green; `announce` is last.
- `wait_for_master_dataset` may show `up_for_reschedule` while it checks every
  15 seconds. If it times out, inspect the selected date in its log. The DAG now
  selects an unprocessed expense CSV only when that date has a Parquet partition.
- If `find_pending_day` or `run_batch_layer` reports `PermissionError`, repair the
  shared volume directories using the command in Prep, then trigger a new DAG run.
- If telemetry is missing, check that `fleet.telemetry`,
  `fleet.telemetry.quarantine`, and `fleet.alerts` exist, then look for
  `micro_batch_complete` in the speed-layer logs.
- Run host-side checks with `python scripts/smoke_check.py --api
  http://localhost:18000`. `PENDING` means simulated data/time has not arrived yet;
  `FAIL` means inspect the related service log.

---

## Things that will cost you a retake

1. **Recording before a successful batch.** Confirm `announce` is green and use
   a date returned by `/api/reports`; do not guess the date.
2. **Another stack using ports.** This project needs its listed host ports,
   especially Kafka 29092 and dashboard 18000.
3. **Cold browser tabs.** Grafana and Airflow take 20–40 seconds on first load.
4. **Too little RAM.** 15 services want ~8 GB; below that, containers get killed
   mid-demo.
5. **Deleting persistent demo data.** `docker compose down -v` removes expense
   files, reports, database contents, and the simulated clock. Use `docker compose
   down` to stop the stack while keeping its data.
6. **Forgetting the simulated clock.** The brief explicitly requires declaring any
   time compression, so state it on camera.
