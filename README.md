# Real-Time Fleet Operations Platform — a Lambda Architecture

**EC8202 / EC8203 Applied Big Data Engineering — Mini Project (25%)**
Use case 1: *Ride-hailing fleet operations.*

A complete, runnable data platform that answers one question a fleet operator
actually has:

> **Which vehicles are earning their keep right now, and which ones stopped being
> worth running yesterday?**

Those are two different questions with two different tolerances, which is why
this is a **Lambda architecture**. The first needs an answer in seconds and can
be approximate. The second decides whether a vehicle stays on the road, must
reconcile against an external cost file that only arrives once a day, and has to
be exactly right. A speed layer serves the first, a batch layer serves the
second, and the serving layer presents both while keeping them distinguishable.

---

## Contents

- [What is running](#what-is-running)
- [Architecture](#architecture)
- [Quick start](#quick-start)
- [The five-minute demo](#the-five-minute-demo)
- [Repository layout](#repository-layout)
- [The simulated clock](#the-simulated-clock)
- [Layer by layer](#layer-by-layer)
- [Observability](#observability)
- [Configuration](#configuration)
- [Tests](#tests)
- [Troubleshooting](#troubleshooting)
- [Limitations](#limitations)

---

## What is running

| URL | What it is |
|---|---|
| <http://localhost:8000> | **Live fleet dashboard** — the business view, both layers |
| <http://localhost:8000/docs> | API reference (OpenAPI) |
| <http://localhost:8088> | **Airflow** — the daily reconciliation DAG (`admin` / `admin`) |
| <http://localhost:3000> | **Grafana** — pipeline health dashboard (anonymous access) |
| <http://localhost:9090> | Prometheus — targets, metrics, alert rules |
| <http://localhost:4040> | Spark UI — streaming query progress |
| `localhost:29092` | Kafka |
| `localhost:5432` | Postgres (`fleet` / `fleet` / `fleet`) |

---

## Architecture

```
   SOURCES                    INGESTION            PROCESSING                    STORAGE                SERVING
 ─────────────             ───────────────    ─────────────────────      ──────────────────────    ─────────────────

 telemetry                                   ┌──────────────────────┐
 simulator    ──JSON──▶  Kafka               │  SPEED LAYER         │   ┌────────────────────┐
 12 vehicles             fleet.telemetry ───▶│  Spark Structured    │──▶│ Parquet lake       │
 ~6 events/s             3 partitions        │  Streaming           │   │ master dataset     │
 4% malformed            keyed by vehicle    │                      │   │ sim_date=…         │──┐
                                             │  validate → enrich   │   └────────────────────┘  │
                              ▲              │  → window → alert    │                           │
                              │              │                      │   ┌────────────────────┐  │
                        fleet.telemetry      │                      │──▶│ Postgres  rt_*     │  │
                        .quarantine   ◀──────│  rejected rows       │   │ live views         │  │
                                             └──────────────────────┘   └────────────────────┘  │
                                                                                   ▲            │
 expense                                     ┌──────────────────────┐              │            │
 partner      ──CSV───▶  landing/            │  BATCH LAYER         │              │            │
 1 file per              expenses_<date>.csv │  Spark batch, run by │◀─────────────┼────────────┘
 simulated day           (atomic rename)  ──▶│  Airflow every       │              │
 some rows missing                           │  simulated day       │   ┌────────────────────┐
 some distances disputed                     │                      │──▶│ Postgres  daily_*  │
                                             │  recompute exactly,  │   │ + CSV + HTML report│
                                             │  join, reconcile     │   └────────────────────┘
                                             └──────────────────────┘              │
                                                                                   ▼
                                                                        ┌──────────────────────┐
                                                                        │ FastAPI serving layer│
                                                                        │ merges both views,   │
                                                                        │ labels which is which│
                                                                        │  + live dashboard    │
                                                                        └──────────────────────┘

 OBSERVABILITY  ── JSON logs from every component ─┐
                ── /metrics scraped by Prometheus ─┼──▶ Grafana + 16 alert rules
                ── batch metrics pushed to Pushgateway ─┘
```

### Why Lambda and not Kappa

Kappa is the simpler architecture and the default answer for a stream-first
system, so it needs to be argued down rather than ignored. Three properties of
*this* problem decide it:

1. **One of the two sources is not a stream.** The fuel partner delivers a file
   once a day. Under Kappa that file would have to be shredded into synthetic
   events and replayed through the same pipeline — machinery whose only purpose
   is to make a batch source look like something it is not.
2. **The authoritative answer needs the whole day, not a window.** Revenue is the
   final fare of each *completed* trip and cost only exists after the invoice
   arrives. A streaming aggregate over a watermarked window is structurally
   incapable of being the number a vehicle is retired on.
3. **The two answers have different correctness contracts.** The live view may
   be approximate — it uses HyperLogLog distinct counts and drops late events
   past the watermark, and that is *fine* for a dispatcher. The daily view may
   not. Lambda makes that difference explicit; Kappa hides it behind one
   pipeline and a reprocessing story.

The classic objection to Lambda — maintaining the same logic twice — is real and
is mitigated here rather than denied: validation, zone assignment and the
economic thresholds live in `common/` and `batch_layer/profitability.py` and are
imported by both layers, so business rules exist once even though the execution
paths differ. `/api/lambda/compare/<date>` quantifies the residual gap between
the two views instead of pretending there is none.

Full justification, with the rejected alternatives, is in
[`report/report.pdf`](report/report.pdf).

---

## Quick start

**Requirements:** Docker Desktop with ~8 GB of RAM available, and ports
8000, 8088, 3000, 9090, 9091, 4040, 29092, 5432 free.

> If you ran the Chapter 3 Kafka assignment, stop it first — it binds the same
> Kafka ports: `cd ../chapter3-kafka-orders && docker compose down`.

```powershell
cd miniproject-fleet-lambda

# First run pulls and builds ~2 GB of images; allow 5-10 minutes.
docker compose build
docker compose up -d

# Watch everything come up (the speed layer is last, after Kafka is healthy).
docker compose ps
docker compose logs -f speed-layer
```

Then, from the host:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python scripts/smoke_check.py      # is every layer alive?
```

Open <http://localhost:8000>. Vehicle state appears within ~15 seconds; the
first windowed metrics after ~30 seconds; the first **batch reconciliation after
about five minutes**, which is one simulated day.

Shut down with `docker compose down`, or `docker compose down -v` to also discard
the lake, the database and the simulated calendar.

---

## The five-minute demo

1. **The sources.** `docker compose logs --tail 20 telemetry-producer` — one JSON
   line per event, plus `anomalous_event_emitted` for the deliberately broken
   ones. Then `docker compose exec expense-source ls -la /data/landing/expenses`
   for the daily file drop.

2. **Ingestion is real Kafka**, not an in-process queue:
   ```powershell
   docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic fleet.telemetry
   docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic fleet.telemetry --max-messages 3
   ```

3. **The speed layer, working.** `docker compose logs -f speed-layer` shows one
   `micro_batch_complete` line per trigger with `events`, `clean`, `quarantined`,
   `vehicles_updated` and `ingest_lag_seconds`. The Spark UI at
   <http://localhost:4040> shows both streaming queries and their watermarks.

4. **Validation and quarantine.** The malformed events are rejected, not dropped
   silently:
   ```powershell
   docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic fleet.telemetry.quarantine --max-messages 3
   ```
   Each carries a `violations` array naming the exact rules it broke.

5. **The live dashboard** at <http://localhost:8000> — utilisation, earnings per
   simulated hour, zone breakdown, and idle alerts appearing as vehicles cross
   the threshold.

6. **The batch layer.** In Airflow (<http://localhost:8088>) open
   `fleet_daily_batch`: `find_pending_day → wait_for_master_dataset →
   run_batch_layer → validate_output → mark_processed → announce`. Ticks with no
   new file end as *skipped*, which is the intended behaviour.

7. **The authoritative report.** `/api/reports/daily/<sim_date>/html` — per
   vehicle: revenue, GPS distance, billed distance, variance, cost, profit,
   margin, and flags for unprofitable / disputed / missing expense data.

8. **The Lambda gap, quantified.** `/api/lambda/compare/<sim_date>` puts the
   speed layer's windowed earnings next to the batch layer's exact revenue and
   reports the difference — the trade-off the architecture was chosen for,
   as a number.

9. **Observability.** Grafana (<http://localhost:3000>) → *Fleet pipeline
   health*. Then break something on purpose:
   ```powershell
   docker compose stop telemetry-producer
   ```
   Within ~60 seconds `NoTelemetryIngested` and `TelemetryProducerDown` fire at
   <http://localhost:9090/alerts>, and the dashboard's freshness badge turns red.
   `docker compose start telemetry-producer` recovers it.

---

## Repository layout

```
miniproject-fleet-lambda/
├── docker-compose.yml          14 services: the whole platform
├── docker/
│   ├── app/Dockerfile          simulators + Spark + API (Kafka jars baked in)
│   └── airflow/Dockerfile      Airflow + JVM + PySpark
├── common/                     shared by every layer -- imported, not duplicated
│   ├── config.py               all settings; the only reader of os.environ
│   ├── logging_setup.py        structured JSON logging
│   ├── simclock.py             the shared simulated clock
│   ├── geo.py                  operating zones + haversine
│   ├── fleet.py                the deterministic vehicle roster
│   ├── schemas.py              telemetry and expense-file schemas
│   ├── validation.py           data-quality rules (used by the Spark UDF)
│   └── db.py                   idempotent upserts into the serving store
├── simulators/
│   ├── telemetry_producer.py   streaming source -> Kafka
│   └── expense_batch_source.py daily file drop (atomic publish)
├── speed_layer/stream_job.py   Structured Streaming: 2 queries, 5 jobs
├── batch_layer/
│   ├── daily_reconciliation.py Spark batch: the authoritative day
│   ├── profitability.py        the economics + thresholds (unit-tested)
│   └── report_render.py        CSV + standalone HTML report
├── serving/
│   ├── app.py                  FastAPI: 12 endpoints, both views labelled
│   ├── db.py                   pooled reads
│   └── templates/dashboard.html   the live dashboard
├── airflow/dags/fleet_daily_batch.py
├── sql/                        serving-store schema, applied on first start
├── observability/              Prometheus config, 16 alert rules, Grafana
├── scripts/
│   ├── smoke_check.py          "is every layer alive?"
│   └── fetch_reports.ps1       copy evidence out of the Docker volume
├── tests/                      76 tests, no broker or Spark required
└── report/report.tex           the written report -> report.pdf
```

---

## The simulated clock

The assignment needs a source that delivers "once per day". Waiting 24 real
hours to see a second batch run is not demonstrable, so **one simulated day is
compressed into 5 real minutes** (288×, configurable via `SIM_DAY_SECONDS`):

| real time | simulated time |
|---|---|
| 1 second | 4.8 minutes |
| 12.5 seconds | 1 hour — one windowed aggregation |
| 5 minutes | 1 day — one expense file, one Airflow reconciliation |

Everything downstream is expressed in *simulated* time — window size, watermark,
idle-alert threshold, report dates — so every number keeps its real-world
meaning. Only the wall-clock rate changes.

All components must agree on where the calendar starts, so the first process to
boot writes `/data/_sim_anchor.json` (created atomically with `O_EXCL`) and
everything else reads it. Inspect it with
`docker compose exec api cat /data/_sim_anchor.json`; delete it and restart to
reset the simulated calendar.

---

## Layer by layer

### Ingestion

**Streaming source** — `simulators/telemetry_producer.py`. Twelve vehicles, each
running an `idle → enroute → on_trip → idle` state machine, emitting position,
speed, status and accrued fare every 2 real seconds. Events are keyed by
`vehicle_id` so one vehicle's events stay ordered within a partition. About 4% of
events are deliberately malformed — null position, negative speed, unknown
status, a fare accruing while parked — because real telemetry from cheap
in-vehicle units is not clean.

**Batch source** — `simulators/expense_batch_source.py`. One CSV per simulated
day of per-vehicle fuel and maintenance cost. Published atomically (write to
`.tmp`, then `os.replace`) so Airflow's sensor can never read a half-written
file. Some vehicles are missing from the file, and roughly one in ten reports a
distance that disagrees sharply with the GPS track — both are situations the
batch layer has to survive and surface rather than crash on.

Both sources build their vehicle list from the same `common/fleet.py` roster, so
the two feeds genuinely describe the same fleet.

### Speed layer — `speed_layer/stream_job.py`

Two Structured Streaming queries, so a slow sink cannot stall the other's
watermark:

**`ingest-and-state`** (10s trigger) — parse → validate → enrich → four sinks:
appends **every** parsed event to the Parquet master dataset (rejected rows
included: the batch layer must be able to revise a decision the stream made);
republishes rejected rows to `fleet.telemetry.quarantine` with their violation
reasons; upserts each vehicle's latest state into Postgres; and raises idle
alerts.

**`zone-windows`** (15s trigger) — tumbling 60-simulated-minute windows per
operating zone with a 30-simulated-minute watermark, producing active vehicles,
idle ratio, trips and earnings.

Three details worth defending:

- **Approximation is deliberate.** `approx_count_distinct` (HyperLogLog) is used
  for vehicle and trip counts — exact `countDistinct` is not supported in a
  streaming aggregation, and a fast approximate answer is the speed layer's job.
- **Idle state is kept in SQL, not in Spark.** The `idle_since` column is
  maintained by a `CASE` inside the upsert, so an idle *spell* keeps its start
  time across micro-batches. Alerts are then a query, and a unique constraint on
  `(alert_type, vehicle_id, idle_since)` means one alert per spell rather than
  one per micro-batch.
- **Failures retry, then stop loudly.** A micro-batch's side effects are retried
  three times with reconnection; if they still fail the query stops *without*
  committing offsets, so a restart reprocesses rather than skips.

### Batch layer — `batch_layer/daily_reconciliation.py`

Run by Airflow once per simulated day. Recomputes the day from scratch out of
the master dataset:

- **Revenue** = the final cumulative fare of every completed trip
  (`max(fare)` per `trip_id` — order-independent, which matters because the
  master dataset is a union of micro-batches with no global ordering).
- **Distance** = haversine over consecutive GPS fixes per vehicle, with
  implausible jumps (> 25 km between fixes) discarded rather than allowed to
  dominate the day.
- **Utilisation** = share of the vehicle's events not spent idle.
- **Join** — left join to the expense file on `vehicle_id`. A vehicle that drove
  but was not invoiced, and one invoiced but never seen, are both reported.
- **Reconciliation** — profit, margin, revenue per km, and the variance between
  GPS distance and billed distance; vehicles are flagged *unprofitable*
  (margin < 10%) or *distance disputed* (> 20% variance).

Every write is an upsert keyed on `(sim_date, vehicle_id)`, so re-running a day
replaces it instead of doubling it. Outputs land in Postgres, a CSV, and a
standalone HTML report.

### Serving layer — `serving/app.py`

Twelve endpoints over both views. The design rule: **every response says which
layer produced it** (`"view": "speed", "accuracy": "approximate"` versus
`"view": "batch", "accuracy": "authoritative"`), so a reader always knows whether
a number is fresh or correct. `/api/fleet/vehicles/{id}` merges the two, and
`/api/lambda/compare/{date}` measures the gap between them.

### Orchestration — `airflow/dags/fleet_daily_batch.py`

Runs every 5 real minutes. It does **not** key off Airflow's logical date;
it discovers work by finding an expense file with no completion marker. So a
failed run retries the same day, a late file is still processed, and re-running a
day is deliberate and safe. `validate_output` is a real gate — bad numbers leave
the day unmarked and fail the run rather than reaching the dashboard.

---

## Observability

Three signals, each with a specific job:

**Structured logs** — every component emits one JSON object per line with the
same shape (`ts`, `level`, `component`, `stage`, `event`, plus context), so
`docker compose logs | Select-String trip_id` follows one trip through
ingestion, processing and serving.

**Metrics** — every long-running component exposes `/metrics`; the batch layer
pushes to a Pushgateway instead, because a 30-second job cannot be scraped.
Grafana's *Fleet pipeline health* dashboard covers ingestion rate, quarantine
ratio, micro-batch duration, ingest lag, batch rows and API latency.

**Alerts** — 16 rules in `observability/alert_rules.yml`. Most of them fire on
**the age of the output**, not on error counts, because that is how data
pipelines actually fail: a crashed producer, a stalled stream and a batch job
that never ran all look identical from the outside — no errors, just numbers that
quietly stop moving.

---

## Configuration

Every value is read once, in `common/config.py`, and can be overridden by
environment variable or `.env` (copy `.env.example`).

| Variable | Default | Meaning |
|---|---|---|
| `SIM_DAY_SECONDS` | `300` | real seconds per simulated day (288× compression) |
| `SIM_EPOCH_DATE` | `2026-08-01` | calendar date of simulated day 0 |
| `FLEET_SIZE` | `12` | vehicles in the simulated fleet |
| `EVENT_INTERVAL_SECONDS` | `2.0` | real seconds between emission rounds |
| `ANOMALY_RATE` | `0.04` | fraction of telemetry deliberately malformed |
| `WINDOW_MINUTES` | `60` | zone aggregation window, simulated minutes |
| `WATERMARK_MINUTES` | `30` | lateness tolerated, simulated minutes |
| `IDLE_ALERT_MINUTES` | `45` | idle time before an alert, simulated minutes |
| `LOW_MARGIN_THRESHOLD` | `0.10` | margin below which a vehicle is flagged |
| `LOG_LEVEL` | `INFO` | root log level |

---

## Tests

```powershell
python -m pytest
```

**76 tests, no broker, no Spark, no database.** They cover the logic that would
be expensive to debug inside a running pipeline: the simulated clock's
compression and shared anchor, zone assignment across the whole operating area,
haversine against known distances, every validation rule, the profitability
arithmetic and its thresholds, the report renderer, and — importantly — that
**every deliberately corrupted event is actually rejected**, so the quarantine
path cannot silently pass dirty data.

The suite caught three real bugs during development, including a producer that
emitted fare increments on idle events and so manufactured its own quarantine
traffic.

---

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `speed-layer` restarts on boot | Kafka was not ready. It has `restart: unless-stopped` and recovers; check `docker compose logs speed-layer`. |
| Dashboard shows no vehicles | Give it ~15s for the first micro-batch. Then `docker compose logs speed-layer` — look for `micro_batch_complete`. |
| No batch report yet | Expected for the first ~5 minutes (one simulated day). Check the Airflow DAG; *skipped* runs are normal until a file exists. |
| Airflow DAG missing | The scheduler scans every 30s. `docker compose logs airflow-scheduler`. |
| Port already allocated | Another stack is up — most likely `chapter3-kafka-orders`. `docker compose down` there first. |
| Everything is slow | Docker Desktop RAM. This stack wants ~8 GB; raise it in Settings → Resources. |
| Want a faster demo | `SIM_DAY_SECONDS=120` in `.env`, then `docker compose up -d`. |

---

## Limitations

Stated plainly, because knowing what this is *not* is part of the work:

- **Spark runs in local mode**, in the speed-layer container and inside the
  Airflow worker. Nothing about the code changes on a real cluster — the same
  `spark-submit` targets YARN or Kubernetes with a different `--master` — but
  none of the distributed behaviour (shuffle across executors, node loss,
  speculative execution) is exercised here.
- **Single broker, replication factor 1.** `acks=all` therefore means "the one
  replica has it". Realistic durability needs three brokers and `min.insync.replicas=2`.
- **At-least-once, not exactly-once.** A crash between a sink write and the
  offset commit replays that micro-batch. Idempotent upserts keyed on natural
  keys make the *effect* at-most-once in the serving store, but the Parquet
  master dataset can hold duplicate rows after a restart; the batch layer's
  `max(fare)` per trip is deliberately duplicate-insensitive, though its row
  counts are not.
- **The lake has no compaction.** A 10-second trigger writes many small Parquet
  files. Production needs compaction or a table format (Delta, Iceberg) that
  does it — this is the classic small-files problem, simply not solved here.
- **The expense source is correlated by construction, not by observation.** It
  derives plausible costs from the shared roster's baseline duty rather than from
  the actual GPS track, so the reconciliation variance is realistic in shape but
  not a true independent measurement.
- **In-line retries block their partition.** At real volume the standard fix is
  tiered retry topics; a bounded few seconds is fine at this scale.
- **Security is absent by design.** No TLS, no SASL, no API auth, default
  passwords in `docker-compose.yml`. That is acceptable for a local demo and
  would be the first thing to fix anywhere else.
