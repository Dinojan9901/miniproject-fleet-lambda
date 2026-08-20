"""Wire and file formats for the two ingestion paths.

Deliberately expressed as Spark DDL strings rather than pyspark StructType
objects: the simulators and the API import this module and must not drag PySpark
(and a JVM) into their images. The Spark jobs call
`StructType.fromDDL(TELEMETRY_DDL)` / pass the DDL straight to `from_json`.

Format choice, justified in the report: JSON on the Kafka wire, Parquet in the
lake. JSON keeps the simulated producer trivially inspectable
(`kafka-console-consumer` shows readable events during a demo) at the cost of
size and of a schema that is only enforced on read; Parquet gives the batch layer
columnar scans, predicate pushdown and a typed schema where it actually pays.
Avro + Schema Registry -- as built for the Chapter 3 assignment -- is the
production answer for the wire format and is discussed as such.
"""
from __future__ import annotations

# --- Streaming source: vehicle telemetry ---------------------------------
#
# status is one of: idle | enroute | on_trip
#   idle    - available, not assigned
#   enroute - assigned, driving to the pickup point
#   on_trip - passenger on board, fare accruing
#
# fare is the *cumulative* fare of the current trip; fare_increment is what
# accrued since this vehicle's previous event. The speed layer sums increments
# (so a live earnings figure exists mid-trip); the batch layer takes the final
# cumulative fare of each completed trip as the authoritative revenue. The two
# figures disagree slightly, which is precisely the speed-vs-batch distinction
# Lambda exists to manage.
TELEMETRY_DDL = (
    # Correlation id, minted at the source. It survives into the Parquet master
    # dataset and into any quarantine record, so a single event can be followed
    # across ingestion, processing and storage with one grep. This is the
    # project's tracing primitive -- see the report's observability section.
    "event_id STRING, "
    "trip_id STRING, "
    "driver_id STRING, "
    "vehicle_id STRING, "
    "lat DOUBLE, "
    "lon DOUBLE, "
    "speed_kmh DOUBLE, "
    "status STRING, "
    "fare DOUBLE, "
    "fare_increment DOUBLE, "
    "event_time STRING, "      # ISO-8601, simulated clock -- the event-time axis
    "ingest_time STRING, "     # ISO-8601, real wall clock -- for pipeline latency
    "sim_date STRING, "        # simulated calendar date, the lake partition key
    "seq BIGINT"               # per-vehicle sequence number, for gap detection
)

TELEMETRY_FIELDS = [f.strip().split(" ")[0] for f in TELEMETRY_DDL.split(",")]

VALID_STATUSES = {"idle", "enroute", "on_trip"}

# --- Daily batch source: fuel partner / garage expense file ---------------
EXPENSE_COLUMNS = [
    "vehicle_id",
    "fuel_cost",
    "maintenance_cost",
    "distance_covered",
    "service_flag",
    "sim_date",
]

EXPENSE_DDL = (
    "vehicle_id STRING, "
    "fuel_cost DOUBLE, "
    "maintenance_cost DOUBLE, "
    "distance_covered DOUBLE, "
    "service_flag STRING, "
    "sim_date STRING"
)

CURRENCY = "LKR"
