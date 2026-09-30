"""Single source of configuration for every component.

Nothing else in the project reads os.environ directly, so `SETTINGS` is the
complete description of how a run is wired. Defaults are the docker-compose
values; the host-side scripts override the two hostnames via .env.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _s(name: str, default: str) -> str:
    return os.getenv(name, default)


def _i(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _f(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Settings:
    # --- Kafka ------------------------------------------------------------
    bootstrap_servers: str
    telemetry_topic: str
    quarantine_topic: str
    alerts_topic: str
    topic_partitions: int

    # --- Storage ----------------------------------------------------------
    data_dir: str            # root of the shared volume
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    # --- Simulated clock --------------------------------------------------
    sim_day_seconds: int     # real seconds that make up one simulated day
    sim_epoch_date: str      # calendar date that simulated day 0 represents
    sim_timezone: str        # timezone used for simulated time and local display

    # --- Simulation shape -------------------------------------------------
    fleet_size: int
    event_interval_seconds: float
    anomaly_rate: float

    # --- Pipeline behaviour ----------------------------------------------
    window_minutes: int          # zone aggregation window, in simulated minutes
    watermark_minutes: int       # lateness tolerated, in simulated minutes
    idle_alert_minutes: int      # simulated idle minutes before an alert
    low_margin_threshold: float  # profit margin below which a vehicle is flagged

    # --- Observability ----------------------------------------------------
    pushgateway_url: str
    metrics_port_producer: int
    metrics_port_batch_source: int
    metrics_port_speed: int
    log_level: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def sim_scale(self) -> float:
        """How many simulated seconds pass per real second."""
        return 86400.0 / self.sim_day_seconds

    def psycopg2_kwargs(self) -> dict:
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password,
        }

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=_s("BOOTSTRAP_SERVERS", "kafka:9092"),
            telemetry_topic=_s("TELEMETRY_TOPIC", "fleet.telemetry"),
            quarantine_topic=_s("QUARANTINE_TOPIC", "fleet.telemetry.quarantine"),
            alerts_topic=_s("ALERTS_TOPIC", "fleet.alerts"),
            topic_partitions=_i("TOPIC_PARTITIONS", 3),
            data_dir=_s("DATA_DIR", "/data"),
            postgres_host=_s("POSTGRES_HOST", "postgres"),
            postgres_port=_i("POSTGRES_PORT", 5432),
            postgres_db=_s("POSTGRES_DB", "fleet"),
            postgres_user=_s("POSTGRES_USER", "fleet"),
            postgres_password=_s("POSTGRES_PASSWORD", "fleet"),
            sim_day_seconds=_i("SIM_DAY_SECONDS", 300),
            sim_epoch_date=_s("SIM_EPOCH_DATE", "2026-08-01"),
            sim_timezone=_s("SIM_TIMEZONE", "Asia/Colombo"),
            fleet_size=_i("FLEET_SIZE", 12),
            event_interval_seconds=_f("EVENT_INTERVAL_SECONDS", 2.0),
            anomaly_rate=_f("ANOMALY_RATE", 0.04),
            window_minutes=_i("WINDOW_MINUTES", 60),
            watermark_minutes=_i("WATERMARK_MINUTES", 30),
            idle_alert_minutes=_i("IDLE_ALERT_MINUTES", 45),
            low_margin_threshold=_f("LOW_MARGIN_THRESHOLD", 0.10),
            pushgateway_url=_s("PUSHGATEWAY_URL", "http://pushgateway:9091"),
            metrics_port_producer=_i("METRICS_PORT_PRODUCER", 8001),
            metrics_port_batch_source=_i("METRICS_PORT_BATCH_SOURCE", 8003),
            metrics_port_speed=_i("METRICS_PORT_SPEED", 8002),
            log_level=_s("LOG_LEVEL", "INFO"),
        )


SETTINGS = Settings.from_env()
