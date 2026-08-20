"""Structured logging: one JSON object per line, on every component.

Uniform field names across the simulators, Spark jobs, Airflow tasks and the API
are what make the pipeline traceable end to end -- `docker compose logs | grep
trip_id` follows one trip through ingestion, processing and serving.

Standard fields: ts, level, component, stage, event, plus whatever the call site
passes as `extra`. `stage` is one of ingest | process | store | serve | orchestrate,
so logs can be sliced by pipeline layer regardless of which process produced them.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_RESERVED = set(
    logging.LogRecord(name="", level=0, pathname="", lineno=0, msg="", args=(), exc_info=None).__dict__
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, component: str, stage: str) -> None:
        super().__init__()
        self.component = component
        self.stage = stage

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "component": self.component,
            "stage": getattr(record, "stage", self.stage),
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and key != "stage":
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure(component: str, stage: str, level: str = "INFO") -> logging.Logger:
    """Install the JSON formatter on the root logger and return a named logger."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(component, stage))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    # py4j is extremely chatty at DEBUG and drowns out the pipeline's own logs.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)

    return logging.getLogger(component)
