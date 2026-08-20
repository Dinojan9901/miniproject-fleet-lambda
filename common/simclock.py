"""The simulated clock. One simulated day = SIM_DAY_SECONDS of real time (default 300s).

Why this exists: the assignment requires a source that delivers a file "once per
day". Waiting 24 real hours to see a second batch run is not demonstrable, so
time is compressed by a factor of 86400 / SIM_DAY_SECONDS = 288x at the default
setting. Everything downstream -- windowing, watermarks, the Airflow schedule,
the daily report -- is expressed in *simulated* time and therefore keeps its
real-world meaning; only the wall-clock rate changes.

    real second  ->  288 simulated seconds
    real 5 min   ->  1 simulated day
    real 12.5 s  ->  1 simulated hour

The anchor problem: every container must agree on where simulated time starts,
or the producer and the batch source disagree about what "today" is. The first
process to start materialises the anchor into `<data_dir>/_sim_anchor.json`
(created atomically with O_EXCL) and every other process reads it. The file is
also the thing you inspect when a demo looks out of sync, and deleting it resets
the simulated calendar.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ANCHOR_FILENAME = "_sim_anchor.json"


@dataclass(frozen=True)
class SimClock:
    real_anchor: float          # unix seconds when simulated time started
    sim_epoch: datetime         # simulated instant corresponding to real_anchor
    day_seconds: int            # real seconds per simulated day

    @property
    def scale(self) -> float:
        """Simulated seconds elapsed per real second."""
        return 86400.0 / self.day_seconds

    def now(self, real_now: float | None = None) -> datetime:
        """Current simulated instant."""
        real_now = time.time() if real_now is None else real_now
        return self.sim_epoch + timedelta(seconds=(real_now - self.real_anchor) * self.scale)

    def sim_date(self, real_now: float | None = None) -> date:
        return self.now(real_now).date()

    def day_index(self, real_now: float | None = None) -> int:
        """How many complete simulated days have elapsed since the epoch."""
        real_now = time.time() if real_now is None else real_now
        return int((real_now - self.real_anchor) // self.day_seconds)

    def real_seconds_until_next_day(self, real_now: float | None = None) -> float:
        real_now = time.time() if real_now is None else real_now
        elapsed = real_now - self.real_anchor
        return self.day_seconds - (elapsed % self.day_seconds)

    def describe(self, real_now: float | None = None) -> dict:
        return {
            "sim_now": self.now(real_now).isoformat(timespec="seconds"),
            "sim_date": self.sim_date(real_now).isoformat(),
            "sim_day_index": self.day_index(real_now),
            "day_seconds": self.day_seconds,
            "compression": f"{self.scale:.0f}x",
        }

    # -- construction ------------------------------------------------------

    @classmethod
    def shared(cls, data_dir: str | Path, day_seconds: int, epoch_date: str,
               wait_seconds: float = 30.0) -> "SimClock":
        """Read the shared anchor, creating it if this is the first process up.

        O_CREAT|O_EXCL makes creation atomic, so if several containers start at
        once exactly one writes the file and the rest read what it wrote.
        """
        path = Path(data_dir) / ANCHOR_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "real_anchor": time.time(),
            "sim_epoch": f"{epoch_date}T00:00:00+00:00",
            "day_seconds": day_seconds,
        }
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
        except FileExistsError:
            payload = _read_with_retry(path, wait_seconds)

        return cls(
            real_anchor=float(payload["real_anchor"]),
            sim_epoch=datetime.fromisoformat(payload["sim_epoch"]).astimezone(timezone.utc),
            day_seconds=int(payload["day_seconds"]),
        )

    @classmethod
    def fixed(cls, real_anchor: float, epoch_date: str = "2026-08-01", day_seconds: int = 300) -> "SimClock":
        """A clock with an explicit anchor -- used by the tests."""
        return cls(
            real_anchor=real_anchor,
            sim_epoch=datetime.fromisoformat(f"{epoch_date}T00:00:00+00:00"),
            day_seconds=day_seconds,
        )


def _read_with_retry(path: Path, wait_seconds: float) -> dict:
    """The winner of the create race may not have finished writing yet."""
    deadline = time.time() + wait_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            content = path.read_text(encoding="utf-8")
            if content.strip():
                return json.loads(content)
        except (OSError, json.JSONDecodeError) as exc:  # partial write, keep trying
            last_error = exc
        time.sleep(0.2)
    raise RuntimeError(f"could not read simulated-clock anchor at {path}: {last_error}")
