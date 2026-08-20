"""The simulated clock -- time compression, day boundaries, and the shared anchor.

Every test pins the anchor explicitly, so nothing here depends on wall time.
"""
from datetime import datetime, timezone

import pytest

from common.simclock import SimClock


def test_compression_factor():
    clock = SimClock.fixed(real_anchor=1000.0, day_seconds=300)
    assert clock.scale == pytest.approx(288.0)


def test_one_real_day_second_is_one_simulated_day():
    clock = SimClock.fixed(real_anchor=1000.0, epoch_date="2026-08-01", day_seconds=300)
    assert clock.now(1000.0) == datetime(2026, 8, 1, tzinfo=timezone.utc)
    assert clock.now(1300.0) == datetime(2026, 8, 2, tzinfo=timezone.utc)
    assert clock.now(1600.0) == datetime(2026, 8, 3, tzinfo=timezone.utc)


def test_midday_is_halfway_through_the_real_interval():
    clock = SimClock.fixed(real_anchor=0.0, day_seconds=300)
    assert clock.now(150.0) == datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def test_sim_date_and_day_index_track_boundaries():
    clock = SimClock.fixed(real_anchor=0.0, epoch_date="2026-08-01", day_seconds=300)
    assert clock.sim_date(0.0).isoformat() == "2026-08-01"
    assert clock.day_index(0.0) == 0
    assert clock.sim_date(299.0).isoformat() == "2026-08-01"
    assert clock.sim_date(300.0).isoformat() == "2026-08-02"
    assert clock.day_index(300.0) == 1
    assert clock.day_index(901.0) == 3


def test_seconds_until_next_day_counts_down_then_resets():
    clock = SimClock.fixed(real_anchor=0.0, day_seconds=300)
    assert clock.real_seconds_until_next_day(0.0) == pytest.approx(300.0)
    assert clock.real_seconds_until_next_day(120.0) == pytest.approx(180.0)
    assert clock.real_seconds_until_next_day(299.0) == pytest.approx(1.0)
    assert clock.real_seconds_until_next_day(300.0) == pytest.approx(300.0)


def test_shared_anchor_is_created_once_and_reused(tmp_path):
    """The second process must adopt the first process's anchor, not make its own.

    This is what keeps the telemetry producer and the expense source agreeing on
    which simulated day it is.
    """
    first = SimClock.shared(tmp_path, day_seconds=300, epoch_date="2026-08-01")
    second = SimClock.shared(tmp_path, day_seconds=300, epoch_date="2026-08-01")

    assert (tmp_path / "_sim_anchor.json").exists()
    assert first.real_anchor == second.real_anchor
    assert first.sim_epoch == second.sim_epoch
    assert first.day_seconds == second.day_seconds


def test_shared_anchor_ignores_a_later_disagreement_about_settings(tmp_path):
    """First writer wins: a container started with different settings must not
    silently shift the calendar for everyone else."""
    first = SimClock.shared(tmp_path, day_seconds=300, epoch_date="2026-08-01")
    second = SimClock.shared(tmp_path, day_seconds=60, epoch_date="2030-01-01")
    assert second.day_seconds == first.day_seconds == 300
    assert second.sim_epoch == first.sim_epoch


def test_describe_is_log_friendly():
    clock = SimClock.fixed(real_anchor=0.0, day_seconds=300)
    described = clock.describe(real_now=450.0)
    assert described["compression"] == "288x"
    assert described["sim_date"] == "2026-08-02"
    assert described["sim_day_index"] == 1
    assert set(described) == {"sim_now", "sim_date", "sim_day_index", "day_seconds", "compression"}
