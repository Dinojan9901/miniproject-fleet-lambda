"""The vehicle roster, generated deterministically from a fixed seed.

Both simulated sources build the roster from this module. That is what makes the
two feeds *about the same fleet*: the telemetry simulator drives vehicle
FLEET-003 around, and the fuel partner's daily file bills for FLEET-003 against
the same baseline duty cycle. Without a shared roster the daily reconciliation
would be joining unrelated random numbers.

Deterministic seeding also means a demo can be re-run and produce a comparable
report, which matters when the marking asks you to reproduce results.
"""
from __future__ import annotations

import random
from dataclasses import dataclass

ROSTER_SEED = 20260801

MODELS = ["Toyota Aqua", "Suzuki Wagon R", "Honda Fit", "Toyota Prius", "Suzuki Alto"]


@dataclass(frozen=True)
class Vehicle:
    vehicle_id: str
    driver_id: str
    model: str
    baseline_km_per_day: float      # expected distance on a normal simulated day
    fuel_efficiency_km_per_l: float
    duty_cycle: float               # fraction of the simulated day spent on trips


def build_fleet(size: int, seed: int = ROSTER_SEED) -> list[Vehicle]:
    rng = random.Random(seed)
    fleet: list[Vehicle] = []
    for i in range(1, size + 1):
        fleet.append(
            Vehicle(
                vehicle_id=f"FLEET-{i:03d}",
                driver_id=f"DRV-{i:03d}",
                model=rng.choice(MODELS),
                baseline_km_per_day=round(rng.uniform(90.0, 210.0), 1),
                fuel_efficiency_km_per_l=round(rng.uniform(11.0, 22.0), 1),
                duty_cycle=round(rng.uniform(0.35, 0.75), 3),
            )
        )
    return fleet


def fleet_index(size: int, seed: int = ROSTER_SEED) -> dict[str, Vehicle]:
    return {v.vehicle_id: v for v in build_fleet(size, seed)}
