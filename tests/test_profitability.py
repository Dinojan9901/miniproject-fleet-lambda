"""The economics of the daily reconciliation and its classification thresholds."""
import pytest

from batch_layer.profitability import (
    DISTANCE_DISPUTE_PCT,
    LOW_MARGIN_FRACTION,
    distance_variance_pct,
    is_disputed,
    is_unprofitable,
    margin_pct,
    profit,
    revenue_per_km,
    summarise,
    total_cost,
)


def vehicle(**overrides) -> dict:
    row = {
        "vehicle_id": "FLEET-001", "trips": 8, "revenue": 12000.0, "total_cost": 7000.0,
        "profit": 5000.0, "unprofitable": False, "distance_disputed": False,
        "expense_data_present": True,
    }
    row.update(overrides)
    return row


def test_total_cost_treats_missing_components_as_zero():
    assert total_cost(4000.0, 900.0) == pytest.approx(4900.0)
    assert total_cost(4000.0, None) == pytest.approx(4000.0)
    assert total_cost(None, None) == 0.0


def test_profit_and_margin():
    assert profit(10000.0, 7500.0) == pytest.approx(2500.0)
    assert margin_pct(10000.0, 2500.0) == pytest.approx(25.0)


def test_margin_is_undefined_without_revenue():
    """Dividing by zero revenue would report a nonsense margin; None is honest."""
    assert margin_pct(0.0, -3000.0) is None
    assert margin_pct(-5.0, -5.0) is None


def test_revenue_per_km():
    assert revenue_per_km(9000.0, 150.0) == pytest.approx(60.0)
    assert revenue_per_km(9000.0, 0.0) is None


def test_variance_is_positive_when_the_partner_claims_more_distance():
    """Positive variance is the direction that costs the operator money."""
    assert distance_variance_pct(100.0, 120.0) == pytest.approx(20.0)
    assert distance_variance_pct(100.0, 80.0) == pytest.approx(-20.0)
    assert distance_variance_pct(100.0, 100.0) == pytest.approx(0.0)


def test_variance_needs_a_meaningful_baseline():
    """A vehicle that barely moved would produce an enormous, meaningless variance."""
    assert distance_variance_pct(0.4, 30.0) is None
    assert distance_variance_pct(100.0, None) is None


def test_dispute_threshold_is_symmetric_and_exclusive():
    assert is_disputed(DISTANCE_DISPUTE_PCT + 0.1)
    assert is_disputed(-(DISTANCE_DISPUTE_PCT + 0.1))
    assert not is_disputed(DISTANCE_DISPUTE_PCT)      # exactly at the threshold is tolerated
    assert not is_disputed(5.0)
    assert not is_disputed(None)


def test_a_loss_is_unprofitable():
    assert is_unprofitable(revenue=8000.0, profit_value=-200.0)
    assert is_unprofitable(revenue=8000.0, profit_value=0.0)


def test_a_thin_margin_is_unprofitable_even_when_positive():
    """Barely breaking even does not justify keeping the asset on the road."""
    revenue = 10000.0
    just_under = revenue * LOW_MARGIN_FRACTION - 1
    just_over = revenue * LOW_MARGIN_FRACTION + 1
    assert is_unprofitable(revenue, just_under)
    assert not is_unprofitable(revenue, just_over)


def test_no_revenue_day_is_unprofitable():
    assert is_unprofitable(revenue=0.0, profit_value=-4000.0)


def test_summarise_rolls_up_the_fleet():
    rows = [
        vehicle(vehicle_id="A", revenue=10000.0, total_cost=6000.0, profit=4000.0, trips=10),
        vehicle(vehicle_id="B", revenue=5000.0, total_cost=6000.0, profit=-1000.0, trips=4,
                unprofitable=True),
        vehicle(vehicle_id="C", revenue=8000.0, total_cost=4000.0, profit=4000.0, trips=6,
                distance_disputed=True, expense_data_present=False),
    ]
    s = summarise(rows)
    assert s["vehicles"] == 3
    assert s["trips"] == 20
    assert s["revenue"] == pytest.approx(23000.0)
    assert s["total_cost"] == pytest.approx(16000.0)
    assert s["profit"] == pytest.approx(7000.0)
    assert s["margin_pct"] == pytest.approx(30.43, abs=0.01)
    assert s["unprofitable_vehicles"] == 1
    assert s["disputed_vehicles"] == 1
    assert s["missing_expense_rows"] == 1


def test_summarise_handles_missing_cost_data():
    """A vehicle with no expense row must not poison the fleet total."""
    rows = [vehicle(total_cost=None, profit=12000.0, expense_data_present=False)]
    s = summarise(rows)
    assert s["total_cost"] == 0.0
    assert s["missing_expense_rows"] == 1
