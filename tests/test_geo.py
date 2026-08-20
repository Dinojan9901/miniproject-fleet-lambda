"""Zone assignment and distance -- the two geographic primitives the pipeline uses."""
import pytest

from common.geo import (
    LAT_MAX,
    LAT_MIN,
    LON_MAX,
    LON_MIN,
    OUT_OF_AREA,
    all_zones,
    clamp_to_area,
    haversine_km,
    zone_for,
)


def test_six_distinct_zones():
    zones = all_zones()
    assert len(zones) == 6
    assert len(set(zones)) == 6


def test_corners_land_in_the_expected_zones():
    # South-west corner is row 0 / col 0; north-east is the last row and column.
    assert zone_for(LAT_MIN, LON_MIN) == "Z1-Dehiwala"
    assert zone_for(LAT_MAX - 1e-6, LON_MAX - 1e-6) == "Z6-Rajagiriya"


def test_every_zone_is_reachable():
    """Sweep the box and confirm the grid actually produces all six zones."""
    seen = set()
    for i in range(20):
        for j in range(20):
            lat = LAT_MIN + (LAT_MAX - LAT_MIN) * i / 20
            lon = LON_MIN + (LON_MAX - LON_MIN) * j / 20
            seen.add(zone_for(lat, lon))
    assert seen == set(all_zones())


@pytest.mark.parametrize("lat, lon", [
    (0.0, 0.0),
    (LAT_MIN - 0.01, LON_MIN + 0.01),
    (LAT_MAX + 0.01, LON_MIN + 0.01),
    (LAT_MIN + 0.01, LON_MAX + 0.01),
    (None, 79.86),
    (6.90, None),
])
def test_outside_the_operating_area(lat, lon):
    """Leaving the city is a business fact, not a validation error."""
    assert zone_for(lat, lon) == OUT_OF_AREA


def test_upper_bound_is_exclusive_but_still_assigned():
    assert zone_for(LAT_MAX, LON_MIN) == OUT_OF_AREA      # exactly at the edge is outside
    assert zone_for(LAT_MAX - 1e-9, LON_MIN) != OUT_OF_AREA


def test_haversine_zero_for_the_same_point():
    assert haversine_km(6.9271, 79.8612, 6.9271, 79.8612) == pytest.approx(0.0)


def test_haversine_known_distance():
    """Colombo Fort to Dehiwala is about 9 km."""
    km = haversine_km(6.9344, 79.8428, 6.8511, 79.8636)
    assert 9.0 < km < 10.0


def test_haversine_is_symmetric():
    a = haversine_km(6.90, 79.85, 6.95, 79.90)
    b = haversine_km(6.95, 79.90, 6.90, 79.85)
    assert a == pytest.approx(b)


def test_one_degree_of_latitude_is_about_111_km():
    assert haversine_km(6.0, 79.9, 7.0, 79.9) == pytest.approx(111.19, abs=0.5)


def test_clamp_keeps_vehicles_inside_the_box():
    lat, lon = clamp_to_area(90.0, 180.0)
    assert LAT_MIN <= lat < LAT_MAX and LON_MIN <= lon < LON_MAX
    assert zone_for(lat, lon) != OUT_OF_AREA
