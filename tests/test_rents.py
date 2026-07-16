from comps_pipeline import _calc_weighted_rent_per_sqft


def _facility(pricing):
    return [{"pricing": pricing}]


def test_in_store_rate_used_when_present():
    facs = _facility([{"unit_type": "drive_up", "size": "10x10",
                       "in_store_rate": 150, "web_rate": 120}])
    # 10x10 = 100 sqft → prefers in-store: 150/100 = 1.5
    assert _calc_weighted_rent_per_sqft(facs, "drive_up") == 1.5


def test_falls_back_to_web_rate():
    facs = _facility([{"unit_type": "drive_up", "size": "10x10",
                       "in_store_rate": None, "web_rate": 120}])
    assert _calc_weighted_rent_per_sqft(facs, "drive_up") == 1.2


def test_none_when_no_pricing_for_type():
    facs = _facility([{"unit_type": "climate_control", "size": "10x10",
                       "in_store_rate": 150}])
    assert _calc_weighted_rent_per_sqft(facs, "drive_up") is None
    assert _calc_weighted_rent_per_sqft(facs, "climate_control") == 1.5


def test_none_when_empty():
    assert _calc_weighted_rent_per_sqft([], "drive_up") is None


def test_weights_normalized_across_sizes():
    # Two sizes at the same $/sqft must blend to that same $/sqft
    facs = _facility([
        {"unit_type": "drive_up", "size": "5x10", "in_store_rate": 100},   # 2.0/sqft
        {"unit_type": "drive_up", "size": "10x10", "in_store_rate": 200},  # 2.0/sqft
    ])
    assert _calc_weighted_rent_per_sqft(facs, "drive_up") == 2.0
