from comps_pipeline import classify_facility, calc_facility_assumptions


def test_classify_boundaries():
    assert classify_facility(None) == "single_story"
    assert classify_facility(1.99) == "multi_story"
    assert classify_facility(2.0) == "mixed"
    assert classify_facility(4.0) == "mixed"
    assert classify_facility(4.01) == "single_story"


def test_single_story_defaults():
    a = calc_facility_assumptions("single_story")
    assert a["yield_pct"] == 0.40
    assert a["cost_per_sqft"] == 50.0


def test_multi_story_defaults():
    a = calc_facility_assumptions("multi_story")
    # Lehi, UT reference: 85,000 rentable / (1.6 ac * 43,560) ≈ 122%
    assert 1.21 < a["yield_pct"] < 1.23
    assert a["cost_per_sqft"] == 95.0


def test_mixed_split_hits_target_sqft():
    # 2 acres: per PROFORMA_LOGIC.md the split is ~77% multi-story and the
    # combined rentable sqft should hit the 90,000 target.
    a = calc_facility_assumptions("mixed", acres=2.0)
    assert 0.75 < a["ms_frac"] < 0.80
    assert abs(a["ms_frac"] + a["ss_frac"] - 1.0) < 1e-6
    assert abs(a["ms_sqft"] + a["ss_sqft"] - 90_000) < 500
    # Effective blended yield ≈ 90,000 / (2 * 43,560)
    assert abs(a["yield_pct"] - 90_000 / (2 * 43_560)) < 0.01


def test_mixed_without_acres_falls_back_to_single_story():
    a = calc_facility_assumptions("mixed", acres=None)
    assert a["yield_pct"] == 0.40
    assert a["cost_per_sqft"] == 50.0
