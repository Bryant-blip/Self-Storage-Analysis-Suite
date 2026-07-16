import openpyxl
import pytest

from db_utils import calc_proforma_cells, _calc_yoc


def _sheet(cells: dict):
    ws = openpyxl.Workbook().active
    for ref, val in cells.items():
        ws[ref] = val
    return ws


def test_standard_layout():
    ws = _sheet({"C5": 5.0, "C6": 500_000, "E3": "single_story", "E5": 0.40,
                 "E6": 1.20, "E7": 0.90, "E8": 0.25, "E9": 0.07, "E10": 50})
    c = calc_proforma_cells(ws)
    assert c["acres"] == 5.0
    assert c["asking_price"] == 500_000
    assert c["avg_psf"] == 1.20
    assert c["yield_pct"] == 0.40
    assert c["facility_type"] == "single_story"


def test_missing_cells_are_none_not_guessed():
    c = calc_proforma_cells(_sheet({}))
    assert c["avg_psf"] is None
    assert c["yield_pct"] is None
    assert c["facility_type"] is None


def test_mixed_layout_detected_and_blended():
    # E5 empty + B15 populated → mixed template layout
    ws = _sheet({"C5": 2.0, "C6": 800_000, "C8": 0.07,
                 "B15": 60_000, "D15": 1.60, "D16": 0.90, "D17": 0.25, "D18": 95,
                 "B24": 30_000, "D24": 1.00, "D25": 0.90, "D26": 0.25, "D27": 50})
    c = calc_proforma_cells(ws)
    assert c["facility_type"] == "mixed"
    # sqft-weighted rent: (60k*1.6 + 30k*1.0) / 90k = 1.40
    assert c["avg_psf"] == pytest.approx(1.40)
    # sqft-weighted cost: (60k*95 + 30k*50) / 90k = 80.0
    assert c["cost_per_sqft"] == pytest.approx(80.0)
    # yield: 90k rentable / (2 ac * 43,560)
    assert c["yield_pct"] == pytest.approx(90_000 / (2 * 43_560))
    assert c["occupancy"] == 0.90          # identical minis pass through
    assert c["du_psf"] == 1.00


def test_yoc_known_numbers():
    cells = {"acres": 5.0, "avg_psf": 1.2, "yield_pct": 0.40, "occupancy": 0.90,
             "expense_ratio": 0.25, "cap_rate": 0.07, "cost_per_sqft": 50,
             "asking_price": 500_000}
    # net rentable 87,120 → annual NOI 846,806.40 → total cost 4,856,000
    assert _calc_yoc(cells) == pytest.approx(846_806.4 / 4_856_000)


def test_yoc_missing_input_returns_none():
    assert _calc_yoc({"acres": 5.0, "avg_psf": None, "yield_pct": 0.4,
                      "occupancy": 0.9, "expense_ratio": 0.25,
                      "cap_rate": 0.07, "cost_per_sqft": 50,
                      "asking_price": None}) is None
