"""Run:  python tests/test_heat_formulas.py   (or: python -m pytest -q)"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
from src import heat_formulas as hf
CFG = json.load(open(os.path.join(HERE, "..", "conf", "project_config.json")))


def test_kelvin():
    assert abs(hf.kelvin_to_c(300.15) - 27.0) < 1e-9


def test_rh_100_when_dewpoint_equals_temperature():
    assert abs(hf.relative_humidity(30, 30) - 100) < 1e-9


def test_wbgt_known_value():          # T=35, Td=25 -> e~31.6 hPa -> WBGT~36.2
    assert abs(hf.wbgt_simple(35, 25) - 36.2) < 0.1


def test_wbgt_rises_with_humidity():
    assert hf.wbgt_simple(35, 28) > hf.wbgt_simple(35, 15)


def test_wind():
    assert abs(hf.wind_direction_deg(0, -5) - 0) < 1e-9
    assert abs(hf.wind_direction_deg(-5, 0) - 90) < 1e-9
    assert abs(hf.wind_speed(3, 4) - 5) < 1e-9


def test_grid_35_points_contains_doha():
    pts = hf.grid_points(CFG["grid"])
    assert len(pts) == CFG["quality"]["expected_points"] == 35
    assert (CFG["target_point"]["latitude"], CFG["target_point"]["longitude"]) in pts


def test_expected_hours():            # 16,802 days in 1980-2025
    assert hf.expected_hours("1980-01-01", "2025-12-31") == 16802 * 24


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn(); print("PASS", name)
