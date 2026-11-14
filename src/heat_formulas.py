"""Shared formulas for the heat-stress project.
Plain-Python versions are tested in tests/test_heat_formulas.py; the spark_* versions
are used in the Databricks notebooks, so both follow the same definitions."""
import math
from datetime import date

KELVIN = 273.15
QATAR_UTC_OFFSET_HOURS = 3

# Physical validity ranges used in the silver step (after unit conversion)
RANGES = {
    "t_c": (-10.0, 60.0),        # 2 m air temperature, degC
    "td_c": (-40.0, 40.0),       # 2 m dewpoint, degC
    "sp_hpa": (850.0, 1100.0),   # surface pressure, hPa
    "tcc": (0.0, 1.0),           # total cloud cover, fraction
    "ssrd_jm2": (0.0, 5.0e6),    # hourly accumulated solar radiation, J/m2
    "wind_comp": (-60.0, 60.0),  # u10 / v10, m/s
}


def kelvin_to_c(k):
    return k - KELVIN


def vapour_pressure_hpa(td_c):
    """Vapour pressure from dewpoint (Magnus form), hPa."""
    return 6.105 * math.exp(17.27 * td_c / (237.7 + td_c))


def relative_humidity(t_c, td_c):
    return 100.0 * vapour_pressure_hpa(td_c) / vapour_pressure_hpa(t_c)


def wbgt_simple(t_c, td_c):
    """Simplified shade WBGT (Australian Bureau of Meteorology):
    WBGT = 0.567*T + 0.393*e + 3.94 (e in hPa). Ignores sun and wind -> an estimate."""
    return 0.567 * t_c + 0.393 * vapour_pressure_hpa(td_c) + 3.94


def wind_speed(u, v):
    return math.hypot(u, v)


def wind_direction_deg(u, v):
    """Direction the wind blows FROM (0 = north, 90 = east)."""
    return (270.0 - math.degrees(math.atan2(v, u))) % 360.0


def grid_points(grid):
    def frange(a, b, s):
        n = int(round((b - a) / s))
        return [round(a + i * s, 2) for i in range(n + 1)]
    return [(lat, lon) for lat in frange(grid["lat_min"], grid["lat_max"], grid["step"])
            for lon in frange(grid["lon_min"], grid["lon_max"], grid["step"])]


def expected_hours(date_start, date_end):
    return ((date.fromisoformat(date_end) - date.fromisoformat(date_start)).days + 1) * 24


def point_id(lat, lon):
    return f"lat{lat:.2f}_lon{lon:.2f}"


# ---------- Spark versions ----------
def _F():
    from pyspark.sql import functions as F
    return F


def spark_vapour_pressure_hpa(td):
    F = _F()
    return F.lit(6.105) * F.exp(F.lit(17.27) * td / (F.lit(237.7) + td))


def spark_relative_humidity(t, td):
    return _F().lit(100.0) * spark_vapour_pressure_hpa(td) / spark_vapour_pressure_hpa(t)


def spark_wbgt_simple(t, td):
    F = _F()
    return F.lit(0.567) * t + F.lit(0.393) * spark_vapour_pressure_hpa(td) + F.lit(3.94)


def spark_wind_speed(u, v):
    return _F().sqrt(u * u + v * v)


def spark_wind_direction_deg(u, v):
    F = _F()
    return F.pmod(F.lit(270.0) - F.degrees(F.atan2(v, u)), F.lit(360.0))
