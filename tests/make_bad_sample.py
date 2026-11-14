"""Creates tests/data/bad_sample_v1/era5_bad_sample.csv - the SEPARATE test input for the controlled
quality failure. 116 rows: 96 good, 10 duplicates, 4 missing temperature, 3 text in a number column,
3 impossible values -> 10 rejected (8.6 %) > 1 % limit -> the quality gate must FAIL."""
import csv, os, random
from datetime import datetime, timedelta
random.seed(42)
OUT = os.path.join(os.path.dirname(__file__), "data", "bad_sample_v1", "era5_bad_sample.csv")
os.makedirs(os.path.dirname(OUT), exist_ok=True)
COLS = ["valid_time", "latitude", "longitude", "t2m", "d2m", "u10", "v10", "sp", "ssrd", "tcc"]
rows = []
for lat, lon in [(25.25, 51.5), (25.5, 51.5)]:
    for h in range(48):
        t = datetime(2024, 7, 1) + timedelta(hours=h)
        rows.append([t.strftime("%Y-%m-%d %H:%M:%S"), lat, lon, round(308 + random.uniform(-4, 4), 2),
                     round(298 + random.uniform(-3, 2), 2), round(random.uniform(-5, 5), 2), round(random.uniform(-5, 5), 2),
                     round(100000 + random.uniform(-300, 300), 1), round(max(0, random.uniform(-1e6, 3e6)), 1),
                     round(random.uniform(0, 0.3), 3)])
dupes = [list(r) for r in random.sample(rows, 10)]
missing = [list(r) for r in random.sample(rows, 4)]
for r in missing: r[3] = ""
bad_type = [list(r) for r in random.sample(rows, 3)]
for r in bad_type: r[4] = "N/A"
impossible = [list(r) for r in random.sample(rows, 3)]
impossible[0][3] = 673.15                      # 400 C
impossible[1][4] = impossible[1][3] + 15       # dewpoint above temperature
impossible[2][7] = 50000                       # 500 hPa at sea level
for i, r in enumerate(missing + bad_type + impossible):   # unique times -> rejected, not de-duplicated
    r[0] = (datetime(2024, 8, 1) + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
with open(OUT, "w", newline="") as f:
    w = csv.writer(f); w.writerow(COLS); w.writerows(rows + dupes + missing + bad_type + impossible)
print("wrote", OUT)
