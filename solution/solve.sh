#!/bin/bash
set -euo pipefail

mkdir -p /app/output

cat > /app/dedupe_report.py << 'PY'
#!/usr/bin/env python3
"""Dedupe station readings by (station_id, timestamp), last row wins."""

import csv
import json
from pathlib import Path

INPUT_PATH = Path("/app/inputs/station_readings.csv")
OUTPUT_DIR = Path("/app/output")
DEDUPED_PATH = OUTPUT_DIR / "deduped.csv"
STATS_PATH = OUTPUT_DIR / "stats.json"


def fmt_decimal(x: float) -> str:
    r = round(x, 1)
    if r == 0:
        return "0.0"
    return str(r)


def median_from_sorted_values(vals: list[float]) -> float:
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    skipped_malformed_rows = 0
    rows_in_order: list[tuple[str, str, float]] = []
    with INPUT_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = (row.get("station_id") or "").strip()
            ts = (row.get("timestamp") or "").strip()
            tc_raw = row.get("temperature_c")
            if not sid or not ts:
                skipped_malformed_rows += 1
                continue
            if tc_raw is None or str(tc_raw).strip() == "":
                skipped_malformed_rows += 1
                continue
            try:
                temp = float(str(tc_raw).strip())
            except ValueError:
                skipped_malformed_rows += 1
                continue
            rows_in_order.append((sid, ts, temp))

    total_in = len(rows_in_order)
    last_temp: dict[tuple[str, str], float] = {}
    for sid, ts, temp in rows_in_order:
        last_temp[(sid, ts)] = temp

    duplicate_rows_dropped = total_in - len(last_temp)
    deduped_row_count = len(last_temp)

    deduped_rows = [
        (sid, ts, last_temp[(sid, ts)])
        for (sid, ts) in last_temp
    ]
    deduped_rows.sort(key=lambda r: (r[0], r[1]))

    with DEDUPED_PATH.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "station_id", "temperature_c"])
        for sid, ts, temp in deduped_rows:
            w.writerow([ts, sid, temp])

    by_station: dict[str, list[float]] = {}
    for sid, _ts, temp in deduped_rows:
        by_station.setdefault(sid, []).append(temp)

    all_temps = [temp for _sid, _ts, temp in deduped_rows]
    median_all = median_from_sorted_values(all_temps)

    stations = []
    for sid in sorted(by_station.keys()):
        temps = by_station[sid]
        mean = sum(temps) / len(temps)
        med = median_from_sorted_values(temps)
        stations.append(
            {
                "station_id": sid,
                "readings": len(temps),
                "min_temperature_c": fmt_decimal(min(temps)),
                "max_temperature_c": fmt_decimal(max(temps)),
                "median_temperature_c": fmt_decimal(med),
                "avg_temperature_c": fmt_decimal(mean),
            }
        )

    stats = {
        "duplicate_rows_dropped": duplicate_rows_dropped,
        "skipped_malformed_rows": skipped_malformed_rows,
        "deduped_row_count": deduped_row_count,
        "station_count": len(by_station),
        "median_temperature_c_all": fmt_decimal(median_all),
        "stations": stations,
    }
    STATS_PATH.write_text(json.dumps(stats, indent=2) + "\n")


if __name__ == "__main__":
    main()
PY

chmod +x /app/dedupe_report.py
python3 /app/dedupe_report.py
