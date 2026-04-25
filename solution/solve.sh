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


def fmt_avg(mean: float) -> str:
    r = round(mean, 1)
    if r == 0:
        return "0.0"
    return str(r)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rows_in_order: list[tuple[str, str, float]] = []
    with INPUT_PATH.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_in_order.append(
                (
                    row["station_id"].strip(),
                    row["timestamp"].strip(),
                    float(row["temperature_c"]),
                )
            )

    total_in = len(rows_in_order)
    last_temp: dict[tuple[str, str], float] = {}
    for sid, ts, temp in rows_in_order:
        last_temp[(sid, ts)] = temp

    duplicate_rows_dropped = total_in - len(last_temp)

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

    stations = []
    for sid in sorted(by_station.keys()):
        temps = by_station[sid]
        mean = sum(temps) / len(temps)
        stations.append(
            {
                "station_id": sid,
                "readings": len(temps),
                "avg_temperature_c": fmt_avg(mean),
            }
        )

    stats = {
        "duplicate_rows_dropped": duplicate_rows_dropped,
        "station_count": len(by_station),
        "stations": stations,
    }
    STATS_PATH.write_text(json.dumps(stats, indent=2) + "\n")


if __name__ == "__main__":
    main()
PY

chmod +x /app/dedupe_report.py
python3 /app/dedupe_report.py
