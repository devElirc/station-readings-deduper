#!/bin/bash
set -euo pipefail

mkdir -p /app/output

cat > /app/dedupe_report.py << 'PY'
#!/usr/bin/env python3
"""Build deduped station readings and registry-aware station statistics."""

import csv
import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

INPUT_PATH = Path("/app/inputs/station_readings.csv")
REGISTRY_PATH = Path("/app/inputs/station_registry.json")
OUTPUT_DIR = Path("/app/output")
DEDUPED_PATH = OUTPUT_DIR / "deduped.csv"
STATS_PATH = OUTPUT_DIR / "stats.json"
QUALITY_CODES = ("OK", "WARN", "EST")


def localize_naive(naive: datetime, zone_name: str) -> tuple[datetime, bool]:
    zone = ZoneInfo(zone_name)

    def candidates(wall: datetime) -> list[datetime]:
        valid = []
        for fold in (0, 1):
            aware = wall.replace(tzinfo=zone, fold=fold)
            roundtrip = aware.astimezone(timezone.utc).astimezone(zone)
            if roundtrip.replace(tzinfo=None) == wall:
                valid.append(aware)
        return valid

    current = naive
    shifted = False
    for _ in range(24 * 60 + 1):
        valid = candidates(current)
        if valid:
            instant = max(valid, key=lambda dt: dt.astimezone(timezone.utc))
            return instant.astimezone(timezone.utc), shifted
        current += timedelta(minutes=1)
        shifted = True
    raise ValueError("could not resolve nonexistent local time")


def parse_instant(
    ts: str, station_id: str | None = None, zones: dict[str, str] | None = None
) -> tuple[datetime | None, bool]:
    t = ts.strip()
    if not t:
        return None, False
    try:
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        dt = datetime.fromisoformat(t)
    except ValueError:
        return None, False
    if dt.tzinfo is None:
        zone_name = "UTC"
        if station_id is not None and zones is not None:
            zone_name = zones.get(station_id, "UTC")
        try:
            return localize_naive(dt, zone_name)
        except Exception:
            return None, False
    return dt.astimezone(timezone.utc), False


def fmt_decimal(x: float) -> str:
    r = round(x, 1)
    if r == 0:
        return "0.0"
    return str(r)


def median_from_values(vals: list[float]) -> float:
    ordered = sorted(vals)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def resolve_alias(station_id: str, aliases: dict[str, str]) -> str:
    seen: dict[str, int] = {}
    current = station_id
    while current in aliases:
        if current in seen:
            cycle = list(seen)[seen[current] :]
            return min(cycle)
        seen[current] = len(seen)
        current = aliases[current]
    return current


def load_registry() -> tuple[dict[str, str], dict[str, str], list[dict], list[dict]]:
    data = json.loads(REGISTRY_PATH.read_text())
    aliases = {str(k).strip(): str(v).strip() for k, v in data["aliases"].items()}
    zones = {
        resolve_alias(str(k).strip(), aliases): str(v).strip()
        for k, v in data.get("station_timezones", {}).items()
    }

    suppressions = []
    for item in data["suppressions"]:
        suppressions.append(
            {
                "station_id": str(item["station_id"]).strip(),
                "start": parse_instant(str(item["start"]))[0],
                "end": parse_instant(str(item["end"]))[0],
            }
        )

    calibrations = []
    for idx, item in enumerate(data["calibrations"]):
        calibrations.append(
            {
                "station_id": str(item["station_id"]).strip(),
                "start": parse_instant(str(item["start"]))[0],
                "end": parse_instant(str(item["end"]))[0],
                "offset_c": float(item["offset_c"]),
                "index": idx,
            }
        )
    return aliases, zones, suppressions, calibrations


def in_interval(instant: datetime, start: datetime, end: datetime) -> bool:
    return start <= instant < end


def is_suppressed(station_id: str, instant: datetime, suppressions: list[dict]) -> bool:
    return any(
        item["station_id"] == station_id
        and in_interval(instant, item["start"], item["end"])
        for item in suppressions
    )


def calibrated_temperature(
    station_id: str, instant: datetime, temp: float, calibrations: list[dict]
) -> float:
    matches = [
        item
        for item in calibrations
        if item["station_id"] == station_id
        and in_interval(instant, item["start"], item["end"])
    ]
    if not matches:
        return temp
    chosen = max(matches, key=lambda item: (item["start"], item["index"]))
    return temp + chosen["offset_c"]


def empty_counts() -> dict[str, int]:
    return {code: 0 for code in QUALITY_CODES}


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    aliases, zones, suppressions, calibrations = load_registry()

    skipped_malformed_rows = 0
    skipped_suppressed_rows = 0
    shifted_nonexistent_timestamps = 0
    valid_rows: list[tuple[str, str, float, str, datetime]] = []

    with INPUT_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid_raw = (row.get("station_id") or "").strip()
            ts = (row.get("timestamp") or "").strip()
            temp_raw = (row.get("temperature_c") or "").strip()
            quality = (row.get("quality_code") or "").strip()
            if not sid_raw or not ts or not temp_raw or quality not in QUALITY_CODES:
                skipped_malformed_rows += 1
                continue

            station_id = resolve_alias(sid_raw, aliases)
            instant, shifted = parse_instant(ts, station_id, zones)
            if instant is None:
                skipped_malformed_rows += 1
                continue

            try:
                temp = float(temp_raw)
            except ValueError:
                skipped_malformed_rows += 1
                continue
            if not math.isfinite(temp):
                skipped_malformed_rows += 1
                continue

            if shifted:
                shifted_nonexistent_timestamps += 1
            if is_suppressed(station_id, instant, suppressions):
                skipped_suppressed_rows += 1
                continue

            temp = calibrated_temperature(station_id, instant, temp, calibrations)
            valid_rows.append((station_id, ts, temp, quality, instant))

    last_win: dict[tuple[str, datetime], tuple[str, str, float, str, datetime]] = {}
    for station_id, ts, temp, quality, instant in valid_rows:
        last_win[(station_id, instant)] = (station_id, ts, temp, quality, instant)

    deduped_rows = list(last_win.values())
    deduped_rows.sort(key=lambda r: (r[0], r[4]))

    with DEDUPED_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "station_id", "temperature_c", "quality_code"])
        for station_id, ts, temp, quality, _instant in deduped_rows:
            writer.writerow([ts, station_id, str(temp), quality])

    by_station: dict[str, list[tuple[datetime, float, str]]] = {}
    global_counts = empty_counts()
    for station_id, _ts, temp, quality, instant in deduped_rows:
        by_station.setdefault(station_id, []).append((instant, temp, quality))
        global_counts[quality] += 1

    all_temps = [temp for _station_id, _ts, temp, _quality, _instant in deduped_rows]
    stations = []
    for station_id in sorted(by_station):
        readings = sorted(by_station[station_id], key=lambda r: r[0])
        temps = [temp for _instant, temp, _quality in readings]
        counts = empty_counts()
        for _instant, _temp, quality in readings:
            counts[quality] += 1
        gaps = [
            int((right[0] - left[0]).total_seconds() // 60)
            for left, right in zip(readings, readings[1:])
        ]
        quality_runs = 0
        previous_quality = None
        for _instant, _temp, quality in readings:
            if quality != previous_quality:
                quality_runs += 1
                previous_quality = quality
        stations.append(
            {
                "station_id": station_id,
                "readings": len(readings),
                "min_temperature_c": fmt_decimal(min(temps)),
                "max_temperature_c": fmt_decimal(max(temps)),
                "median_temperature_c": fmt_decimal(median_from_values(temps)),
                "avg_temperature_c": fmt_decimal(sum(temps) / len(temps)),
                "quality_counts": counts,
                "longest_gap_minutes": max(gaps) if gaps else None,
                "quality_runs": quality_runs,
            }
        )

    stats = {
        "duplicate_rows_dropped": len(valid_rows) - len(deduped_rows),
        "skipped_malformed_rows": skipped_malformed_rows,
        "skipped_suppressed_rows": skipped_suppressed_rows,
        "shifted_nonexistent_timestamps": shifted_nonexistent_timestamps,
        "deduped_row_count": len(deduped_rows),
        "station_count": len(by_station),
        "median_temperature_c_all": fmt_decimal(median_from_values(all_temps)),
        "global_quality_counts": global_counts,
        "stations": stations,
    }
    STATS_PATH.write_text(json.dumps(stats, indent=2) + "\n")


if __name__ == "__main__":
    main()
PY

chmod +x /app/dedupe_report.py
python3 /app/dedupe_report.py
