"""Verify station CSV dedupe tool, deduped output, and stats JSON."""

import csv
import json
import subprocess
from pathlib import Path

INPUT_CSV = Path("/app/inputs/station_readings.csv")
SCRIPT_PATH = Path("/app/dedupe_report.py")
DEDUPED_CSV = Path("/app/output/deduped.csv")
STATS_JSON = Path("/app/output/stats.json")


def _fmt_avg(mean: float) -> str:
    """Match instruction: round(mean, 1) then str; zero maps to 0.0."""
    r = round(mean, 1)
    if r == 0:
        return "0.0"
    return str(r)


def _reference_from_input() -> tuple[list[tuple[str, str, float]], dict]:
    """Recompute expected deduped rows and stats from the bundled CSV."""
    rows_in_order: list[tuple[str, str, float]] = []
    with INPUT_CSV.open(newline="") as f:
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
    dropped = total_in - len(last_temp)
    deduped = [
        (sid, ts, last_temp[(sid, ts)])
        for (sid, ts) in last_temp
    ]
    deduped.sort(key=lambda r: (r[0], r[1]))
    by_station: dict[str, list[float]] = {}
    for sid, _ts, temp in deduped:
        by_station.setdefault(sid, []).append(temp)
    stations = []
    for sid in sorted(by_station.keys()):
        temps = by_station[sid]
        mean = sum(temps) / len(temps)
        stations.append(
            {
                "station_id": sid,
                "readings": len(temps),
                "avg_temperature_c": _fmt_avg(mean),
            }
        )
    stats = {
        "duplicate_rows_dropped": dropped,
        "station_count": len(by_station),
        "stations": stations,
    }
    return deduped, stats


def test_script_exists():
    """Verify /app/dedupe_report.py exists."""
    assert SCRIPT_PATH.is_file(), "Expected /app/dedupe_report.py"


def test_script_has_shebang():
    """Verify the script starts with the required Python shebang."""
    first = SCRIPT_PATH.read_text().splitlines()[0]
    assert first == "#!/usr/bin/env python3", f"Unexpected shebang: {first!r}"


def test_script_compiles():
    """Verify /app/dedupe_report.py is valid Python syntax."""
    proc = subprocess.run(
        ["python3", "-m", "py_compile", str(SCRIPT_PATH)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr


def test_output_files_exist():
    """Verify deduped CSV and stats JSON were produced under /app/output/."""
    assert DEDUPED_CSV.is_file(), "Missing /app/output/deduped.csv"
    assert STATS_JSON.is_file(), "Missing /app/output/stats.json"


def test_deduped_csv_matches_reference():
    """Verify deduped rows and sort order match a recomputation from input."""
    expected_rows, _ = _reference_from_input()
    with DEDUPED_CSV.open(newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == [
            "timestamp",
            "station_id",
            "temperature_c",
        ], "Unexpected CSV header"
        actual = [
            (
                row["station_id"].strip(),
                row["timestamp"].strip(),
                float(row["temperature_c"]),
            )
            for row in reader
        ]
    assert actual == expected_rows, "deduped.csv does not match expected dedupe"


def test_stats_json_matches_reference():
    """Verify stats.json fields match recomputation from the input CSV."""
    _, expected_stats = _reference_from_input()
    data = json.loads(STATS_JSON.read_text())
    assert data == expected_stats, f"stats mismatch: {data!r} vs {expected_stats!r}"


def test_stats_schema():
    """Verify stats.json has required top-level keys and station entry shape."""
    data = json.loads(STATS_JSON.read_text())
    assert set(data.keys()) == {
        "duplicate_rows_dropped",
        "station_count",
        "stations",
    }
    assert isinstance(data["duplicate_rows_dropped"], int)
    assert isinstance(data["station_count"], int)
    assert isinstance(data["stations"], list)
    for entry in data["stations"]:
        assert set(entry.keys()) == {
            "station_id",
            "readings",
            "avg_temperature_c",
        }
        assert isinstance(entry["readings"], int)
        assert isinstance(entry["avg_temperature_c"], str)
