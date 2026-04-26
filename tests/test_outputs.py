"""Verify registry-aware station CSV dedupe, path policy, and outputs."""

import csv
import json
import math
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

INPUT_CSV = Path("/app/inputs/station_readings.csv")
REGISTRY_JSON = Path("/app/inputs/station_registry.json")
SCRIPT_PATH = Path("/app/dedupe_report.py")
DEDUPED_CSV = Path("/app/output/deduped.csv")
STATS_JSON = Path("/app/output/stats.json")
QUALITY_CODES = ("OK", "WARN", "EST")

# CWD for subprocess runs of /app/dedupe_report.py so strace resolves relative paths
# against /app (prevents quietly opening harness files via relative paths).
DEDUPE_SCRIPT_CWD = "/app"

# Anti-cheat: benchmark / verifier trees the agent script must never read.
_STRACE_FORBIDDEN_PREFIXES = ("/tests", "/oracle", "/solution", "/logs")

# Outside /app, only normal OS/CPython paths are allowed (Debian slim + strace).
_STRACE_ALLOWED_NON_APP_PREFIXES = (
    "/usr/",
    "/lib/",
    "/lib64/",
    "/bin/",
    "/sbin/",
    "/etc/",
    "/dev/",
    "/proc/",
    "/sys/",
    "/run/",
    "/tmp/",
)


def _strace_collect_path_arguments(log_text: str) -> set[str]:
    """Extract pathname arguments from common strace file-related syscalls."""
    paths: set[str] = set()
    patterns = (
        r'openat64?\([^"]*,\s*"([^"]+)"',
        r'open\("([^"]+)"',
        r'newfstatat\([^,]+,\s*"([^"]+)"',
        r'stat\("([^"]+)"',
        r'statx\([^,]+,\s*"([^"]+)"',
    )
    for pattern in patterns:
        for match in re.finditer(pattern, log_text):
            paths.add(match.group(1))
    return paths


def _normalize_traced_path(raw: str, cwd: str) -> str:
    """Normalize a strace path; resolve relative paths against the traced process cwd."""
    s = (raw or "").strip()
    if not s:
        return ""
    if " (deleted)" in s:
        s = s.split(" (deleted)", 1)[0]
    if s.startswith("/"):
        return os.path.normpath(s)
    return os.path.normpath(os.path.join(cwd, s))


def _localize_naive(naive: datetime, zone_name: str) -> tuple[datetime, bool]:
    """Interpret a naive wall time, choosing later ambiguous instants and shifting gaps."""
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


def _parse_instant(
    ts: str, station_id: str | None = None, zones: dict[str, str] | None = None
) -> tuple[datetime | None, bool]:
    """Parse timestamps exactly as the task specifies."""
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
            return _localize_naive(dt, zone_name)
        except Exception:
            return None, False
    return dt.astimezone(timezone.utc), False


def _fmt_decimal(x: float) -> str:
    """Format one decimal after round(x, 1), mapping negative zero to 0.0."""
    r = round(x, 1)
    if r == 0:
        return "0.0"
    return str(r)


def _median_from_values(vals: list[float]) -> float:
    """Return the median using sorted numeric values and even-count averaging."""
    ordered = sorted(vals)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _resolve_alias(station_id: str, aliases: dict[str, str]) -> str:
    """Resolve chained aliases; cycles collapse to the lexicographically smallest id."""
    seen: dict[str, int] = {}
    current = station_id
    while current in aliases:
        if current in seen:
            cycle = list(seen)[seen[current] :]
            return min(cycle)
        seen[current] = len(seen)
        current = aliases[current]
    return current


def _load_registry() -> tuple[dict[str, str], dict[str, str], list[dict], list[dict]]:
    """Load aliases, suppression windows, and calibration windows."""
    data = json.loads(REGISTRY_JSON.read_text())
    aliases = {str(k).strip(): str(v).strip() for k, v in data["aliases"].items()}
    zones = {
        _resolve_alias(str(k).strip(), aliases): str(v).strip()
        for k, v in data.get("station_timezones", {}).items()
    }

    suppressions = []
    for item in data["suppressions"]:
        suppressions.append(
            {
                "station_id": str(item["station_id"]).strip(),
                "start": _parse_instant(str(item["start"]))[0],
                "end": _parse_instant(str(item["end"]))[0],
            }
        )

    calibrations = []
    for index, item in enumerate(data["calibrations"]):
        calibrations.append(
            {
                "station_id": str(item["station_id"]).strip(),
                "start": _parse_instant(str(item["start"]))[0],
                "end": _parse_instant(str(item["end"]))[0],
                "offset_c": float(item["offset_c"]),
                "index": index,
            }
        )
    return aliases, zones, suppressions, calibrations


def _in_interval(instant: datetime, start: datetime, end: datetime) -> bool:
    """Return whether instant is in a start-inclusive, end-exclusive interval."""
    return start <= instant < end


def _is_suppressed(station_id: str, instant: datetime, suppressions: list[dict]) -> bool:
    """Return whether a station/instant falls inside any suppression window."""
    return any(
        item["station_id"] == station_id
        and _in_interval(instant, item["start"], item["end"])
        for item in suppressions
    )


def _calibrated_temperature(
    station_id: str, instant: datetime, temp: float, calibrations: list[dict]
) -> float:
    """Apply the latest-start matching calibration, breaking ties by file order."""
    matches = [
        item
        for item in calibrations
        if item["station_id"] == station_id
        and _in_interval(instant, item["start"], item["end"])
    ]
    if not matches:
        return temp
    chosen = max(matches, key=lambda item: (item["start"], item["index"]))
    return temp + chosen["offset_c"]


def _empty_counts() -> dict[str, int]:
    """Create a zeroed OK/WARN/EST quality-count mapping."""
    return {code: 0 for code in QUALITY_CODES}


def _reference_from_input() -> tuple[list[tuple[str, str, float, str]], dict]:
    """Recompute expected deduped rows and stats from CSV plus registry."""
    aliases, zones, suppressions, calibrations = _load_registry()
    skipped_malformed_rows = 0
    skipped_suppressed_rows = 0
    shifted_nonexistent_timestamps = 0
    valid_rows: list[tuple[str, str, float, str, datetime]] = []

    with INPUT_CSV.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            station_raw = (row.get("station_id") or "").strip()
            ts = (row.get("timestamp") or "").strip()
            temp_raw = (row.get("temperature_c") or "").strip()
            quality = (row.get("quality_code") or "").strip()

            if (
                not station_raw
                or not ts
                or not temp_raw
                or quality not in QUALITY_CODES
            ):
                skipped_malformed_rows += 1
                continue

            station_id = _resolve_alias(station_raw, aliases)
            instant, shifted = _parse_instant(ts, station_id, zones)
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
            if _is_suppressed(station_id, instant, suppressions):
                skipped_suppressed_rows += 1
                continue

            temp = _calibrated_temperature(station_id, instant, temp, calibrations)
            valid_rows.append((station_id, ts, temp, quality, instant))

    last_win: dict[tuple[str, datetime], tuple[str, str, float, str, datetime]] = {}
    for station_id, ts, temp, quality, instant in valid_rows:
        last_win[(station_id, instant)] = (station_id, ts, temp, quality, instant)

    deduped = list(last_win.values())
    deduped.sort(key=lambda row: (row[0], row[4]))

    by_station: dict[str, list[tuple[datetime, float, str]]] = {}
    global_counts = _empty_counts()
    for station_id, _ts, temp, quality, instant in deduped:
        by_station.setdefault(station_id, []).append((instant, temp, quality))
        global_counts[quality] += 1

    stations = []
    for station_id in sorted(by_station):
        readings = sorted(by_station[station_id], key=lambda row: row[0])
        temps = [temp for _instant, temp, _quality in readings]
        counts = _empty_counts()
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
                "min_temperature_c": _fmt_decimal(min(temps)),
                "max_temperature_c": _fmt_decimal(max(temps)),
                "median_temperature_c": _fmt_decimal(_median_from_values(temps)),
                "avg_temperature_c": _fmt_decimal(sum(temps) / len(temps)),
                "quality_counts": counts,
                "longest_gap_minutes": max(gaps) if gaps else None,
                "quality_runs": quality_runs,
            }
        )

    all_temps = [temp for _station_id, _ts, temp, _quality, _instant in deduped]
    stats = {
        "duplicate_rows_dropped": len(valid_rows) - len(deduped),
        "skipped_malformed_rows": skipped_malformed_rows,
        "skipped_suppressed_rows": skipped_suppressed_rows,
        "shifted_nonexistent_timestamps": shifted_nonexistent_timestamps,
        "deduped_row_count": len(deduped),
        "station_count": len(by_station),
        "median_temperature_c_all": _fmt_decimal(_median_from_values(all_temps)),
        "global_quality_counts": global_counts,
        "stations": stations,
    }
    out_rows = [(station_id, ts, temp, quality) for station_id, ts, temp, quality, _ in deduped]
    return out_rows, stats


def _assert_csv_matches(expected_rows: list[tuple[str, str, float, str]]) -> None:
    """Assert deduped.csv rows preserve winning timestamp and adjusted temperature."""
    with DEDUPED_CSV.open(newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == [
            "timestamp",
            "station_id",
            "temperature_c",
            "quality_code",
        ], "Unexpected CSV header"
        actual = [
            (
                row["station_id"].strip(),
                row["timestamp"].strip(),
                float(row["temperature_c"]),
                row["quality_code"].strip(),
            )
            for row in reader
        ]
    assert actual == expected_rows, "deduped.csv does not match expected registry dedupe"


def _assert_stats_matches(expected_stats: dict) -> None:
    """Assert stats.json exactly matches registry-aware recomputation."""
    data = json.loads(STATS_JSON.read_text())
    assert data == expected_stats, f"stats mismatch: {data!r} vs {expected_stats!r}"


def test_script_exists():
    """Verify /app/dedupe_report.py exists."""
    assert SCRIPT_PATH.is_file(), "Expected /app/dedupe_report.py"


def test_script_has_shebang():
    """Verify the script starts with the required Python shebang."""
    first = SCRIPT_PATH.read_text(encoding="utf-8").splitlines()[0]
    assert first == "#!/usr/bin/env python3", f"Unexpected shebang: {first!r}"


def test_script_is_executable():
    """Verify dedupe_report.py is executable as required by the task."""
    assert os.access(SCRIPT_PATH, os.X_OK), (
        "/app/dedupe_report.py must be chmod +x (executable bit)"
    )


def test_script_compiles():
    """Verify /app/dedupe_report.py is valid Python syntax."""
    proc = subprocess.run(
        ["python3", "-m", "py_compile", str(SCRIPT_PATH)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr


def test_script_rebuild_under_strace_respects_app_paths():
    """Run a clean rebuild under strace; forbid harness paths; only allowed /app opens."""
    if shutil.which("strace") is None:
        raise AssertionError("strace must be installed in the verifier image")

    shutil.rmtree("/app/output", ignore_errors=True)
    log_path = Path("/tmp/dedupe_strace.log")
    if log_path.exists():
        log_path.unlink()

    proc = subprocess.run(
        ["strace", "-f", "-o", str(log_path), "python3", str(SCRIPT_PATH)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=DEDUPE_SCRIPT_CWD,
    )
    assert proc.returncode == 0, (proc.stderr, proc.stdout)
    assert DEDUPED_CSV.is_file(), "Script did not create /app/output/deduped.csv"
    assert STATS_JSON.is_file(), "Script did not create /app/output/stats.json"

    log_text = log_path.read_text(errors="replace")
    raw_paths = _strace_collect_path_arguments(log_text)

    for raw in sorted(raw_paths):
        norm = _normalize_traced_path(raw, DEDUPE_SCRIPT_CWD)
        if not norm or norm == ".":
            continue
        assert "canary_decoy" not in norm, f"Must not open decoy paths: {raw!r}"
        for root in _STRACE_FORBIDDEN_PREFIXES:
            assert not (norm == root or norm.startswith(root + "/")), (
                f"Must not open benchmark harness paths under {root!r}: {raw!r} "
                f"(normalized: {norm!r})"
            )
        under_app = norm.startswith("/app/") or norm == "/app"
        if under_app:
            ok = (
                norm == "/app"
                or norm == "/app/dedupe_report.py"
                or norm == "/app/inputs/station_readings.csv"
                or norm == "/app/inputs/station_registry.json"
                or norm.startswith("/app/__pycache__/")
                or norm.startswith("/app/output/")
            )
            assert ok, f"Disallowed open under /app: {raw!r} (normalized: {norm!r})"
        else:
            allowed_non_app = any(
                norm == p.rstrip("/") or norm.startswith(p)
                for p in _STRACE_ALLOWED_NON_APP_PREFIXES
            )
            assert allowed_non_app, (
                f"Disallowed access outside /app (possible harness leak): {raw!r} "
                f"-> {norm!r}"
            )


def test_script_rebuild_matches_reference():
    """Deleting /app/output and running the script must reproduce both outputs."""
    shutil.rmtree("/app/output", ignore_errors=True)
    proc = subprocess.run(
        ["python3", str(SCRIPT_PATH)],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=DEDUPE_SCRIPT_CWD,
    )
    assert proc.returncode == 0, proc.stderr
    assert DEDUPED_CSV.is_file()
    assert STATS_JSON.is_file()

    expected_rows, expected_stats = _reference_from_input()
    _assert_csv_matches(expected_rows)
    _assert_stats_matches(expected_stats)


def test_deduped_csv_matches_reference():
    """Verify aliasing, suppression, calibration, last-wins dedupe, and sort order."""
    expected_rows, _ = _reference_from_input()
    _assert_csv_matches(expected_rows)


def test_stats_json_matches_reference():
    """Verify stats fields, quality counts, medians, averages, and longest gaps."""
    _, expected_stats = _reference_from_input()
    _assert_stats_matches(expected_stats)


def test_stats_schema():
    """Verify stats.json has required top-level keys and station entry shape."""
    data = json.loads(STATS_JSON.read_text())
    assert set(data.keys()) == {
        "duplicate_rows_dropped",
        "skipped_malformed_rows",
        "skipped_suppressed_rows",
        "shifted_nonexistent_timestamps",
        "deduped_row_count",
        "station_count",
        "median_temperature_c_all",
        "global_quality_counts",
        "stations",
    }
    for key in (
        "duplicate_rows_dropped",
        "skipped_malformed_rows",
        "skipped_suppressed_rows",
        "shifted_nonexistent_timestamps",
        "deduped_row_count",
        "station_count",
    ):
        assert isinstance(data[key], int)
    assert isinstance(data["median_temperature_c_all"], str)
    assert set(data["global_quality_counts"]) == set(QUALITY_CODES)
    assert all(isinstance(value, int) for value in data["global_quality_counts"].values())
    assert isinstance(data["stations"], list)
    for entry in data["stations"]:
        assert set(entry.keys()) == {
            "station_id",
            "readings",
            "min_temperature_c",
            "max_temperature_c",
            "median_temperature_c",
            "avg_temperature_c",
            "quality_counts",
            "longest_gap_minutes",
            "quality_runs",
        }
        assert isinstance(entry["readings"], int)
        assert set(entry["quality_counts"]) == set(QUALITY_CODES)
        assert all(isinstance(value, int) for value in entry["quality_counts"].values())
        assert entry["longest_gap_minutes"] is None or isinstance(
            entry["longest_gap_minutes"], int
        )
        assert isinstance(entry["quality_runs"], int)
        for key in (
            "min_temperature_c",
            "max_temperature_c",
            "median_temperature_c",
            "avg_temperature_c",
        ):
            assert isinstance(entry[key], str)
