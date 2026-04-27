I am getting double-counted station readings under `/app` after a registry update. 
Write `/app/dedupe_report.py` to rebuild a clean dataset and a stats report.

The first line must be exactly `#!/usr/bin/env python3`, and the script must be executable. 
The harness runs from `/app`. While running, your script may only read `/app/inputs/station_readings.csv` and `/app/inputs/station_registry.json`. 
It must write `/app/output/deduped.csv` and `/app/output/stats.json` (create `/app/output` if needed). 
Do not read or import anything under `/tests`, `/oracle`, `/solution`, or `/logs`.

The CSV columns are `timestamp`, `station_id`, `temperature_c`, `quality_code`. 
The file may start with a UTF-8 BOM, so open it with BOM-safe UTF-8 decoding (for example `encoding="utf-8-sig"`). 
For each row, strip ASCII whitespace from every field. If the stripped `timestamp` or `station_id` is empty, if `temperature_c` is blank / not a float / not finite, if `quality_code` is not exactly `OK`, `WARN`, or `EST`, or if the timestamp cannot be parsed as described below, skip the row and increment `skipped_malformed_rows`.

Use only the Python standard library. 
Do not use dynamic code execution (`eval`, `exec`, `compile`, or `__import__`) and do not rely on third-party helper libraries like numpy/pandas/pytz/dateutil.

Resolve the row’s station id through the registry alias map before any timezone work. 
Aliases can chain. If you hit a cycle, collapse it to the lexicographically smallest id in the cycle. 
Use this canonical id for timezone lookup, suppression/calibration lookup, the dedupe key, and outputs.

You should assume hidden tests include large input files, so the implementation should be reasonably efficient (avoid quadratic passes over all rows).

Parse timestamps with `datetime.fromisoformat` (rewrite a trailing `Z` to `+00:00` first). 
If the parsed datetime is timezone-aware, convert it to UTC. If it is naive, interpret it as local wall time in the canonical station’s timezone using `zoneinfo` and the registry’s `station_timezones` mapping (default `UTC` if missing). If the local time is ambiguous (fall-back overlap), choose the later UTC instant. If the local time does not exist (spring-forward gap), nudge the naive time forward one minute at a time until it becomes valid; if any nudging happened for that row, increment `shifted_nonexistent_timestamps` by 1. Do this gap-shift before suppression checks so the shift counter still increments even if the row is later suppressed.

Timezone keys in `station_timezones` may themselves be aliases. When building the canonical station id → timezone mapping, resolve each key to a canonical id. If multiple `station_timezones` entries resolve to the same canonical id, the later entry in the registry file wins.

After you have the final UTC instant, apply these steps in order. First, suppression: if the instant is inside any suppression window for that canonical station, skip the row and increment `skipped_suppressed_rows`. Suppression windows are UTC half-open intervals (start inclusive, end exclusive). Second, calibration: if not suppressed and the instant is inside any calibration window (also half-open), add its `offset_c` to the temperature. If multiple calibration windows match, choose the one with the latest start time; if still tied, choose the later entry in the registry file. Third, deduplicate: key is (canonical station id, final UTC instant), and the last row in file order wins. Build both outputs from this deduped set.

Write `/app/output/deduped.csv` with header `timestamp,station_id,temperature_c,quality_code`. For each kept row, write the winner’s stripped input timestamp string, the canonical station id, the calibrated temperature formatted as `str(round(x, 1))` (but write `-0.0` as `0.0`), and the quality code. Sort rows by station_id (string order) then by UTC instant.

Write `/app/output/stats.json` as JSON with exactly these top-level keys: `duplicate_rows_dropped`, `skipped_malformed_rows`, `skipped_suppressed_rows`, `shifted_nonexistent_timestamps`, `deduped_row_count`, `station_count`, `median_temperature_c_all`, `global_quality_counts`, `stations`. The first six are integers. `duplicate_rows_dropped` is how many non-suppressed, well-formed rows were dropped by deduplication. `median_temperature_c_all` is a one-decimal string using the same temperature formatting rules as the CSV. `global_quality_counts` must be an object with exactly `OK`, `WARN`, `EST` integer counts. `stations` must be a list sorted by station_id.

Each station entry must have exactly these keys: `station_id`, `readings`, `min_temperature_c`, `max_temperature_c`, `median_temperature_c`, `avg_temperature_c`, `quality_counts`, `longest_gap_minutes`, `quality_runs`. Compute medians by sorting numeric values and averaging the two middle values when the count is even. `avg_temperature_c` is the mean (this exact key). `quality_runs` is 1 plus the number of times the quality code changes when walking kept readings in UTC order (a single reading gives 1). `longest_gap_minutes` is the largest whole-minute gap between consecutive kept UTC instants, or null if there is only one kept reading. Format all station temperature strings with the same one-decimal formatting and map `-0.0` to `0.0`.
