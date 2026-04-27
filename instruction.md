We’re getting double-counted station readings under `/app` after a registry update. Please write a small Python script at `/app/dedupe_report.py` that rebuilds a clean, deduplicated dataset plus summary statistics.

Create /app/dedupe_report.py and make sure it can run as an executable script.

The first line must be exactly #!/usr/bin/env python3.

The test runner will run the script from the /app folder.

When the script runs, it should only read /app/inputs/station_readings.csv and /app/inputs/station_registry.json.

The script should save the output files as /app/output/deduped.csv and /app/output/stats.json.

Create the /app/output folder if it does not already exist.

Do not read from or import anything inside /tests, /oracle, /solution, or /logs.

/app/inputs/station_readings.csv contains four columns: timestamp, station_id, temperature_c, and quality_code.

The CSV file may start with a UTF-8 BOM. Open it with BOM-safe UTF-8 decoding, such as encoding="utf-8-sig", so the BOM does not become part of the first column name.

For each row, remove extra ASCII whitespace from every field.

If timestamp or station_id is empty after trimming, skip that row and increase skipped_malformed_rows.

If temperature_c is empty, not a valid float, or not a finite number, skip that row and increase skipped_malformed_rows.

If quality_code is not exactly OK, WARN, or EST, skip that row and increase skipped_malformed_rows.

If the timestamp cannot be parsed using the timestamp rules below, skip that row and increase skipped_malformed_rows.

Before doing any timezone work, convert the input station_id to its canonical station id using the registry aliases map.

Alias chains may have more than one step.

If an alias chain has a cycle, use the smallest station id in that cycle by lexicographic order as the canonical id.

Use the canonical id for everything else: timezone lookup, suppression/calibration lookup, dedupe key, and all outputs.

Keys in `station_timezones` may be aliases too. When building the mapping from canonical station id → IANA zone name, resolve each timezone key to a canonical id. If multiple `station_timezones` entries resolve to the same canonical id, the **later** entry in the registry file wins.

Parse each timestamp with datetime.fromisoformat.

Before parsing, replace a trailing Z with +00:00.

If the parsed timestamp already has timezone information, convert it to UTC.

If the timestamp has no timezone information, treat it as local time for that canonical station.

Use zoneinfo and the station timezone from the registry’s station_timezones map.

If the station has no timezone in the registry, use UTC.

If the local time is ambiguous during a fall-back time change, choose the later UTC time.

If the local time does not exist during a spring-forward time change, move the time forward one minute at a time until it becomes valid.

If a row needed this time adjustment, increase shifted_nonexistent_timestamps by 1 for that row.

Do this time adjustment before checking suppression windows.

So even if the row is later skipped because of suppression, it should still count in shifted_nonexistent_timestamps.

A shifted timestamp should not count as malformed unless another validation rule already failed.

After you have the final UTC time, process the row in this order.

First, check suppression.

If the UTC time falls inside a suppression window for that canonical station, skip the row and increase skipped_suppressed_rows.

Suppression windows use UTC half-open ranges, meaning the start time is included and the end time is not included.

Second, apply calibration if the row was not suppressed.

If the UTC time falls inside a calibration window, add that window’s offset_c value to the temperature.

Calibration windows also use UTC half-open ranges.

If more than one calibration window matches, use the one with the latest start time.

If there is still a tie, use the later entry from the registry file.

Third, deduplicate the rows.

Use (canonical station id, final UTC time) as the dedupe key.

If multiple rows have the same key, keep the last one from the CSV file.

Build both output files from this final deduplicated data.
Write the cleaned CSV file to /app/output/deduped.csv.

The CSV header must be exactly:

timestamp,station_id,temperature_c,quality_code

Keep the columns in that same order.

For each kept row, write the cleaned input timestamp, the canonical station id, the calibrated temperature, and the quality code.

Format the temperature with str(round(x, 1)).

If the formatted temperature becomes -0.0, write it as 0.0.

Sort the CSV rows by station_id first, then by the final UTC time.

Write the summary JSON file to /app/output/stats.json.

The JSON must contain exactly these top-level fields:

duplicate_rows_dropped, skipped_malformed_rows, skipped_suppressed_rows, shifted_nonexistent_timestamps, deduped_row_count, station_count, median_temperature_c_all, global_quality_counts, and stations.

The first six fields must be integers.

Format median_temperature_c_all as a one-decimal string, using the same temperature format as the CSV.

global_quality_counts must include exactly three counts: OK, WARN, and EST.

The stations list must be sorted by station_id.

Each station item must contain exactly these fields:

station_id, readings, min_temperature_c, max_temperature_c, median_temperature_c, avg_temperature_c, quality_counts, longest_gap_minutes, and quality_runs.

For each station, calculate the median by sorting the temperatures.

If the station has an even number of readings, average the two middle values.

avg_temperature_c must be the mean temperature.

quality_runs should count how many continuous quality-code groups the station has in UTC order.

A station with one reading has quality_runs equal to 1.

longest_gap_minutes should be the largest full-minute gap between two consecutive UTC readings.

If a station has only one reading, set longest_gap_minutes to null.

Format all station temperature fields with the same one-decimal format, and write -0.0 as 0.0.
