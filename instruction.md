Duplicate weather station rows live in /app/inputs/station_readings.csv. 
The file is UTF-8 and may begin with a UTF-8 BOM; the header row is timestamp, station_id, temperature_c. 
Temperatures are Celsius as decimals.

Implement a Python 3 program at /app/dedupe_report.py (include #!/usr/bin/env python3 and make it executable). 
It must read input only from /app/inputs/station_readings.csv (no other path may be opened for reading under /app except the script file itself and whatever Python creates under /app/__pycache__ if applicable). 
Skip any data row where station_id or timestamp is empty after stripping surrounding ASCII whitespace, or temperature_c is missing/blank/whitespace-only, or temperature_c parses with float() but is not finite (reject nan and infinities), or the timestamp cannot be interpreted as specified below; count all such skips in skipped_malformed_rows (integer). 
Only non-skipped rows participate in dedupe.

Timestamp interpretation (must match exactly): take the timestamp field after strip. 
If it ends with an ASCII uppercase Z only, replace that suffix with +00:00 before parsing. 
Parse with datetime.fromisoformat from the Python standard library. 
If the result has no timezone, treat the wall time as UTC. 
Otherwise convert to UTC with astimezone. 
Any ValueError from fromisoformat counts as malformed. 
The dedupe key is (station_id.strip(), utc_instant) where utc_instant is that UTC-aware datetime. 
When two rows share the same key, keep the last row in file order; the written timestamp and station_id columns are the stripped strings from that winning row (temperature is its float value). 
Write /app/output/deduped.csv with columns timestamp, station_id, temperature_c and no duplicate keys. 
Sort rows by station_id ascending using Python’s default string ordering on the written station_id, then by utc_instant chronological ascending (not lexicographic on timestamp strings).

Also write /app/output/stats.json as a JSON object with: duplicate_rows_dropped (non-skipped input rows minus distinct dedupe keys after last-wins), skipped_malformed_rows, deduped_row_count, station_count, median_temperature_c_all (string: median of every retained temperature after dedupe, pooling all stations), and stations (array sorted by station_id ascending on the written ids). 
Each station entry is {"station_id": "<id>", "readings": <int>, "min_temperature_c": "<string>", "max_temperature_c": "<string>", "median_temperature_c": "<string>", "avg_temperature_c": "<string>"} from that station’s retained temperatures after dedupe. Median: sort floats ascending; odd count → middle element; even → mean of the two middle elements. 
Format min, max, median, and avg with exactly one digit after the decimal using Python round(x, 1) then str, mapping -0.0 to "0.0" if it appears.

Create /app/output if it is missing.
