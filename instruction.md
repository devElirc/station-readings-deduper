I have duplicate weather station rows in /app/inputs/station_readings.csv (header: timestamp, station_id, temperature_c). 
Timestamps are ISO-8601 strings; temperatures are Celsius as decimals.

Implement a Python 3 program at /app/dedupe_report.py (include #!/usr/bin/env python3 and make it executable). 
It should read that CSV only, dedupe by (station_id, timestamp) keeping the last row in file order when keys repeat, then write /app/output/deduped.csv with the same three columns and no duplicate keys. 
Rows must be sorted by station_id ascending (string sort), then timestamp ascending.

Also write /app/output/stats.json as a JSON object with: duplicate_rows_dropped (integer count of rows discarded because a later row replaced the same station+timestamp), station_count (number of distinct station_id values in the deduped data), and stations (array sorted by station_id ascending). 
Each array element is {"station_id": "<id>", "readings": <int count of rows for that station after dedupe>, "avg_temperature_c": "<string>"} where avg_temperature_c is the mean of that station’s retained temperature_c values, formatted with exactly one digit after the decimal point using Python’s round(x, 1) semantics, then str so -0.0 becomes "0.0" if it ever appears.

Create /app/output if it is missing. Do not read any path other than /app/inputs/station_readings.csv for input.
