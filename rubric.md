Agent creates executable /app/dedupe_report.py with the exact required shebang and rebuilds both output files under /app/output, +3
Agent confines input and output access to the allowed /app paths and avoids reading tests, oracle, solution, logs, or decoy files, +5
Agent parses the CSV with BOM-safe decoding, trims fields, rejects malformed rows, and maintains skipped_malformed_rows accurately, +3
Agent resolves chained aliases and alias cycles before timezone, suppression, calibration, dedupe, and output decisions, +3
Agent converts aware and naive timestamps to UTC correctly, including alias-resolved timezones, ambiguous fall-back times, and nonexistent spring-forward times, +3
Agent increments shifted_nonexistent_timestamps before suppression when a nonexistent local timestamp is shifted and later suppressed, +3
Agent applies suppression, calibration, and last-row-wins deduplication in the required order using half-open UTC intervals and latest-start calibration tie-breaking, +5
Agent writes deduped.csv with canonical station ids, preserved winning timestamp text, required sorting, and one-decimal temperature formatting with -0.0 mapped to 0.0, +3
Agent writes stats.json with the exact schema, integer counters, global counts, sorted station entries, medians, averages, quality_runs, and longest_gap_minutes computed from deduped UTC-ordered rows, +3
Agent uses data structures that keep large input processing reasonably efficient instead of repeatedly scanning all rows for each row, +2
Agent manually runs python3 /app/dedupe_report.py and inspects both generated output files before finishing, +2
Agent imports forbidden third-party timezone or data-processing libraries, uses dynamic execution, or reads harness-only paths, -5
Agent computes suppression, calibration, dedupe, quality_runs, or longest_gap_minutes in file order when UTC order is required, -3
Agent counts shifted nonexistent timestamps only after suppression or fails to count shifted rows that are later suppressed, -3
Agent emits malformed CSV or JSON, omits required keys, adds extra keys, or writes outputs only to stdout, -3
Agent hard-codes bundled fixture outputs instead of implementing a general transformation for changed inputs, -5
