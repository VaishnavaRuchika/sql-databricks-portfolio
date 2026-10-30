# Geospatial Data Quality Pipeline

A runnable Python + SQL portfolio project that validates point data and identifies possible duplicate places. Uses only the Python standard library; no account, API key, or package installation is required. Sample locations and names are synthetic and do not represent employer data.

## Run locally

Use Python 3.10 or later. From this project directory:

```bash
python pipeline.py
python -m unittest discover -s tests -v
```

For your own CSV:

```bash
python pipeline.py --input data/places.csv --output output --radius-m 50
```

Required headers: `id,name,latitude,longitude`. Coordinates are decimal degrees in WGS84. Extra columns are ignored. Input errors such as missing headers fail the run; individual invalid rows are quarantined in the report.

## What the pipeline does

1. Reads a CSV and verifies its schema.
2. Rejects blank IDs, repeated IDs, blank names, and invalid or nonfinite coordinates.
3. Normalizes accepted names with Unicode case folding and whitespace normalization.
4. Compares same-name records using Haversine distance; pairs within the configured radius become review candidates.
5. Rebuilds SQLite tables and produces a JSON quality report. Repeated runs replace outputs rather than appending duplicate records.

Outputs: `output/quality.sqlite` contains accepted `places` and row-level `issues`; `output/report.json` contains counts, rule summaries, violations, and duplicate candidates. `sql/quality_summary.sql` runs against the database and groups violations by rule. Rejected rows may violate more than one rule, so issue counts can exceed rejected-row counts.

## Reproducible sample result

The included 10 rows produce **5 accepted rows, 5 rejected rows, and 1 possible duplicate pair** (`p1` and `p2`, approximately 11.12 m apart). These are sample results, not production performance claims.

## Design decisions and limits

- Duplicate IDs follow a strict first-seen policy: a later ID is rejected even when its first occurrence was invalid. This makes identifier conflicts visible rather than silently choosing a winner.
- Near duplicates are candidates for human review. The pipeline never merges them automatically; nearby same-name locations can be legitimate.
- Matching is exact after name normalization. Spelling variations, transliterations, addresses, and polygons are outside this version's scope.
- Haversine uses a spherical Earth approximation and handles the date line; it is unsuitable for survey-grade measurements.
- Memory use is O(n); comparisons are O(sum of squared name-group sizes), O(n²) in the worst case. This version targets small CSV batches. Spatial indexing and streamed ingestion would be appropriate next steps for larger datasets.
- Output directories are regenerated for each run. Use separate directories to preserve previous reports. SQLite and JSON writes are not an atomic transaction across both files.

## Interview walkthrough

Explain the input contract, why invalid records are quarantined, why duplicate detection produces candidates rather than deletions, how SQL distinguishes violations from affected rows, and the scaling tradeoff of grouping by name before comparing distances. Run the sample and inspect its output before describing this as a project you understand.
