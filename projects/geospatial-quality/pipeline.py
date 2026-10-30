"""Dependency-free point-data validation and SQL reporting."""
import argparse
import csv
import json
import math
import sqlite3
from pathlib import Path

REQUIRED = {'id', 'name', 'latitude', 'longitude'}


def distance_m(a, b):
    """Great-circle distance, including points across the date line."""
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2-lat1)/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin((lon2-lon1)/2)**2
    return 6371008.8 * 2 * math.asin(math.sqrt(min(1, max(0, h))))


def validate(rows, radius=50):
    if not math.isfinite(radius) or radius < 0:
        raise ValueError('Radius must be finite and nonnegative')
    issues, clean, seen = [], [], set()
    for number, row in enumerate(rows, 2):
        reasons = []
        identifier = (row.get('id') or '').strip()
        name = (row.get('name') or '').strip()
        if not identifier:
            reasons.append('missing_id')
        elif identifier in seen:
            reasons.append('duplicate_id')
        if identifier:
            seen.add(identifier)
        if not name:
            reasons.append('missing_name')
        try:
            lat, lon = float(row['latitude']), float(row['longitude'])
            if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            reasons.append('invalid_coordinates')
        if reasons:
            issues.extend({'row': number, 'id': identifier, 'rule': rule} for rule in reasons)
        else:
            clean.append({'row': number, 'id': identifier, 'name': name, 'latitude': lat, 'longitude': lon})
    # Names narrow candidate comparisons; distance verifies candidates.
    groups, pairs = {}, []
    for place in clean:
        key = ' '.join(place['name'].casefold().split())
        for other in groups.get(key, []):
            distance = distance_m((place['latitude'], place['longitude']), (other['latitude'], other['longitude']))
            if distance <= radius:
                pairs.append({'id_a': other['id'], 'id_b': place['id'], 'distance_m': round(distance, 2)})
        groups.setdefault(key, []).append(place)
    return clean, issues, pairs


def run(source, output, radius=50):
    with Path(source).open(newline='', encoding='utf-8-sig') as handle:
        reader = csv.DictReader(handle)
        if not REQUIRED.issubset(reader.fieldnames or []):
            raise ValueError('CSV requires: ' + ', '.join(sorted(REQUIRED)))
        rows = list(reader)
    clean, issues, pairs = validate(rows, radius)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(output / 'quality.sqlite') as db:
        db.executescript('DROP TABLE IF EXISTS places; DROP TABLE IF EXISTS issues; CREATE TABLE places (source_row INTEGER, id TEXT, name TEXT, latitude REAL, longitude REAL); CREATE TABLE issues (source_row INTEGER, id TEXT, rule TEXT);')
        db.executemany('INSERT INTO places VALUES (:row,:id,:name,:latitude,:longitude)', clean)
        db.executemany('INSERT INTO issues VALUES (:row,:id,:rule)', issues)
        query = Path(__file__).with_name('sql').joinpath('quality_summary.sql').read_text()
        summary = [dict(zip(['rule', 'issue_count', 'affected_rows'], row)) for row in db.execute(query)]
    report = {'input_rows': len(rows), 'accepted_rows': len(clean), 'rejected_rows': len(rows)-len(clean), 'radius_m': radius, 'issues': issues, 'rule_summary': summary, 'duplicate_candidates': pairs}
    (output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: report[k] for k in ('input_rows', 'accepted_rows', 'rejected_rows')}))
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=Path(__file__).parent / 'data' / 'places.csv')
    parser.add_argument('--output', type=Path, default=Path('output'))
    parser.add_argument('--radius-m', type=float, default=50)
    args = parser.parse_args()
    run(args.input, args.output, args.radius_m)
