import json
import tempfile
import unittest
from pathlib import Path
from pipeline import validate, distance_m, run


def row(identifier='1', name='Park', lat='0', lon='0'):
    return dict(id=identifier, name=name, latitude=lat, longitude=lon)


class QualityTests(unittest.TestCase):
    def test_sample_and_repeat_run(self):
        with tempfile.TemporaryDirectory() as directory:
            for _ in range(2):
                result = run(Path(__file__).parents[1] / 'data/places.csv', directory)
                self.assertEqual((result['input_rows'], result['accepted_rows'], result['rejected_rows']), (10, 5, 5))
                self.assertEqual(len(result['duplicate_candidates']), 1)
                self.assertEqual(json.loads((Path(directory)/'report.json').read_text()), result)

    def test_bad_coordinates(self):
        for lat, lon in [('NaN','0'), ('inf','0'), ('0','181'), ('-91','0'), ('','0')]:
            self.assertEqual(validate([row(lat=lat, lon=lon)])[1][0]['rule'], 'invalid_coordinates')

    def test_multiple_rules_and_id_reservation(self):
        clean, issues, _ = validate([row(name='', lat='bad'), row()])
        self.assertEqual(clean, [])
        self.assertEqual({i['rule'] for i in issues}, {'missing_name', 'invalid_coordinates', 'duplicate_id'})

    def test_date_line_and_name_normalization(self):
        self.assertLess(distance_m((0,179.9999),(0,-179.9999)), 25)
        self.assertEqual(len(validate([row(lon='179.9999'), row('2',' PARK ',lon='-179.9999')])[2]), 1)

    def test_empty_and_radius(self):
        self.assertEqual(validate([]), ([], [], []))
        for radius in [-1, float('nan'), float('inf')]:
            with self.assertRaises(ValueError):
                validate([], radius)

    def test_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'bad.csv'
            path.write_text('name\nPark\n')
            with self.assertRaises(ValueError):
                run(path, Path(directory)/'output')


if __name__ == '__main__':
    unittest.main()
