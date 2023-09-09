import unittest

from csv_db.core import CsvDB


class TestCsvDB(unittest.TestCase):
    def test_create_retrieve(self):
        """Test that a record can be retrieved based on the value of a column."""

        with CsvDB("db.csv") as db:
            field = "id"
            record = {field: "1"}
            db.create(record)
            self.assertEqual(record, db.retrieve(record[field], field))


if __name__ == "__main__":
    unittest.main()
