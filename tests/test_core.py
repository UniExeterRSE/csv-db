import unittest

from csv_db.core import CsvDB


class TestCsvDB(unittest.TestCase):
    def setUp(self) -> None:
        self.file_name = "db.csv"
        self.pkey = "id"

    def test_create_retrieve(self):
        """Test that a record can be retrieved based on the value of a column."""

        with CsvDB(self.file_name) as db:
            record = {self.pkey: "1"}
            db.create(record)
            self.assertEqual(record, db.retrieve(record[self.pkey], self.pkey))

    def test_create_take_non_string_values(self):
        """Test that records can be created and retrieved based on non-string
        values."""

        with CsvDB(self.file_name) as db:
            record = {self.pkey: 1}
            db.create(record)
            self.assertEqual({self.pkey: "1"}, db.retrieve(record[self.pkey], self.pkey))


if __name__ == "__main__":
    unittest.main()
