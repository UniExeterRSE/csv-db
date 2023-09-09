import unittest

from csv_db.core import CsvDB


class TestCsvDB(unittest.TestCase):
    def setUp(self) -> None:
        self.file_name = "db.csv"
        self.fields = ["id", "col1"]
        self.pkey = "id"
        self.record = {self.pkey: "1", "col1": "a"}

    def test_create_retrieve(self):
        """Test that a record can be retrieved based on the value of a column."""

        with CsvDB(self.file_name, self.fields) as db:
            db.create(self.record)
            self.assertEqual(self.record, db.retrieve(self.record[self.pkey], self.pkey))

    def test_create_take_non_string_values(self):
        """Test that records can be created and retrieved based on non-string
        values."""

        with CsvDB(self.file_name, self.fields) as db:
            record = {self.pkey: 1, "col1": 2}
            db.create(record)
            self.assertEqual(
                {self.pkey: "1", "col1": "2"}, db.retrieve(record[self.pkey], self.pkey)
            )

    def test_create_error_missing_field(self):
        """Test that a ValueError is raised if the record submitted for creation has
        a field missing."""

        with CsvDB(self.file_name, self.fields) as db:
            record = {self.pkey: "1"}
            with self.assertRaisesRegex(
                ValueError, "^Argument 'record' missing the following fields: 'col1'.$"
            ):
                db.create(record)

    def test_create_error_missing_fields_multiple(self):
        """Test that a ValueError is raised if the record submitted for creation has
        multiple fields missing."""

        with CsvDB(self.file_name, ["a", "b"]) as db:
            record = {self.pkey: "1"}
            with self.assertRaisesRegex(
                ValueError, "^Argument 'record' missing the following fields: 'a', 'b'.$"
            ):
                db.create(record)


if __name__ == "__main__":
    unittest.main()
