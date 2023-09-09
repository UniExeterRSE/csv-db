import os
import pathlib
import tempfile
import unittest

from csv_db.core import CsvDB, CsvFile


def exact(string: str):
    """Turn a string into a regular expressions that defines an exact match on the
    string.
    """
    escaped = string
    for char in ["\\", "(", ")"]:
        escaped = escaped.replace(char, "\\" + char)

    return "^" + escaped + "$"


class TestCsvDB(unittest.TestCase):
    def setUp(self) -> None:
        self.file_name = "db.csv"
        self.fields = ["id", "col1"]
        self.pkey = "id"
        self.db = CsvDB(self.file_name, self.fields)
        self.record = {self.pkey: "1", "col1": "a"}

    def test_create_retrieve(self):
        """Test that a record can be retrieved based on the value of a column."""

        self.db.create(self.record)
        self.assertEqual(self.record, self.db.retrieve(self.record[self.pkey], self.pkey))

    def test_create_take_non_string_values(self):
        """Test that records can be created and retrieved based on non-string
        values."""

        record = {self.pkey: 1, "col1": 2}
        self.db.create(record)
        self.assertEqual(
            {self.pkey: "1", "col1": "2"}, self.db.retrieve(record[self.pkey], self.pkey)
        )

    def test_create_error_missing_field(self):
        """Test that a ValueError is raised if the record submitted for creation has
        a field missing."""

        record = {self.pkey: "1"}
        with self.assertRaisesRegex(
            ValueError, "^Argument 'record' missing the following fields: 'col1'.$"
        ):
            self.db.create(record)

    def test_create_error_missing_fields_multiple(self):
        """Test that a ValueError is raised if the record submitted for creation has
        multiple fields missing."""

        with CsvDB(self.file_name, ["a", "b"]) as db:
            record = {self.pkey: "1"}
            with self.assertRaisesRegex(
                ValueError, "^Argument 'record' missing the following fields: 'a', 'b'.$"
            ):
                db.create(record)


class TestCsvFile(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.tmp_dir = self._dir.name
        self.path = os.path.join(self.tmp_dir, "file.csv")

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_open_creates_file(self):
        """Test that a file is created when the file is opened for writing."""

        csvfile = CsvFile(self.path)
        try:
            csvfile.open(mode="w")
            self.assertTrue(pathlib.Path(self.path).exists())
        finally:
            csvfile.close()

    def test_open_creates_file_header(self):
        """Test that a csv file is created with a specified header
        when the file is opened for writing."""

        header = ["a", "b"]
        csvfile = CsvFile(self.path, header)
        try:
            csvfile.open(mode="w")
        finally:
            csvfile.close()

        with open(self.path, mode="r", newline=None) as f:
            expected = ",".join(header) + "\n"
            self.assertEqual(expected, f.read())

    def test_initialise_file_exists_error(self):
        """Test that a FileExistsError is raised if a file already exists
        at the given path when initialising."""

        pathlib.Path(self.path).touch()
        with self.assertRaisesRegexp(
            FileExistsError,
            exact(f"Could not create csv file at {self.path}: file already exists"),
        ):
            CsvFile(self.path)

    def test_context_manager(self):
        """Test that CsvFile instance can be used as a context manager."""

        with CsvFile(self.path):
            pass


if __name__ == "__main__":
    unittest.main()
