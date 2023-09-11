import os
import tempfile
import unittest

from csv_db.core import CsvDB, DatabaseLookupError, FieldsMismatchError


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
        self._dir = tempfile.TemporaryDirectory()
        self.tmp_dir = self._dir.name
        self.path = os.path.join(self.tmp_dir, "db.csv")
        self.fields = ["id", "col1"]
        self.pkey = "id"
        self.db = CsvDB(self.path, self.fields)
        self.record = {self.pkey: "1", "col1": "a"}

        # Database with two records in it
        self.path2 = os.path.join(self.tmp_dir, "db2.csv")
        self.db2 = CsvDB(self.path2, self.fields)
        self.record2 = {self.pkey: "2", "col1": "b"}
        self.db2.create(self.record)
        self.db2.create(self.record2)

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_initialise_existing_file(self):
        """Test that a database can be initialised on an existing csv file having
        the correct fields."""

        with open(self.path, mode="x", newline="") as f:
            f.write(",".join(self.fields) + "\n")

        _ = CsvDB(self.path, self.fields)

    def test_initialise_existing_file_fields_have_diff_order(self):
        """Test that a database can be initialised on an existing csv file having
        the correct fields but in a different order to those supplied."""

        with open(self.path, mode="x", newline="") as f:
            f.write(",".join(reversed(self.fields)) + "\n")

        _ = CsvDB(self.path, self.fields)

    @unittest.skip("Not yet implemented")
    def test_initialise_existing_file_repeated_fields_error(self):
        """Test that a RepeatedFieldsError is raised if a database is initialised
        on an existing csv file which contains repeated fields."""
        # TODO

    @unittest.skip("Not yet implemented")
    def test_initialise_repeated_fields_error(self):
        """Test that a RepeatedFieldsError is raised if the fields supplied at
        initialisation contain repeats."""
        # TODO

    def test_initialise_existing_file_wrong_header_error(self):
        """Test that an FieldsMismatchError is raised if a database is initialised on
        an existing csv file that has a different header to the one specified."""

        with open(self.path, mode="x", newline="") as f:
            f.write("a,b\n")

        with self.assertRaisesRegex(
            FieldsMismatchError,
            exact(f"'fields' does not agree with the fields defined in {self.path}"),
        ):
            _ = CsvDB(self.path, self.fields)

    def test_create_retrieve(self):
        """Test that a record can be retrieved based on the value of a column."""

        self.db.create(self.record)
        self.assertEqual(self.record, self.db.retrieve(self.record[self.pkey], self.pkey))

    def test_create_makes_csv_with_header(self):
        """Test that a header row is written in the csv file when a first record is
        created."""

        self.db.create(self.record)
        with open(self.path, mode="r", newline=None) as csvfile:
            expected = ",".join(self.fields) + "\n"
            self.assertEqual(expected, csvfile.readline())

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

        db = CsvDB(self.path, ["a", "b"])
        record = {self.pkey: "1"}
        with self.assertRaisesRegex(
            ValueError, "^Argument 'record' missing the following fields: 'a', 'b'.$"
        ):
            db.create(record)

    def test_create_multiple_records(self):
        """Test that multiple records can be added to the database sequentially."""

        record2 = {self.pkey: "2", "col1": "b"}
        self.db.create(self.record)
        self.db.create(record2)
        for record in [self.record, record2]:
            self.assertEqual(record, self.db.retrieve(record[self.pkey], self.pkey))

    def test_create_multiple_records_csv_content(self):
        """Test that the csv file has the expected content when multple records are
        created sequentially."""

        record2 = {self.pkey: "2", "col1": "b"}
        self.db.create(self.record)
        self.db.create(record2)
        with open(self.path, mode="r", newline=None) as csvfile:
            expected = (
                [",".join(self.fields) + "\n"]
                + [",".join([self.record[self.pkey], self.record["col1"]]) + "\n"]
                + [",".join([record2[self.pkey], record2["col1"]]) + "\n"]
            )
            self.assertEqual(expected, csvfile.readlines())

    def test_create_add_record_to_existing_file(self):
        """Test that a record can be added to an existing database file when
        initialised as a new database."""

        # Create initial file
        self.db.create(self.record)

        # Create new database from same file and add record
        db = CsvDB(self.path, self.fields)
        record2 = {self.pkey: "2", "col1": "b"}
        db.create(record2)

        self.assertEqual(self.record, db.retrieve(self.record[self.pkey], self.pkey))
        self.assertEqual(record2, db.retrieve(record2[self.pkey], self.pkey))

    def test_create_add_records_to_existing_file_fields_have_diff_order(self):
        """Test that a record can be added to a database in which the fields are in a
        different order to those supplied at initialisation."""

        with open(self.path, mode="x", newline="") as f:
            f.write(",".join(self.fields) + "\n")

        db = CsvDB(self.path, list(reversed(self.fields)))
        db.create(self.record)
        self.assertEqual(self.record, db.retrieve(self.record[self.pkey], self.pkey))

    def test_update_replaces_correct_record(self):
        """Test that the record with the specified field value gets updated and no other
        records get updated."""

        lookup_val = self.record2[self.pkey]
        updated_record = {self.pkey: lookup_val, "col1": "c"}
        self.db2.update(lookup_val, self.pkey, updated_record)
        self.assertEqual(updated_record, self.db2.retrieve(lookup_val, self.pkey))
        self.assertEqual(
            self.record, self.db2.retrieve(self.record[self.pkey], self.pkey)
        )

    def test_update_replaces_record_convert_value_to_str(self):
        """Test that the record with the specified field value gets updated after
        converting the value to a string."""

        self.db.create({self.pkey: "1", "col1": "a"})
        new_record = {self.pkey: "1", "col1": "b"}
        self.db.update(1, self.pkey, new_record)
        self.assertEqual(new_record, self.db.retrieve(new_record[self.pkey], self.pkey))

    def test_update_cannot_find_record_error(self):
        """Test that a DatabaseLookupError is raised if the key/value combination supplied to
        update cannot be found in the database."""

        val = "-1"
        with self.assertRaisesRegexp(
            DatabaseLookupError,
            exact(f"Could not find record with {self.pkey} = {val}."),
        ):
            self.db2.update(val, self.pkey, self.record)


if __name__ == "__main__":
    unittest.main()
