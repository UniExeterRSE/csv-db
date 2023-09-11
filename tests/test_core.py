import os
import tempfile
import unittest
from typing import Iterable

from csv_db.core import CsvDB, DatabaseLookupError, FieldsMismatchError


def exact(string: str):
    """Turn a string into a regular expressions that defines an exact match on the
    string.
    """
    escaped = string
    for char in ["\\", "(", ")"]:
        escaped = escaped.replace(char, "\\" + char)

    return "^" + escaped + "$"


def write_csv_row(path, data: Iterable[str], mode="x"):
    """Write data to a csv file as a comma-separated row."""
    with open(path, mode=mode, newline="") as f:
        f.write(",".join(data) + "\n")


class TestCsvDB(unittest.TestCase):
    def setUp(self) -> None:
        # Temp directory for holding csv file during a unit test run
        self._dir = tempfile.TemporaryDirectory()
        self.tmp_dir = self._dir.name

        # An empty database with fields and record to add to it
        self.path = os.path.join(self.tmp_dir, "db.csv")
        self.pkey = "id"
        self.col1 = "col1"
        self.fields = [self.pkey, self.col1]
        self.db = CsvDB(self.path, self.fields)
        self.record = {self.pkey: "1", self.col1: "a"}

        # Database with two records in it
        self.path2 = os.path.join(self.tmp_dir, "db2.csv")
        self.db2 = CsvDB(self.path2, self.fields)
        self.record2 = {self.pkey: "2", self.col1: "b"}
        self.db2.create(self.record)
        self.db2.create(self.record2)

        # Database where order of fields differs from that of the underlying file
        self.path3 = os.path.join(self.tmp_dir, "db3.csv")
        write_csv_row(self.path3, self.fields)
        self.dbrev = CsvDB(self.path3, list(reversed(self.fields)))

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_initialise_existing_file(self):
        """Test that a database can be initialised on an existing csv file having
        the correct fields."""

        write_csv_row(self.path, self.fields)
        _ = CsvDB(self.path, self.fields)

    def test_initialise_existing_file_fields_have_diff_order(self):
        """Test that a database can be initialised on an existing csv file having
        the correct fields but in a different order to those supplied."""

        write_csv_row(self.path, reversed(self.fields))
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

        write_csv_row(self.path, ["a", "b"])
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

        record = {self.pkey: 1, self.col1: 2}
        self.db.create(record)
        self.assertEqual(
            {self.pkey: "1", self.col1: "2"},
            self.db.retrieve(record[self.pkey], self.pkey),
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

        record2 = {self.pkey: "2", self.col1: "b"}
        self.db.create(self.record)
        self.db.create(record2)
        for record in [self.record, record2]:
            self.assertEqual(record, self.db.retrieve(record[self.pkey], self.pkey))

    def test_create_multiple_records_csv_content(self):
        """Test that the csv file has the expected content when multple records are
        created sequentially."""

        record2 = {self.pkey: "2", self.col1: "b"}
        self.db.create(self.record)
        self.db.create(record2)
        with open(self.path, mode="r", newline=None) as csvfile:
            expected = (
                [",".join(self.fields) + "\n"]
                + [",".join([self.record[self.pkey], self.record[self.col1]]) + "\n"]
                + [",".join([record2[self.pkey], record2[self.col1]]) + "\n"]
            )
            self.assertEqual(expected, csvfile.readlines())

    def test_create_add_record_to_existing_file(self):
        """Test that a record can be added to an existing database file when
        initialised as a new database."""

        # Create initial file
        self.db.create(self.record)

        # Create new database from same file and add record
        db = CsvDB(self.path, self.fields)
        record2 = {self.pkey: "2", self.col1: "b"}
        db.create(record2)

        self.assertEqual(self.record, db.retrieve(self.record[self.pkey], self.pkey))
        self.assertEqual(record2, db.retrieve(record2[self.pkey], self.pkey))

    def test_create_add_records_to_existing_file_fields_have_diff_order(self):
        """Test that a record can be added to a database in which the fields are in a
        different order to those supplied at initialisation."""

        self.dbrev.create(self.record)
        self.assertEqual(
            self.record, self.dbrev.retrieve(self.record[self.pkey], self.pkey)
        )

    def test_retrieve_no_file_return_none(self):
        """Test that ``None`` is returned if the csv file behind the database hasn't
        been created yet."""

        self.assertIsNone(self.db.retrieve("2", self.pkey))

    def test_retrieve_no_records_in_db(self):
        """Test that no record is returned when the database has no records in it."""

        write_csv_row(self.path, self.fields)
        self.assertEqual(None, self.db.retrieve("1", self.pkey))

    def test_retrieve_no_record_return_none(self):
        """Test that ``None`` is returned if no record exists with the given field/value
        combination. (Also tests that the header row isn't included in the search.)"""

        self.db.create(self.record)
        self.assertIsNone(self.db.retrieve(self.pkey, self.pkey))

    def test_retrieve_missing_field_error(self):
        """Test that a DatabaseLookupError is raised if the field provided does not match
        a field in the database."""

        field = "not-present"
        with self.assertRaisesRegexp(
            DatabaseLookupError,
            exact(f"'{field}' does not define a field in the database."),
        ):
            self.db2.retrieve("1", field)

    def test_update_replaces_correct_record(self):
        """Test that the record with the specified field value gets updated and no other
        records get updated."""

        lookup_val = self.record2[self.pkey]
        updated_record = {self.pkey: lookup_val, self.col1: "c"}
        self.db2.update(lookup_val, self.pkey, updated_record)
        self.assertEqual(updated_record, self.db2.retrieve(lookup_val, self.pkey))
        self.assertEqual(
            self.record, self.db2.retrieve(self.record[self.pkey], self.pkey)
        )

    def test_update_replaces_record_convert_value_to_str(self):
        """Test that the record with the specified field value gets updated after
        converting the value to a string."""

        self.db.create(self.record)
        new_record = {self.pkey: "1", self.col1: "b"}
        self.db.update(1, self.pkey, new_record)
        self.assertEqual(new_record, self.db.retrieve(new_record[self.pkey], self.pkey))

    def test_update_replaces_record_existing_file_fields_have_diff_order(self):
        """Test that a record can be updated when the fields in the database file are in a
        different order to those supplied at initialisation."""

        self.dbrev.create(self.record)
        new_record = {self.pkey: "1", self.col1: "b"}
        self.dbrev.update(1, self.pkey, new_record)
        self.assertEqual(
            new_record, self.dbrev.retrieve(new_record[self.pkey], self.pkey)
        )

    def test_update_missing_field_error(self):
        """Test that a DatabaseLookupError is raised if the field provided does not match
        a field in the database."""

        field = "not-present"
        with self.assertRaisesRegexp(
            DatabaseLookupError,
            exact(f"'{field}' does not define a field in the database."),
        ):
            self.db2.update("1", field, self.record)

    def test_update_cannot_find_record_error(self):
        """Test that a DatabaseLookupError is raised if the key/value combination supplied to
        update cannot be found in the database."""

        val = "-1"
        with self.assertRaisesRegexp(
            DatabaseLookupError,
            exact(f"Could not find record with {self.pkey} = {val}."),
        ):
            self.db2.update(val, self.pkey, self.record)

    def test_update_no_records_in_db_error(self):
        """Test that a DatabaseLookupError is raised if there are no records in the
        database. (Also tests that the header row is not included in the search when
        updating.)"""

        write_csv_row(self.path, self.fields)
        val = self.pkey
        with self.assertRaisesRegexp(
            DatabaseLookupError,
            exact(f"Could not find record with {self.pkey} = {val}."),
        ):
            self.db.update(val, self.pkey, self.record)

    def test_query_no_file(self):
        """Test that no records are returned by the query if the database csv file hasn't
        been created yet."""

        self.assertEqual([], self.db.query())

    def test_query_no_records_in_db(self):
        """Test that no records are returned when the database has no records in it."""

        write_csv_row(self.path, self.fields)
        self.assertEqual([], self.db.query())

    def test_query_no_arg_return_all_records(self):
        """Test that all records from the database are returned when no predicate
        function is supplied."""

        expected = [self.record, self.record2]
        self.assertEqual(expected, self.db2.query())

    def test_query_existing_file_fields_have_diff_order(self):
        """Test that records are returned when the fields in the database file are in a
        different order to those supplied at initialisation."""

        self.dbrev.create(self.record)
        self.assertEqual([self.record], self.dbrev.query())


if __name__ == "__main__":
    unittest.main()
