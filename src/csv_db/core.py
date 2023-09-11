import csv
import pathlib
from collections.abc import Collection
from typing import Any, Literal


class CsvDB(object):
    def __init__(self, path: str, fields: Collection[str]):
        self._path = pathlib.Path(path)
        self._fields = fields
        self._csvfile = None
        self._verify_fields()

    def _verify_fields(self) -> None:
        if not self._path.exists():
            return None
        with open(self._path, mode="r") as csvfile:
            fields = csvfile.readline().strip().split(",")
        if not set(self._fields) == set(fields):
            raise FieldsMismatchError(
                f"'fields' does not agree with the fields defined in {self._path}"
            )

    def create(self, record: dict[str, Any]):
        missing_fields = ", ".join([f"'{k}'" for k in self._fields if k not in record])
        if missing_fields:
            raise ValueError(
                f"Argument 'record' missing the following fields: {missing_fields}."
            )
        mode = "a" if self._path.exists() else "x"
        try:
            self._open(mode=mode)
            writer = csv.DictWriter(self._csvfile, fieldnames=self._fields)
            if mode == "x":
                writer.writeheader()
            writer.writerow(record)
        finally:
            self._close()

    def _open(self, mode: Literal["a", "x"]):
        self._csvfile = open(self._path, mode=mode, newline="")

    def _close(self):
        if self._csvfile is not None:
            self._csvfile.close()

    def retrieve(self, value: Any, field: str):
        with open(self._path, mode="r", newline="") as csvfile:
            for row in csv.DictReader(csvfile, self._fields):
                try:
                    if row[field] == str(value):
                        return row
                except KeyError:
                    raise DatabaseLookupError(
                        f"'{field}' does not define a field in the database."
                    )

    def update(self, value: Any, field: str, record: dict[str, Any]) -> None:
        with open(self._path, mode="r", newline="") as csvfile:
            records = list(csv.DictReader(csvfile, self._fields))

        try:
            field_values = [rec[field] for rec in records]
            records[field_values.index(str(value))] = record
        except KeyError:
            raise DatabaseLookupError(
                f"'{field}' does not define a field in the database."
            )
        except ValueError:
            raise DatabaseLookupError(f"Could not find record with {field} = {value}.")

        with open(self._path, mode="w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, self._fields)
            writer.writeheader()
            writer.writerows(records)


class FieldsMismatchError(Exception):
    pass


class DatabaseLookupError(Exception):
    pass
