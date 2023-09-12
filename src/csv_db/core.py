from __future__ import annotations

import csv
import pathlib
from collections.abc import Callable, Collection, Iterator
from typing import Any, Optional, TypeAlias

Record: TypeAlias = dict[str, str]


class CsvDB(object):
    def __init__(self, path: str, fields: Collection[str]):
        self._path = pathlib.Path(path)
        self._fields = fields
        self._fields_arg = "fields"
        self._validate_db_initialisation()

    def _validate_db_initialisation(self) -> None:
        if len(self._fields) == 0:
            raise ValueError(
                f"Argument '{self._fields_arg}' defines an empty collection."
            )

        if self._has_missing_field_names(self._fields):
            raise ValueError(f"Argument '{self._fields_arg}' contains empty field names.")

        if repeated_fields := self._make_repeated_fields_str(self._fields):
            raise RepeatedFieldsError(
                f"Argument '{self._fields_arg}' contains repeated fields: {repeated_fields}."
            )

        if not self._path.exists():
            return

        with open(self._path, mode="r") as csvfile:
            file_fields = tuple(csvfile.readline().strip().split(","))

        if self._has_missing_field_names(file_fields):
            raise MissingFieldsError(
                f"Database file {self._path} contains empty field names."
            )

        if repeated_fields := self._make_repeated_fields_str(file_fields):
            raise RepeatedFieldsError(
                f"Database file {self._path} contains repeated fields: {repeated_fields}."
            )

        if not set(self._fields) == set(file_fields):
            raise FieldsMismatchError(
                f"'{self._fields_arg}' does not agree with the fields defined in {self._path}."
            )

    @staticmethod
    def _has_missing_field_names(fields: Collection[str]):
        return any(f == "" for f in fields)

    @staticmethod
    def _make_repeated_fields_str(fields: Collection[str]):
        return ", ".join(sorted({f"'{f}'" for f in fields if fields.count(f) > 1}))

    def create(self, record: dict[str, Any]):
        missing_fields = ", ".join([f"'{k}'" for k in self._fields if k not in record])
        if missing_fields:
            raise ValueError(
                f"Argument 'record' missing the following fields: {missing_fields}."
            )
        mode = "a" if self._path.exists() else "x"
        with open(self._path, mode=mode, newline="") as csvfile:
            writer = csv.DictWriter(csvfile, self._fields)
            if mode == "x":
                writer.writeheader()
            writer.writerow(record)

    def retrieve(self, value: Any, field: str) -> Optional[Record]:
        self._validate_field(field)
        if not self._path.exists():
            return None

        with open(self._path, mode="r", newline="") as csvfile:
            reader = self._make_data_reader(csvfile)
            for row in reader:
                if row[field] == str(value):
                    return row

    def _validate_field(self, field: str):
        if field not in self._fields:
            raise DatabaseLookupError(
                f"'{field}' does not define a field in the database."
            )

    def _make_data_reader(self, csvfile: Iterator[Record]):
        reader = csv.DictReader(csvfile, self._fields)
        _ = next(reader)
        return reader

    def query(
        self, predicate_fn: Optional[Callable[[Record], bool]] = None
    ) -> tuple[Record]:
        if not self._path.exists():
            return tuple()
        with open(self._path, mode="r", newline="") as csvfile:
            try:
                return tuple(filter(predicate_fn, self._make_data_reader(csvfile)))
            except KeyError as exc:
                raise DatabaseLookupError(
                    "Bad 'predicate_fn': attempted to look up a field not in the database."
                ) from exc
            except Exception as exc:
                raise exc.__class__(f"Bad 'predicate_fn': {exc}") from exc

    def update(self, value: Any, field: str, record: dict[str, Any]) -> None:
        self._validate_field(field)
        records = list(self.query())
        field_values = tuple(rec[field] for rec in records)
        try:
            records[field_values.index(str(value))] = record
        except ValueError as exc:
            raise DatabaseLookupError(
                f"Could not find record with {field} = {value}."
            ) from exc

        with open(self._path, mode="w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, self._fields)
            writer.writeheader()
            writer.writerows(records)


class FieldsMismatchError(Exception):
    pass


class RepeatedFieldsError(Exception):
    pass


class MissingFieldsError(Exception):
    pass


class DatabaseLookupError(Exception):
    pass
