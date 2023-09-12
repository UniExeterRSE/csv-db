from __future__ import annotations

import csv
import pathlib
from collections.abc import Callable, Collection, Iterator
from typing import Any, Iterable, Optional, TypeAlias

Record: TypeAlias = dict[str, str]


class CsvDB(object):
    def __init__(self, path: str, fields: Collection[str]):
        self._path = pathlib.Path(path)
        self._fields = self._make_fields(fields)

    def _make_fields(self, fields: Collection[str]) -> _Fields:
        try:
            _fields = _Fields(fields)
        except RepeatedFieldsError as exc:
            msg = (
                f"Argument 'fields' contains repeated fields: {exc.repeated_fields_str}."
            )
            raise exc.__class__(msg, repeated_fields=exc.repeated_fields)

        if not self._path.exists():
            return _fields

        try:
            with open(self._path, mode="r") as csvfile:
                file_fields = _Fields(csvfile.readline().strip().split(","))
        except RepeatedFieldsError as exc:
            msg = f"Database file {self._path} contains repeated fields: {exc.repeated_fields_str}."
            raise exc.__class__(msg, repeated_fields=exc.repeated_fields)

        if not _fields == file_fields:
            raise FieldsMismatchError(
                f"'fields' does not agree with the fields defined in {self._path}"
            )

        return _fields

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
    ) -> list[Record]:
        if not self._path.exists():
            return []
        with open(self._path, mode="r", newline="") as csvfile:
            try:
                return list(filter(predicate_fn, self._make_data_reader(csvfile)))
            except KeyError as exc:
                raise DatabaseLookupError(
                    "Bad 'predicate_fn': attempted to look up a field not in the database."
                ) from exc
            except Exception as exc:
                raise exc.__class__(f"Bad 'predicate_fn': {exc}") from exc

    def update(self, value: Any, field: str, record: dict[str, Any]) -> None:
        self._validate_field(field)
        records = self.query()
        field_values = [rec[field] for rec in records]
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
    def __init__(self, *args, repeated_fields: tuple[str] = None):
        super().__init__(*args)
        self._repeated_fields = repeated_fields
        self._repeated_fields_str = ", ".join(f"'{f}'" for f in repeated_fields)

    @property
    def repeated_fields(self):
        return self._repeated_fields

    @property
    def repeated_fields_str(self):
        return self._repeated_fields_str


class DatabaseLookupError(Exception):
    pass


class _Fields(tuple):
    def __new__(cls, iterable: Iterable[str]):
        return super().__new__(cls, iterable)

    def __init__(self, iterable: Iterable[str]):
        self._check_repeated_fields()

    def _check_repeated_fields(self) -> None:
        repeated_fields = sorted({f for f in self if self.count(f) > 1})
        if repeated_fields:
            raise RepeatedFieldsError(repeated_fields=repeated_fields)

    def __eq__(self, other: _Fields):
        return set(self) == set(other)
