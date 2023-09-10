import contextlib
import csv
import pathlib
from collections.abc import Collection
from types import TracebackType
from typing import Any, Literal, Optional


class CsvDB(object):
    def __init__(self, path: str, fields: Collection[str]):
        self._path = path
        self._fields = fields

    def create(self, record: dict[str, Any]):
        missing_fields = ", ".join([f"'{k}'" for k in self._fields if k not in record])
        if missing_fields:
            raise ValueError(
                f"Argument 'record' missing the following fields: {missing_fields}."
            )

        with open(self._path, mode="w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self._fields)
            writer.writeheader()
            writer.writerow(record)

    def retrieve(self, value: Any, field: str):
        with open(self._path, mode="r", newline="") as csvfile:
            for row in csv.DictReader(csvfile, self._fields):
                if row[field] == str(value):
                    return row


class CsvFile(contextlib.AbstractContextManager):
    def __init__(self, path: str, header: Optional[Collection[str]] = None):
        self._path = self._check_new_path(path)
        self._header = header
        self._file = None

    @staticmethod
    def _check_new_path(path: str):
        if pathlib.Path(path).exists():
            raise FileExistsError(
                f"Could not create csv file at {path}: file already exists"
            )
        return path

    def __enter__(self) -> type["CsvFile"]:
        return super().__enter__()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        return super().__exit__(__exc_type, __exc_value, __traceback)

    def open(self, mode: Literal["r", "w"] = "r"):
        is_new_file = not pathlib.Path(self._path).exists()
        self._file = open(self._path, mode=mode, newline="")
        if is_new_file and self._header is not None:
            csv.writer(self._file).writerow(self._header)

    def close(self):
        if self._file is not None:
            self._file.close()
