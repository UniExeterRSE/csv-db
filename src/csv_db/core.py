import contextlib
import csv
from collections.abc import Collection
from types import TracebackType
from typing import Any, Optional


class CsvDB(contextlib.AbstractContextManager):
    def __init__(self, path: str, fields: Collection[str]):
        self._records = []
        self._fields = fields

    def __enter__(self):
        return super().__enter__()

    def __exit__(
        self,
        __exc_type: type[BaseException] | None,
        __exc_value: BaseException | None,
        __traceback: TracebackType | None,
    ) -> bool | None:
        return super().__exit__(__exc_type, __exc_value, __traceback)

    def create(self, record: dict[str, Any]):
        missing_fields = ", ".join([f"'{k}'" for k in self._fields if k not in record])
        if missing_fields:
            raise ValueError(
                f"Argument 'record' missing the following fields: {missing_fields}."
            )

        self._records.append({k: str(v) for k, v in record.items()})

    def retrieve(self, value: Any, field: str):
        return self._records[0]


class CsvFile(object):
    def __init__(self, path: str, header: Optional[Collection[str]] = None):
        self._path = path
        self._header = header
        self._create()

    def _create(self):
        with open(self._path, mode="w", newline="") as csvfile:
            if self._header is not None:
                csv.writer(csvfile).writerow(self._header)
