import contextlib
from types import TracebackType
from typing import Any


class CsvDB(contextlib.AbstractContextManager):
    def __init__(self, path: str):
        self._records = []

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
        self._records.append(record)

    def retrieve(self, value: Any, field: str):
        return self._records[0]
