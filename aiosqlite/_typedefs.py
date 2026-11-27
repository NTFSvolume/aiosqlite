from __future__ import annotations

import sqlite3
import sys
from collections.abc import Callable, Mapping
from typing import Any, Literal, Protocol, TypeAlias, TypedDict, TypeVar

_T_co = TypeVar("_T_co", covariant=True)


class SupportsLenAndGetItem(Protocol[_T_co]):
    def __len__(self) -> int: ...
    def __getitem__(self, k: int, /) -> _T_co: ...


SqliteData: TypeAlias = str | int | float | None
AdaptedInputData: TypeAlias = SqliteData | Any

Parameters: TypeAlias = (
    SupportsLenAndGetItem[AdaptedInputData] | Mapping[str, AdaptedInputData]
)

RowFactoryOptions: TypeAlias = (
    type[sqlite3.Row] | Callable[[sqlite3.Cursor, tuple[Any, ...]], object] | None
)

IsolationLevel = Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None


class _ConnectionParamsP311(TypedDict, total=False):
    timeout: float
    detect_types: int
    isolation_level: IsolationLevel
    check_same_thread: bool
    factory: type[sqlite3.Connection]
    cached_statements: int
    uri: bool


if sys.version_info >= (3, 12):

    class ConnectionParams(_ConnectionParamsP311, total=False):
        autocommit: bool

else:
    ConnectionParams = _ConnectionParamsP311
