# Copyright Amethyst Reese
# Licensed under the MIT license
from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator, Callable, Iterable
from typing import TYPE_CHECKING, ParamSpec, Self, TypeVar

from typing_extensions import deprecated

from ._typedefs import Parameters, RowFactoryOptions

if TYPE_CHECKING:
    from .core import Connection

_T = TypeVar("_T")
_P = ParamSpec("_P")


class Cursor:
    def __init__(self, conn: Connection, cursor: sqlite3.Cursor) -> None:
        self.iter_chunk_size = conn._iter_chunk_size
        self._conn: Connection = conn
        self._cursor: sqlite3.Cursor = cursor

    def __repr__(self) -> str:
        return f"{type(self).__name__}(iter_chunk_size={self.iter_chunk_size!r}, _conn={self._conn!r}, _cursor={self._cursor!r})"

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    def __aiter__(self) -> AsyncIterator[sqlite3.Row]:
        """The cursor proxy is also an async iterator."""
        return self._fetch_chunked()

    async def _fetch_chunked(self) -> AsyncIterator[sqlite3.Row]:
        while rows := await self.fetchmany(self.iter_chunk_size):
            for row in rows:
                yield row

    async def _execute(
        self, fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
    ) -> _T:
        """Execute the given function on the shared connection's thread."""
        return await self._conn._execute(fn, *args, **kwargs)

    @deprecated("Use Cursor.aclose()")
    async def close(self) -> None:
        """Close the cursor."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the cursor."""
        await self._execute(self._cursor.close)

    async def execute(self, sql: str, parameters: Parameters | None = None) -> Cursor:
        """Execute the given query."""
        if parameters is None:
            parameters = ()
        await self._execute(self._cursor.execute, sql, parameters)
        return self

    async def executemany(self, sql: str, parameters: Iterable[Parameters]) -> Cursor:
        """Execute the given multiquery."""
        await self._execute(self._cursor.executemany, sql, parameters)
        return self

    async def executescript(self, sql_script: str) -> Cursor:
        """Execute a user script."""
        await self._execute(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> sqlite3.Row | None:
        """Fetch a single row."""
        return await self._execute(self._cursor.fetchone)

    async def fetchmany(self, size: int | None = None) -> list[sqlite3.Row]:
        """Fetch up to `cursor.arraysize` number of rows."""
        args = (size,) if size is not None else ()
        return await self._execute(self._cursor.fetchmany, *args)

    async def fetchall(self) -> list[sqlite3.Row]:
        """Fetch all remaining rows."""
        return await self._execute(self._cursor.fetchall)

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int | None:
        return self._cursor.lastrowid

    @property
    def arraysize(self) -> int:
        return self._cursor.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._cursor.arraysize = value

    @property
    def description(self) -> tuple[tuple[str, None, None, None, None, None, None], ...]:
        return self._cursor.description

    @property
    def row_factory(self) -> RowFactoryOptions:
        return self._cursor.row_factory

    @row_factory.setter
    def row_factory(self, factory: RowFactoryOptions) -> None:
        self._cursor.row_factory = factory

    @property
    def connection(self) -> Connection:
        return self._conn
