# Copyright Amethyst Reese
# Licensed under the MIT license

"""Core implementation of aiosqlite proxies"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import logging
import os
import sqlite3
from collections.abc import AsyncIterator, Callable, Generator, Iterable
from functools import partial
from queue import Empty, Queue, SimpleQueue
from threading import Thread
from typing import TYPE_CHECKING, Any, ParamSpec, Self, TypeVar, TypeVarTuple, Unpack
from warnings import warn

from typing_extensions import Sentinel, deprecated

from .context import contextmanager
from .cursor import Cursor

if TYPE_CHECKING:
    from types import TracebackType

    from ._typedefs import (
        AuthorizerCallback,
        ConnectionParams,
        IsolationLevel,
        Parameters,
        RowFactoryOptions,
    )

__all__ = ["Connection", "Cursor", "connect"]


LOG = logging.getLogger("aiosqlite")


_T = TypeVar("_T")
_P = ParamSpec("_P")
_STOP_RUNNING_SENTINEL = Sentinel("_STOP_RUNNING_SENTINEL")
_Msg = tuple[asyncio.Future | None, Callable[[], Any], contextvars.Context]
_TxQueue = SimpleQueue[_Msg]
_Ts = TypeVarTuple("_Ts")


def _set_result(fut: asyncio.Future, result: Any) -> None:
    """Set the result of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_result(result)


def _set_exception(fut: asyncio.Future, e: BaseException) -> None:
    """Set the exception of a future if it hasn't been set already."""
    if not fut.done():
        fut.set_exception(e)


def _call_soon_threadsafe(
    loop: asyncio.AbstractEventLoop, callback: Callable[[*_Ts], object], *args: *_Ts
) -> None:
    if loop.is_closed():
        return
    try:
        loop.call_soon_threadsafe(callback, *args)
    except RuntimeError:
        # The loop can close between the check and scheduling from this thread.
        if not loop.is_closed():
            raise


def _execute(future: asyncio.Future | None, function: Callable[[], Any]) -> bool:
    LOG.debug("executing %s", function)
    try:
        result = function()
    except BaseException as e:  # noqa: BLE001
        LOG.debug("returning exception %s", e)
        if future is not None:
            _call_soon_threadsafe(future.get_loop(), _set_exception, future, e)
        return False
    else:
        LOG.debug("operation %s completed", function)
        if future is not None:
            _call_soon_threadsafe(future.get_loop(), _set_result, future, result)
        return result is _STOP_RUNNING_SENTINEL


def _connection_worker_thread(tx: _TxQueue) -> None:
    """Execute function calls on a separate thread."""
    while True:
        # Continues running until all queue items are processed,
        # even after connection is closed (so we can finalize all
        # futures)

        future, function, ctx = tx.get()
        try:
            if ctx.run(_execute, future, function):
                break
        finally:
            # Do not retain request-local values while waiting for the next item.
            del ctx, function, future


@dataclasses.dataclass(slots=True, kw_only=True)
class ConnectionCache:
    isolation_level: IsolationLevel
    row_factory: RowFactoryOptions
    text_factory: Callable[[bytes], Any]


class Connection:
    def __init__(
        self,
        connector: Callable[[], sqlite3.Connection],
        iter_chunk_size: int,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._running = True
        self._connection: sqlite3.Connection | None = None
        self._connector = connector
        self._tx: _TxQueue = SimpleQueue()
        self._iter_chunk_size = iter_chunk_size
        self._thread = Thread(target=_connection_worker_thread, args=(self._tx,))
        self._cache: ConnectionCache

        if loop is not None:
            warn(
                "aiosqlite.Connection no longer uses the `loop` parameter",
                DeprecationWarning,
                stacklevel=2,
            )

    def __repr__(self) -> str:
        field_names = ("_running", "_connection", "_thread", "_iter_chunk_size")
        fields = (f"{name}={getattr(self, name)!r}" for name in field_names)
        return f"<{type(self).__name__}({', '.join(fields)}>"

    def __del__(self) -> None:
        if self._connection is None:
            return

        warn(
            (
                f"{self!r} was deleted before being closed. "
                "Please use 'async with' to make sure the connection is closed properly."
            ),
            ResourceWarning,
            stacklevel=1,
        )

        # Don't try to be creative here, the event loop may have already been closed.
        # Simply stop the worker thread, and let the underlying sqlite3 connection
        # be finalized by its own __del__.
        self.stop()

    def _open(self) -> None:
        # Keep ownership on the worker even if the awaiting task is cancelled.
        self._connection = c = self._connector()
        self._cache = ConnectionCache(
            isolation_level=c.isolation_level,
            row_factory=c.row_factory,
            text_factory=c.text_factory,
        )

    def stop(self) -> asyncio.Future | None:
        """Stop the background thread. Prefer `async with` or `await close()`"""
        if not self._running:
            return None
        self._running = False

        def close_and_stop():
            if self._connection is not None:
                self._connection.close()
                self._connection = None
            return _STOP_RUNNING_SENTINEL

        try:
            future = asyncio.get_event_loop().create_future()
        except Exception:  # noqa: BLE001
            future = None

        self._tx.put_nowait((future, close_and_stop, contextvars.copy_context()))
        return future

    @deprecated(
        "Please use 'async with' to make sure the connection is closed properly"
    )
    def __await__(self) -> Generator[Any, None, Self]:
        return self.__aenter__().__await__()

    async def __aenter__(self) -> Self:
        """Connect to the actual sqlite database."""
        if self._connection is None:
            future = asyncio.get_event_loop().create_future()
            self._tx.put_nowait((future, self._open, contextvars.copy_context()))

            try:
                self._thread.start()
                await future
            except BaseException:
                self.stop()
                raise

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            if self._connection is not None:
                await self._execute(self._conn.__exit__, exc_type, exc_val, exc_tb)
        finally:
            await self.aclose()

    @deprecated("Use Connection.aclose()")
    async def close(self) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Complete queued queries/cursors and close the connection."""
        if self._connection is None:
            return

        try:
            await self._execute(self._conn.close)
        except Exception:
            LOG.info("exception occurred while closing connection")
            raise
        finally:
            self._connection = None
            future = self.stop()
            if future:
                await future

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._connection is None:
            raise ValueError("no active connection")

        return self._connection

    def _get_cache(self) -> ConnectionCache:
        _ = self._conn
        return self._cache

    def _execute(
        self, fn: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs
    ) -> asyncio.Future[_T]:
        """Queue a function with the given arguments for execution."""
        if not self._running or not self._connection:
            raise ValueError("Connection closed")

        function = partial(fn, *args, **kwargs)
        future = asyncio.get_event_loop().create_future()
        self._tx.put_nowait((future, function, contextvars.copy_context()))
        return future

    def _execute_insert(self, sql: str, parameters: Parameters) -> sqlite3.Row | None:
        with contextlib.closing(self._conn.execute(sql, parameters)) as cursor:
            return cursor.execute("SELECT last_insert_rowid()").fetchone()

    def _execute_fetchall(self, sql: str, parameters: Parameters) -> list[sqlite3.Row]:
        with contextlib.closing(self._conn.execute(sql, parameters)) as cursor:
            return cursor.fetchall()

    def _execute_fetchone(self, sql: str, parameters: Parameters) -> sqlite3.Row | None:
        with contextlib.closing(self._conn.execute(sql, parameters)) as cursor:
            return cursor.fetchone()

    @contextmanager
    async def cursor(self) -> Cursor:
        """Create an aiosqlite cursor wrapping a sqlite3 cursor object."""
        return Cursor(self, await self._execute(self._conn.cursor))

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self._execute(self._conn.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self._execute(self._conn.rollback)

    @contextmanager
    async def execute(self, sql: str, parameters: Parameters | None = None) -> Cursor:
        """Helper to create a cursor and execute the given query."""
        if parameters is None:
            parameters = ()
        cursor = await self._execute(self._conn.execute, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def execute_insert(
        self, sql: str, parameters: Parameters | None = None
    ) -> sqlite3.Row | None:
        """Helper to insert and get the last_insert_rowid."""
        if parameters is None:
            parameters = ()
        return await self._execute(self._execute_insert, sql, parameters)

    @contextmanager
    async def execute_fetchall(
        self, sql: str, parameters: Parameters | None = None
    ) -> list[sqlite3.Row]:
        """Helper to execute a query and return all the data."""
        if parameters is None:
            parameters = ()
        return await self._execute(self._execute_fetchall, sql, parameters)

    @contextmanager
    async def execute_fetchone(
        self, sql: str, parameters: Parameters | None = None
    ) -> sqlite3.Row | None:
        if parameters is None:
            parameters = ()
        return await self._execute(self._execute_fetchone, sql, parameters)

    @contextmanager
    async def executemany(self, sql: str, parameters: Iterable[Parameters]) -> Cursor:
        """Helper to create a cursor and execute the given multiquery."""
        cursor = await self._execute(self._conn.executemany, sql, parameters)
        return Cursor(self, cursor)

    @contextmanager
    async def executescript(self, sql_script: str) -> Cursor:
        """Helper to create a cursor and execute a user script."""
        cursor = await self._execute(self._conn.executescript, sql_script)
        return Cursor(self, cursor)

    async def interrupt(self) -> None:
        """Interrupt pending queries."""
        return self._conn.interrupt()

    async def create_function(
        self,
        name: str,
        num_params: int,
        func: Callable,
        deterministic: bool = False,
    ) -> None:
        """Create user-defined function that can be later used
        within SQL statements. Must be run within the same thread
        that query executions take place so instead of executing directly
        against the connection, we defer this to `run` function.

        If ``deterministic`` is true, the created function is marked as deterministic,
        which allows SQLite to perform additional optimizations. This flag is supported
        by SQLite 3.8.3 or higher, ``NotSupportedError`` will be raised if used with
        older versions.
        """
        await self._execute(
            self._conn.create_function,
            name,
            num_params,
            func,
            deterministic=deterministic,
        )

    @property
    def in_transaction(self) -> bool:
        return self._conn.in_transaction

    @property
    def isolation_level(self) -> IsolationLevel:
        return self._get_cache().isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel) -> None:
        self._get_cache().isolation_level = value

        def update():
            self._conn.isolation_level = value

        self._execute(update)

    @property
    def row_factory(self) -> RowFactoryOptions:
        return self._get_cache().row_factory

    @row_factory.setter
    def row_factory(self, factory: RowFactoryOptions) -> None:
        self._get_cache().row_factory = factory

        def update():
            self._conn.row_factory = factory

        self._execute(update)

    @property
    def text_factory(self) -> Callable[[bytes], Any]:
        return self._get_cache().text_factory

    @text_factory.setter
    def text_factory(self, factory: Callable[[bytes], Any]) -> None:
        self._get_cache().text_factory = factory

        def update():
            self._conn.text_factory = factory

        self._execute(update)

    @property
    def total_changes(self) -> int:
        return self._conn.total_changes

    async def enable_load_extension(self, value: bool) -> None:
        await self._execute(self._conn.enable_load_extension, value)

    async def load_extension(self, path: str) -> None:
        await self._execute(self._conn.load_extension, path)

    async def set_progress_handler(
        self,
        handler: Callable[[], int | None],
        n: int,
    ) -> None:
        await self._execute(self._conn.set_progress_handler, handler, n)

    async def set_trace_callback(self, handler: Callable) -> None:
        await self._execute(self._conn.set_trace_callback, handler)

    async def set_authorizer(
        self,
        authorizer_callback: AuthorizerCallback | None,
    ) -> None:
        """Set an authorizer callback to control database access.

        The authorizer callback is invoked for each SQL statement that is prepared,
        and controls whether specific operations are permitted.

        Example::

            import sqlite3

            def restrict_drops(action_code, arg1, arg2, db_name, trigger_name):
                # Deny all DROP operations
                if action_code == sqlite3.SQLITE_DROP_TABLE:
                    return sqlite3.SQLITE_DENY
                # Allow everything else
                return sqlite3.SQLITE_OK

            await conn.set_authorizer(restrict_drops)

        See ``sqlite3`` documentation for details:
        https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.set_authorizer

        :param authorizer_callback: An optional callable that receives five arguments:

            - ``action_code`` (int): The action to be authorized (e.g., ``SQLITE_READ``)
            - ``arg1`` (str): First argument, meaning depends on ``action_code``
            - ``arg2`` (str): Second argument, meaning depends on ``action_code``
            - ``db_name`` (str): Database name (e.g., ``"main"``, ``"temp"``)
            - ``trigger_name`` (str): Name of trigger or view that is doing the access,
              or ``None``

            The callback should return:

            - ``SQLITE_OK`` (0): Allow the operation
            - ``SQLITE_DENY`` (1): Deny the operation, raise ``sqlite3.DatabaseError``
            - ``SQLITE_IGNORE`` (2): Treat operation as no-op

            Pass ``None`` to remove the authorizer.
        """
        await self._execute(self._conn.set_authorizer, authorizer_callback)

    async def iterdump(self) -> AsyncIterator[str]:
        """Return an async iterator to dump the database in SQL text format.

        Example::

            async for line in db.iterdump():
                ...

        """
        dump_queue: Queue = Queue()

        def dumper():
            try:
                for line in self._conn.iterdump():
                    dump_queue.put_nowait(line)
                dump_queue.put_nowait(None)

            except Exception:
                LOG.exception("exception while dumping db")
                dump_queue.put_nowait(None)
                raise

        fut = self._execute(dumper)
        task = asyncio.ensure_future(fut)

        while True:
            try:
                line: str | None = dump_queue.get_nowait()
                if line is None:
                    break
                yield line

            except Empty:
                if task.done():
                    LOG.warning("iterdump completed unexpectedly")
                    break

                await asyncio.sleep(0.01)

        await task

    async def backup(
        self,
        target: Connection | sqlite3.Connection,
        *,
        pages: int = 0,
        progress: Callable[[int, int, int], None] | None = None,
        name: str = "main",
        sleep: float = 0.250,
    ) -> None:
        """Make a backup of the current database to the target database.

        Takes either a standard sqlite3 or aiosqlite Connection object as the target.
        """
        if isinstance(target, Connection):
            target = target._conn

        await self._execute(
            self._conn.backup,
            target,
            pages=pages,
            progress=progress,
            name=name,
            sleep=sleep,
        )


def connect(
    database: os.PathLike[str] | str,
    *,
    iter_chunk_size: int = 64,
    loop: asyncio.AbstractEventLoop | None = None,
    **kwargs: Unpack[ConnectionParams],
) -> Connection:
    """Create and return a connection proxy to the sqlite database."""
    if loop is not None:
        warn(
            "aiosqlite.connect() no longer uses the `loop` parameter",
            DeprecationWarning,
            stacklevel=2,
        )

    def connector() -> sqlite3.Connection:
        try:
            loc = os.fspath(database)
        except TypeError as e:
            warn(
                f"{e}. Casting as as string",
                UserWarning,
                stacklevel=2,
            )
            loc = str(database)
        else:
            if isinstance(loc, bytes):
                loc = loc.decode("utf-8")

        return sqlite3.connect(loc, **kwargs)

    return Connection(connector, iter_chunk_size)
