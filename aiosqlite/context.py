# Copyright Amethyst Reese
# Licensed under the MIT license

from __future__ import annotations

import functools
from collections.abc import Callable, Coroutine, Generator
from contextlib import AbstractAsyncContextManager
from typing import Any, ParamSpec, TypeVar

from .cursor import Cursor

_T = TypeVar("_T")
_P = ParamSpec("_P")


class Result(AbstractAsyncContextManager[_T], Coroutine[Any, Any, _T]):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro: Coroutine[Any, Any, _T]) -> None:
        self._coro: Coroutine[Any, Any, _T] = coro
        self._obj: _T

    def send(self, value: Any) -> None:
        return self._coro.send(value)

    def throw(self, *args, **kwargs) -> None:
        return self._coro.throw(*args, **kwargs)

    def close(self) -> None:
        return self._coro.close()

    def __await__(self) -> Generator[Any, None, _T]:
        return self._coro.__await__()

    async def __aenter__(self) -> _T:
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if isinstance(self._obj, Cursor):
            await self._obj.aclose()


def contextmanager(
    method: Callable[_P, Coroutine[Any, Any, _T]],
) -> Callable[_P, Result[_T]]:
    @functools.wraps(method)
    def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> Result[_T]:
        return Result(method(*args, **kwargs))

    return wrapper
