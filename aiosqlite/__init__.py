# Copyright Amethyst Reese
# Licensed under the MIT license

"""asyncio bridge to the standard sqlite3 module"""

import importlib.metadata
from sqlite3 import (
    DatabaseError,
    Error,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Row,
    Warning,
    paramstyle,
    register_adapter,
    register_converter,
    sqlite_version,
    sqlite_version_info,
)

__author__ = "Amethyst Reese"
__version__ = importlib.metadata.version("aiosqlite")

from .core import Connection, Cursor, connect

__all__ = [
    "Connection",
    "Cursor",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Row",
    "Warning",
    "__version__",
    "connect",
    "paramstyle",
    "register_adapter",
    "register_converter",
    "sqlite_version",
    "sqlite_version_info",
]
