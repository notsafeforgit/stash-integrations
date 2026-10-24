"""Structured command diagnostics without response bodies, URLs or credentials."""

import errno
from http.client import HTTPException
import sqlite3
from urllib.error import HTTPError, URLError

from .client import Unavailable
from .encoding import InvalidData
from .outbox import Capacity


def describe(error):
    result = {"type": type(error).__name__, "code": "internal_error", "retryable": False}
    if isinstance(error, (HTTPError, Unavailable)):
        status = error.code if isinstance(error, HTTPError) else error.status
        result.update(code="api_unavailable", http_status=status,
                      retryable=status is None or status in (408, 429) or status >= 500)
    elif isinstance(error, (URLError, HTTPException, TimeoutError)):
        result.update(code="transport_unavailable", retryable=True)
    elif isinstance(error, sqlite3.Error):
        code = getattr(error, "sqlite_errorcode", 0)
        # Corrupt/foreign databases and programming errors require review. A
        # busy, unavailable or full filesystem can recover without losing work.
        transient = {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_IOERR,
                     sqlite3.SQLITE_FULL, sqlite3.SQLITE_CANTOPEN, sqlite3.SQLITE_READONLY,
                     sqlite3.SQLITE_PROTOCOL, sqlite3.SQLITE_INTERRUPT}
        result.update(code="database_error", sqlite_code=code,
                      sqlite_name=getattr(error, "sqlite_errorname", "UNKNOWN"),
                      retryable=(code & 255) in transient)
    elif isinstance(error, OSError):
        result.update(code="storage_unavailable", errno=error.errno,
                      os_error=errno.errorcode.get(error.errno, "UNKNOWN"),
                      retryable=error.errno not in (errno.ELOOP, errno.EINVAL))
    elif isinstance(error, Capacity):
        result.update(code="queue_capacity", retryable=True)
    elif isinstance(error, (InvalidData, ValueError, KeyError)):
        result.update(code="validation_failed")
    return result
