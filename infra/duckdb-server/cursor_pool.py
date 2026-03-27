"""Thread-safe DuckDB cursor pool for concurrent MCP query execution."""

import queue
import threading
from contextlib import contextmanager


class CursorPool:
    """Pool of DuckDB cursors for concurrent read access.

    DuckDB supports concurrent reads via .cursor() — each cursor is a
    lightweight sub-connection that can run one query at a time. Multiple
    cursors from one connection can run queries in parallel via MVCC.
    """

    def __init__(self, conn, size=8):
        self._conn = conn
        self._size = size
        self._semaphore = threading.Semaphore(size)
        self._available = queue.Queue()
        for _ in range(size):
            self._available.put(conn.cursor())

    @contextmanager
    def cursor(self):
        """Acquire a cursor from the pool, release on exit."""
        self._semaphore.acquire()
        cur = self._available.get()
        try:
            yield cur
        finally:
            self._available.put(cur)
            self._semaphore.release()

    def execute(self, sql, params=None, max_rows=1000):
        """Acquire cursor, execute query, return (cols, rows). Raises raw duckdb.Error."""
        with self.cursor() as cur:
            result = cur.execute(sql, params or [])
            cols = [d[0] for d in result.description] if result.description else []
            rows = result.fetchmany(max_rows)
            return cols, rows
