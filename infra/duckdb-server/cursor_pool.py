"""Thread-safe DuckDB cursor pool for concurrent MCP query execution."""

import queue
import threading
from contextlib import contextmanager

_DEFAULT_TIMEOUT = 30  # seconds


class CursorPool:
    """Pool of DuckDB cursors for concurrent read access."""

    def __init__(self, conn, size=8, timeout=_DEFAULT_TIMEOUT):
        self._conn = conn
        self._size = size
        self._timeout = timeout
        self._semaphore = threading.Semaphore(size)
        self._available = queue.Queue()
        self._closed = False
        for _ in range(size):
            self._available.put(conn.cursor())

    @contextmanager
    def cursor(self):
        """Acquire a cursor from the pool, release on exit."""
        if self._closed:
            raise RuntimeError("CursorPool is closed")
        if not self._semaphore.acquire(timeout=self._timeout):
            raise TimeoutError(
                f"All {self._size} database cursors busy for {self._timeout}s — try again shortly"
            )
        cur = self._available.get()
        try:
            yield cur
        except Exception:
            # Replace potentially broken cursor
            try:
                cur.close()
            except Exception:
                pass
            cur = self._conn.cursor()
            raise
        finally:
            self._available.put(cur)
            self._semaphore.release()

    def execute(self, sql, params=None, max_rows=1000):
        """Acquire cursor, execute query, return (cols, rows)."""
        with self.cursor() as cur:
            result = cur.execute(sql, params or [])
            cols = [d[0] for d in result.description] if result.description else []
            rows = result.fetchmany(max_rows)
            return cols, rows

    def close(self):
        """Drain all cursors and mark pool as closed."""
        self._closed = True
        while not self._available.empty():
            try:
                cur = self._available.get_nowait()
                cur.close()
            except (queue.Empty, Exception):
                break
