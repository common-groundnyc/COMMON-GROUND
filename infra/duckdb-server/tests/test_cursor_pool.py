import threading
import time
import duckdb
import pytest
from cursor_pool import CursorPool


@pytest.fixture
def pool():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE test_t (id INTEGER, val VARCHAR)")
    conn.execute("INSERT INTO test_t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    p = CursorPool(conn, size=3)
    yield p
    conn.close()


def test_cursor_context_manager(pool):
    with pool.cursor() as cur:
        rows = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
        assert rows[0] == 3


def test_execute_convenience(pool):
    cols, rows = pool.execute("SELECT * FROM test_t ORDER BY id")
    assert cols == ["id", "val"]
    assert len(rows) == 3
    assert rows[0] == (1, "a")


def test_execute_with_params(pool):
    cols, rows = pool.execute("SELECT * FROM test_t WHERE id = ?", [2])
    assert len(rows) == 1
    assert rows[0] == (2, "b")


def test_concurrent_reads(pool):
    results = []
    errors = []
    def reader():
        try:
            with pool.cursor() as cur:
                time.sleep(0.05)
                r = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
                results.append(r[0])
        except Exception as e:
            errors.append(str(e))
    threads = [threading.Thread(target=reader) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"Errors: {errors}"
    assert all(r == 3 for r in results)


def test_pool_exhaustion_queues(pool):
    barrier = threading.Barrier(3)
    acquired = []
    errors = []
    def holder():
        try:
            with pool.cursor() as cur:
                acquired.append(threading.current_thread().name)
                barrier.wait(timeout=2)
                time.sleep(0.1)
        except Exception as e:
            errors.append(str(e))
    def waiter():
        time.sleep(0.05)
        t0 = time.monotonic()
        try:
            with pool.cursor() as cur:
                elapsed = time.monotonic() - t0
                assert elapsed > 0.05
                acquired.append("waiter")
        except Exception as e:
            errors.append(str(e))
    threads = [threading.Thread(target=holder, name=f"holder-{i}") for i in range(3)]
    threads.append(threading.Thread(target=waiter, name="waiter"))
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    assert not errors, f"Errors: {errors}"
    assert "waiter" in acquired


def test_cursor_reusable_after_error(pool):
    with pool.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM nonexistent_table")
    with pool.cursor() as cur:
        rows = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
        assert rows[0] == 3


def test_cursor_timeout():
    """Pool should raise TimeoutError when all cursors are busy."""
    conn = duckdb.connect()
    p = CursorPool(conn, size=1, timeout=0.1)
    with p.cursor() as _:
        with pytest.raises(TimeoutError, match="cursor"):
            with p.cursor() as _:
                pass
    conn.close()


def test_cursor_replaced_after_fatal_error(pool):
    """A cursor that fails should be replaced, not returned broken."""
    with pool.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM nonexistent_table_xyz")
    # Next cursor should work fine
    with pool.cursor() as cur:
        rows = cur.execute("SELECT COUNT(*) FROM test_t").fetchone()
        assert rows[0] == 3


def test_pool_close():
    conn = duckdb.connect()
    p = CursorPool(conn, size=2)
    p.close()
    with pytest.raises(RuntimeError, match="closed"):
        with p.cursor() as _:
            pass
    conn.close()
