"""
DuckDB connection singleton.

The database file lives at GraphO2C/backend/grapho2c.duckdb.
Call get_connection() from any module to get the shared connection.
"""

from pathlib import Path
import duckdb

_DB_PATH = Path(__file__).parent / "grapho2c.duckdb"
_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB connection, creating it if needed."""
    global _conn
    if _conn is None:
        _conn = duckdb.connect(str(_DB_PATH))
    return _conn


def close_connection() -> None:
    global _conn
    if _conn is not None:
        _conn.close()
        _conn = None
