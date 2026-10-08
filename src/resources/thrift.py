"""
Client of the Spark branch's Thrift server (decision D6 revised), the same PyHive
connection dbt-spark opens: the Spark branch's code server runs no JVM of its own. Lakekeeper
is the server's default catalog, so tables are `<namespace>.<table>`.
"""

from typing import Any

from pyhive import hive

from src import config


def _execute(sql: str) -> tuple[list[str], list[tuple]]:
    """Column names and rows of one SQL statement run on the Thrift server.
    One statement per call: a PyHive message carries a single one."""
    conn = hive.connect(config.SPARK_THRIFT_HOST, config.SPARK_THRIFT_PORT)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        if cursor.description is None:  # DDL and DML return no result set
            return [], []
        # Spark names result columns `col` or `table.col`: keep the column name only
        names = [column[0].rsplit(".", 1)[-1] for column in cursor.description]
        return names, [tuple(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def query(sql: str) -> list[tuple]:
    """Rows of one SQL statement run on the Thrift server, as tuples."""
    return _execute(sql)[1]


def records(sql: str) -> list[dict[str, Any]]:
    """Rows of one SQL statement as {column: value} (procedure results, named columns)."""
    names, rows = _execute(sql)
    return [dict(zip(names, row, strict=True)) for row in rows]
