"""
Client of branch A's Spark Thrift server (decision D6 revised), the same PyHive
connection dbt-spark opens: the code server of A runs no JVM of its own. Lakekeeper
is the server's default catalog, so tables are `<namespace>.<table>`.
"""

from pyhive import hive

from src import config


def query(sql: str) -> list[tuple]:
    """Rows of one SQL statement run on the Thrift server."""
    conn = hive.connect(config.SPARK_THRIFT_HOST, config.SPARK_THRIFT_PORT)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        conn.close()
