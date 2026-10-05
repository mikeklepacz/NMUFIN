"""Atomic bank-import writes; PostgreSQL uses its native driver, not DuckDB ATTACH."""
from __future__ import annotations

from contextlib import contextmanager
from threading import RLock

import psycopg

from ..config import get_database_url
from ..db import connect

_LOCAL_IMPORT_LOCK = RLock()
_RECEIPTS_SQL = """
CREATE TABLE IF NOT EXISTS bank_import_commits (
    preview_id VARCHAR PRIMARY KEY,
    batch_ids VARCHAR NOT NULL,
    committed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


_DEDUPE_SQL = """
CREATE TABLE IF NOT EXISTS bank_import_dedupe_keys (
    dedupe_hash VARCHAR PRIMARY KEY
)
"""


class PostgresImportConnection:
    """Expose the existing services' qmark interface for fixed application SQL.

    ClientCursor safely binds literals, including the nullable category predicate
    in payable reconciliation, which PostgreSQL cannot type as a bare parameter.
    This adapter is intentionally limited to import/reconciliation statements.
    """

    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, parameters=None):
        cursor = self.connection.cursor()
        cursor.execute(sql.replace("?", "%s"), parameters)
        return cursor


@contextmanager
def import_transaction():
    database_url = get_database_url()
    if database_url:
        with psycopg.connect(
            database_url, connect_timeout=15, cursor_factory=psycopg.ClientCursor,
            options="-c statement_timeout=120000 -c lock_timeout=60000",
        ) as native:
            conn = PostgresImportConnection(native)
            # Serialize import schema creation and commits across app instances.
            conn.execute("SELECT pg_advisory_xact_lock(684739102)")
            # Also exclude other writers while checking hashes and matching payables.
            conn.execute("LOCK TABLE transactions, payables IN SHARE ROW EXCLUSIVE MODE")
            conn.execute(_RECEIPTS_SQL)
            conn.execute(_DEDUPE_SQL)
            yield conn
        return

    # DuckDB rejects conflicting writers across processes; serialize local threads
    # so ordinary concurrent import jobs can succeed without a write conflict.
    with _LOCAL_IMPORT_LOCK, connect() as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(_RECEIPTS_SQL)
            conn.execute(_DEDUPE_SQL)
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
