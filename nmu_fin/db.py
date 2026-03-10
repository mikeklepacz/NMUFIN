from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import time
from typing import Iterator

import duckdb

from .account_labels import build_account_label
from .config import DATA_DIR, DEFAULT_CATEGORIES, get_database_path, get_database_url


SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS vendors_seq START 1;
CREATE SEQUENCE IF NOT EXISTS categories_seq START 1;
CREATE SEQUENCE IF NOT EXISTS vendor_rules_seq START 1;
CREATE SEQUENCE IF NOT EXISTS category_rules_seq START 1;
CREATE SEQUENCE IF NOT EXISTS raw_import_rows_seq START 1;
CREATE SEQUENCE IF NOT EXISTS transactions_seq START 1;
CREATE SEQUENCE IF NOT EXISTS payables_seq START 1;

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR PRIMARY KEY,
    bank_name VARCHAR,
    account_name VARCHAR NOT NULL,
    account_number VARCHAR,
    currency VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS vendors (
    vendor_id BIGINT PRIMARY KEY DEFAULT nextval('vendors_seq'),
    vendor_name VARCHAR NOT NULL,
    canonical_vendor VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS categories (
    category_id BIGINT PRIMARY KEY DEFAULT nextval('categories_seq'),
    category_name VARCHAR NOT NULL UNIQUE,
    parent_category VARCHAR
);

CREATE TABLE IF NOT EXISTS vendor_rules (
    rule_id BIGINT PRIMARY KEY DEFAULT nextval('vendor_rules_seq'),
    match_pattern VARCHAR NOT NULL UNIQUE,
    canonical_vendor VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS category_rules (
    rule_id BIGINT PRIMARY KEY DEFAULT nextval('category_rules_seq'),
    match_pattern VARCHAR NOT NULL,
    match_type VARCHAR NOT NULL DEFAULT 'description',
    assigned_category VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fx_rates (
    rate_date DATE NOT NULL,
    from_currency VARCHAR NOT NULL,
    to_currency VARCHAR NOT NULL,
    rate DOUBLE NOT NULL,
    PRIMARY KEY (rate_date, from_currency, to_currency)
);

CREATE TABLE IF NOT EXISTS import_batches (
    import_batch_id VARCHAR PRIMARY KEY,
    source_filename VARCHAR NOT NULL,
    parser_name VARCHAR NOT NULL,
    account_id VARCHAR,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_count INTEGER NOT NULL,
    duplicate_count INTEGER NOT NULL,
    status VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_import_rows (
    raw_row_id BIGINT PRIMARY KEY DEFAULT nextval('raw_import_rows_seq'),
    import_batch_id VARCHAR NOT NULL,
    row_number INTEGER NOT NULL,
    raw_payload JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS translation_cache (
    source_text VARCHAR PRIMARY KEY,
    translated_text VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
    id BIGINT PRIMARY KEY DEFAULT nextval('transactions_seq'),
    transaction_id VARCHAR,
    transaction_date DATE NOT NULL,
    posting_date DATE NOT NULL,
    account_id VARCHAR NOT NULL,
    bank_name VARCHAR,
    account_name VARCHAR NOT NULL,
    description_raw VARCHAR NOT NULL,
    description_en VARCHAR,
    description_clean VARCHAR NOT NULL,
    vendor_raw VARCHAR,
    vendor_canonical VARCHAR,
    amount_original DOUBLE NOT NULL,
    currency_original VARCHAR NOT NULL,
    balance DOUBLE,
    amount_usd DOUBLE,
    amount_pln DOUBLE,
    amount_eur DOUBLE,
    operation_type_raw VARCHAR,
    operation_type_en VARCHAR,
    description_translation_pending BOOLEAN DEFAULT FALSE,
    category_id BIGINT,
    direction VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year_month VARCHAR NOT NULL,
    import_batch_id VARCHAR NOT NULL,
    dedupe_hash VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    transaction_type VARCHAR NOT NULL DEFAULT 'standard',
    needs_review BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payables (
    payable_id BIGINT PRIMARY KEY DEFAULT nextval('payables_seq'),
    vendor_canonical VARCHAR NOT NULL,
    category_id BIGINT,
    currency_original VARCHAR NOT NULL,
    amount_original DOUBLE NOT NULL,
    due_date DATE NOT NULL,
    note VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'open',
    linked_transaction_id BIGINT,
    paid_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transactions_dates ON transactions(year_month, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_vendor ON transactions(vendor_canonical);
CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id);
CREATE INDEX IF NOT EXISTS idx_transactions_dedupe ON transactions(dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_payables_status_due ON payables(status, due_date);
CREATE INDEX IF NOT EXISTS idx_payables_vendor_currency ON payables(vendor_canonical, currency_original);
"""


def init_db(db_path: Path | None = None) -> None:
    resolved_path = db_path or get_database_path()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with connect(resolved_path) as conn:
        conn.execute(SCHEMA_SQL)
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('transactions')").fetchall()
        }
        account_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info('accounts')").fetchall()
        }
        if "bank_name" not in account_columns:
            conn.execute("ALTER TABLE accounts ADD COLUMN bank_name VARCHAR")
        if "bank_name" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN bank_name VARCHAR")
        if "description_en" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN description_en VARCHAR")
        if "balance" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN balance DOUBLE")
        if "operation_type_raw" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN operation_type_raw VARCHAR")
        if "operation_type_en" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN operation_type_en VARCHAR")
        if "description_translation_pending" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN description_translation_pending BOOLEAN")
        if "updated_at" not in columns:
            conn.execute("ALTER TABLE transactions ADD COLUMN updated_at TIMESTAMP")
        conn.execute("UPDATE accounts SET bank_name = 'Santander' WHERE bank_name IS NULL")
        conn.execute("UPDATE transactions SET bank_name = 'Santander' WHERE bank_name IS NULL")
        conn.execute("UPDATE transactions SET description_translation_pending = FALSE WHERE description_translation_pending IS NULL")
        conn.execute("UPDATE transactions SET updated_at = CURRENT_TIMESTAMP WHERE updated_at IS NULL")
        normalize_account_labels(conn)
        for category_name, parent_category in DEFAULT_CATEGORIES:
            conn.execute(
                """
                INSERT INTO categories(category_name, parent_category)
                SELECT ?, ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM categories WHERE category_name = ?
                )
                """,
                [category_name, parent_category, category_name],
            )


def normalize_account_labels(conn: duckdb.DuckDBPyConnection) -> None:
    account_rows = conn.execute(
        "SELECT account_id, bank_name, account_number, currency FROM accounts"
    ).fetchall()
    account_numbers = {row[0]: row[2] for row in account_rows}
    account_updates = [
        (
            build_account_label(row[1], row[3], row[2]),
            row[0],
        )
        for row in account_rows
    ]
    if account_updates:
        conn.executemany("UPDATE accounts SET account_name = ? WHERE account_id = ?", account_updates)

    transaction_rows = conn.execute(
        "SELECT id, bank_name, currency_original, account_id FROM transactions"
    ).fetchall()
    transaction_updates = [
        (
            build_account_label(row[1], row[2], account_numbers.get(row[3])),
            row[0],
        )
        for row in transaction_rows
    ]
    if transaction_updates:
        conn.executemany("UPDATE transactions SET account_name = ? WHERE id = ?", transaction_updates)


@contextmanager
def connect(db_path: Path | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    database_url = get_database_url() if db_path is None else None
    if database_url:
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("INSTALL postgres")
            conn.execute("LOAD postgres")
            conn.execute(f"ATTACH '{database_url}' AS nmu_fin_pg (TYPE POSTGRES)")
            conn.execute("USE nmu_fin_pg")
            yield conn
        finally:
            conn.close()
        return

    attempts = 0
    last_error: Exception | None = None
    while attempts < 10:
        try:
            conn = duckdb.connect(str(db_path or get_database_path()))
            break
        except duckdb.IOException as exc:
            last_error = exc
            if "Could not set lock on file" not in str(exc):
                raise
            attempts += 1
            time.sleep(0.05)
    else:
        assert last_error is not None
        raise last_error
    try:
        yield conn
    finally:
        conn.close()
