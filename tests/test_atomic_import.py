from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
import os
import uuid

import psycopg
from psycopg.conninfo import make_conninfo
import pytest

from nmu_fin.db import SCHEMA_SQL, connect, init_db
from nmu_fin.services import imports
from nmu_fin.services.import_storage import import_transaction
from nmu_fin.parsers import ALIOR_HEADERS, parse_known_csv


def csv_source(name="test.csv"):
    header = ';'.join(ALIOR_HEADERS[1])
    rows = [
        '20261004;20261004;TEST SUPPLIER;TEST OWNER;Invoice 1;;;;Transfer;-10,00;PLN;90,00',
        '20261005;20261005;TEST SUPPLIER;TEST OWNER;Invoice 2;;;;Transfer;-20,00;PLN;70,00',
    ]
    return name, ('\n'.join([header, *rows]) + '\n').encode()


@pytest.fixture(params=['duckdb', 'postgres'])
def backend(request, monkeypatch, tmp_path):
    monkeypatch.delenv('DATABASE_URL', raising=False)
    monkeypatch.delenv('NMU_FIN_DATABASE_URL', raising=False)
    monkeypatch.setenv('NMU_FIN_DISABLE_AI_TRANSLATION', '1')
    monkeypatch.setenv('NMU_FIN_DB_PATH', str(tmp_path / 'test.duckdb'))
    if request.param == 'duckdb':
        init_db()
        yield request.param
        return
    admin_url = os.environ.get('NMU_FIN_TEST_POSTGRES_URL')
    if not admin_url:
        pytest.skip('Set NMU_FIN_TEST_POSTGRES_URL to a disposable local PostgreSQL server')
    database = 'nmufin_import_test_' + uuid.uuid4().hex
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(psycopg.sql.SQL('CREATE DATABASE {}').format(psycopg.sql.Identifier(database)))
        try:
            test_url = make_conninfo(admin_url, dbname=database)
            monkeypatch.setenv('NMU_FIN_DATABASE_URL', test_url)
            with psycopg.connect(test_url) as native:
                native.execute(SCHEMA_SQL.replace('DOUBLE', 'DOUBLE PRECISION'))
            yield request.param
        finally:
            admin.execute(psycopg.sql.SQL('DROP DATABASE {} WITH (FORCE)').format(psycopg.sql.Identifier(database)))


def counts():
    with connect() as conn:
        return {table: conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                for table in ['accounts', 'vendors', 'import_batches', 'raw_import_rows', 'transactions']}


def test_retry_and_duplicate_audit(backend):
    preview = imports.build_preview([csv_source()])
    original = deepcopy(preview.rows)
    batch = imports.commit_preview(preview)
    assert imports.commit_preview(preview) == batch
    assert counts()['transactions'] == 2
    # Separate uploads retain audit rows while sums count only original entries.
    duplicate = imports.build_preview([csv_source()])
    imports.commit_preview(duplicate)
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*), SUM(amount_original) FROM transactions WHERE status <> 'duplicate'").fetchone() == (2, -30.0)
        assert conn.execute("SELECT duplicate_count FROM import_batches ORDER BY duplicate_count").fetchall() == [(0,), (2,)]
        assert conn.execute('SELECT COUNT(*) FROM raw_import_rows').fetchone()[0] == 4
        assert conn.execute('SELECT COUNT(*) FROM bank_import_commits').fetchone()[0] == 2
    assert preview.rows == original


def test_concurrent_stale_previews(backend):
    first = imports.build_preview([csv_source()])
    second = imports.build_preview([csv_source()])
    assert first.duplicate_count == second.duplicate_count == 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        batches = list(pool.map(imports.commit_preview, [first, second]))
    assert len(set(batches)) == 2
    with connect() as conn:
        assert conn.execute("SELECT status, COUNT(*) FROM transactions GROUP BY status ORDER BY status").fetchall() == [('duplicate', 2), ('needs_review', 2)]
        assert conn.execute('SELECT duplicate_count FROM import_batches ORDER BY duplicate_count').fetchall() == [(0,), (2,)]


def test_same_preview_concurrent_retry(backend):
    preview = imports.build_preview([csv_source()])
    with ThreadPoolExecutor(max_workers=2) as pool:
        batches = list(pool.map(imports.commit_preview, [preview, preview]))
    assert batches[0] == batches[1]
    assert counts()['transactions'] == 2
    assert counts()['raw_import_rows'] == 2


def test_multi_file_failure_rolls_back_every_write_and_retries(backend):
    preview = imports.build_preview([csv_source('one.csv'), csv_source('two.csv')])
    baseline = counts()
    original = deepcopy(preview.rows)
    def fail_after_rows(stage, total, processed):
        if processed == total:
            raise RuntimeError('injected mid-import failure')
    with pytest.raises(RuntimeError, match='injected'):
        imports.commit_preview(preview, fail_after_rows)
    assert counts() == baseline
    assert preview.rows == original
    imports.commit_preview(preview)
    assert counts()['transactions'] == 4
    assert counts()['raw_import_rows'] == 4


def test_payable_reconciliation_is_atomic_and_duplicate_safe(backend, monkeypatch):
    preview = imports.build_preview([csv_source()])
    vendor = preview.rows[0]['vendor_canonical']
    with connect() as conn:
        conn.execute("INSERT INTO payables(vendor_canonical, currency_original, amount_original, due_date) VALUES (?, 'PLN', 10, '2026-10-04')", [vendor])
    original = imports._reconcile_open_payables_for_transactions
    def fail_after_reconciliation(conn, ids):
        assert original(conn, ids) == 1
        raise RuntimeError('failure after payable update')
    monkeypatch.setattr(imports, '_reconcile_open_payables_for_transactions', fail_after_reconciliation)
    with pytest.raises(RuntimeError, match='payable update'):
        imports.commit_preview(preview)
    assert counts()['transactions'] == 0
    with connect() as conn:
        assert conn.execute('SELECT status, linked_transaction_id FROM payables').fetchone() == ('open', None)
    monkeypatch.setattr(imports, '_reconcile_open_payables_for_transactions', original)
    imports.commit_preview(preview)
    with connect() as conn:
        linked = conn.execute('SELECT linked_transaction_id FROM payables').fetchone()[0]
        assert conn.execute('SELECT amount_original FROM transactions WHERE id = ?', [linked]).fetchone() == (-10.0,)
        # A later duplicate must not consume a second payable.
        conn.execute("INSERT INTO payables(vendor_canonical, currency_original, amount_original, due_date) VALUES (?, 'PLN', 10, '2026-10-04')", [vendor])
    imports.commit_preview(imports.build_preview([csv_source()]))
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM payables WHERE status = 'open'").fetchone()[0] == 1


def test_database_enforces_import_claim_uniqueness(backend):
    if backend != 'postgres':
        pytest.skip('PostgreSQL uniqueness violation type')
    preview = imports.build_preview([csv_source()])
    imports.commit_preview(preview)
    imports.commit_preview(imports.build_preview([csv_source()]))
    with pytest.raises(psycopg.errors.UniqueViolation):
        with import_transaction() as conn:
            conn.execute("INSERT INTO bank_import_dedupe_keys SELECT dedupe_hash FROM transactions LIMIT 1")


def test_polish_and_english_alior_headers_preserve_values():
    name, payload = csv_source()
    parsed = parse_known_csv(name, payload)
    assert parsed.rows[0].amount_original == -10
    assert parsed.rows[0].balance == 90
    assert parsed.rows[0].raw_payload['header'] == ALIOR_HEADERS[1]
    english = payload.decode().replace(';'.join(ALIOR_HEADERS[1]), ';'.join(ALIOR_HEADERS[0]))
    assert parse_known_csv(name, english.encode()).rows[0].amount_original == -10


@pytest.mark.parametrize('replacement', ['Invoice;extra', '"Invoice;extra";extra'])
def test_alior_ambiguous_columns_fail_visibly(replacement):
    name, payload = csv_source()
    malformed = payload.decode().replace('Invoice 1', replacement)
    with pytest.raises(ValueError, match=r'test.csv: CSV row 2: expected 12 fields, found 13'):
        parse_known_csv(name, malformed.encode())


@pytest.mark.parametrize('value', ['', 'NaN', 'Infinity', 'wrong'])
def test_alior_invalid_amount_cannot_become_zero(value):
    name, payload = csv_source()
    with pytest.raises(ValueError, match='test.csv: CSV row 2:'):
        parse_known_csv(name, payload.replace(b'-10,00', value.encode()))


def test_preview_validation_error_is_visible_and_writes_nothing(backend, monkeypatch):
    from fastapi.testclient import TestClient
    from nmu_fin.config import sign_auth_token
    from nmu_fin.web import app, AUTH_COOKIE_NAME, AUTH_COOKIE_PAYLOAD
    monkeypatch.setenv('NMU_FIN_APP_SECRET', 'isolated-test-only-signing-key')
    client = TestClient(app)
    client.cookies.set(AUTH_COOKIE_NAME, sign_auth_token(AUTH_COOKIE_PAYLOAD))
    name, payload = csv_source()
    response = client.post('/imports/preview', files=[('csv_file', (name, payload.replace(b'Invoice 1', b'Invoice;extra'), 'text/csv'))])
    assert response.status_code == 400
    assert 'CSV row 2: expected 12 fields, found 13' in response.text
    assert counts()['transactions'] == 0
    assert counts()['import_batches'] == 0


def test_deleted_source_can_be_reimported(backend):
    preview = imports.build_preview([csv_source()])
    imports.commit_preview(preview)
    with connect() as conn:
        conn.execute('DELETE FROM transactions')
    replacement = imports.build_preview([csv_source()])
    assert replacement.duplicate_count == 0
    imports.commit_preview(replacement)
    with connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM transactions WHERE status <> 'duplicate'").fetchone()[0] == 2


def test_existing_sample_formats_and_categories_are_preserved(backend):
    samples = Path(__file__).resolve().parent.parent / 'Banks Import'
    # Existing English Alior and Santander layouts continue to parse.
    for sample in [samples / 'Alior Bank' / 'Alior Transactions.csv', *sorted((samples / 'Santander Bank').glob('*.csv'))]:
        parsed = parse_known_csv(sample.name, sample.read_bytes())
        assert parsed.rows
    with connect() as conn:
        conn.execute("INSERT INTO categories(category_name, parent_category) VALUES ('Test Materials', 'Expenses')")
        category = conn.execute("SELECT category_id FROM categories WHERE category_name = 'Test Materials'").fetchone()[0]
    preview = imports.build_preview([csv_source()])
    for row in preview.file_previews[0].rows:
        row['category_id'] = category
        row['amount_usd'] = 1.0
        row['amount_pln'] = row['amount_original']
        row['amount_eur'] = 1.0
    imports.commit_preview(preview)
    imports.commit_preview(imports.build_preview([csv_source()]))
    with connect() as conn:
        assert conn.execute("SELECT DISTINCT category_id FROM transactions WHERE status <> 'duplicate'").fetchall() == [(category,)]
