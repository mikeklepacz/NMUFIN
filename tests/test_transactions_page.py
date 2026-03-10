from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from nmu_fin.db import connect, init_db
from nmu_fin.services.imports import build_preview, commit_preview
from nmu_fin.services.transactions import list_transactions
from nmu_fin.web import app

SAMPLES = Path(__file__).resolve().parent.parent / "Banks Import" / "Santander Bank"
TEST_DB_DIR = TemporaryDirectory()
os.environ["NMU_FIN_DB_PATH"] = str(Path(TEST_DB_DIR.name) / "test-transactions.duckdb")
os.environ["NMU_FIN_DISABLE_AI_TRANSLATION"] = "1"


def reset_db() -> None:
    db_path = Path(os.environ["NMU_FIN_DB_PATH"])
    if db_path.exists():
        db_path.unlink()
    init_db()


def test_review_delete_transaction_removes_row() -> None:
    reset_db()
    path = next(SAMPLES.glob("*USD*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)
    with connect() as conn:
        transaction_id = conn.execute("SELECT id FROM transactions ORDER BY id LIMIT 1").fetchone()[0]
    client = TestClient(app)
    response = client.post(
        f"/review/{transaction_id}/delete",
        data={"currency_filter": "USD", "direction_filter": "expense"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    with connect() as conn:
        deleted = conn.execute("SELECT 1 FROM transactions WHERE id = ?", [transaction_id]).fetchone()
    assert deleted is None


def test_transactions_can_be_sorted_and_edited() -> None:
    reset_db()
    path = next(SAMPLES.glob("*USD*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)
    rows = list_transactions(sort_by="date", sort_dir="asc")
    assert rows
    assert rows[0]["transaction_date"] <= rows[-1]["transaction_date"]

    transaction_id = rows[0]["id"]
    client = TestClient(app)
    response = client.post(
        f"/transactions/{transaction_id}",
        data={
            "vendor_canonical": "EDITED VENDOR",
            "category_name": "Sales",
            "sort_by": "last_modified",
            "sort_dir": "desc",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    with connect() as conn:
        updated = conn.execute(
            "SELECT vendor_canonical FROM transactions WHERE id = ?",
            [transaction_id],
        ).fetchone()
    assert updated[0] == "EDITED VENDOR"
