from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from nmu_fin.db import connect, init_db
from nmu_fin.services.fx import upsert_manual_rates
from nmu_fin.services.imports import build_preview, commit_preview
from nmu_fin.services.reports import get_category_options, get_dashboard_data
from nmu_fin.services.rules import reapply_rules_to_transactions, save_category_rule, save_vendor_rule
from nmu_fin.services.settings import create_category, delete_category, update_category
from nmu_fin.web import app


SAMPLES = Path(__file__).resolve().parent.parent / "Banks Import" / "Santander Bank"
ALIOR_SAMPLE = Path(__file__).resolve().parent.parent / "Banks Import" / "Alior Bank" / "Alior Transactions.csv"
TEST_DB_DIR = TemporaryDirectory()
os.environ["NMU_FIN_DB_PATH"] = str(Path(TEST_DB_DIR.name) / "test.duckdb")
os.environ["NMU_FIN_DISABLE_AI_TRANSLATION"] = "1"


def reset_db() -> None:
    db_path = Path(os.environ["NMU_FIN_DB_PATH"])
    if db_path.exists():
        db_path.unlink()
    init_db()


def supported_sample_paths() -> list[Path]:
    paths: list[Path] = []
    for path in SAMPLES.glob("*.csv"):
        try:
            preview = build_preview(path.name, path.read_bytes())
        except ValueError:
            continue
        assert preview.parser_name == "ing_pl_business_v1"
        assert preview.row_count > 0
        paths.append(path)
    return paths


def test_known_csv_formats_parse_and_import() -> None:
    reset_db()
    upsert_manual_rates(__import__("datetime").date(2026, 3, 3), 3.95, 4.30)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 26), 3.56, 4.10)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 24), 4.00, 4.18)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 20), 4.01, 4.16)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 10), 4.02, 4.20)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 9), 4.01, 4.19)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 6), 4.03, 4.18)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 5), 4.05, 4.21)
    upsert_manual_rates(__import__("datetime").date(2026, 3, 8), 4.02, 4.36)
    upsert_manual_rates(__import__("datetime").date(2026, 3, 6), 4.02, 4.42)
    upsert_manual_rates(__import__("datetime").date(2026, 3, 5), 4.00, 4.33)
    upsert_manual_rates(__import__("datetime").date(2026, 3, 4), 3.99, 4.32)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 27), 3.97, 4.30)

    total_rows = 0
    for path in supported_sample_paths():
        preview = build_preview(path.name, path.read_bytes())
        total_rows += preview.row_count
        commit_preview(preview)

    with connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert count == total_rows


def test_alior_csv_preview_detects_bank_and_mixed_currencies() -> None:
    reset_db()
    preview = build_preview(ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes())
    assert preview.parser_name == "alior_business_v1"
    assert preview.parsed_file.bank_name == "Alior"
    assert preview.row_count > 0
    assert {row["currency_original"] for row in preview.rows} >= {"PLN", "USD"}


def test_multi_file_preview_and_commit() -> None:
    reset_db()
    files = [
        (ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes()),
        *[(path.name, path.read_bytes()) for path in sorted(SAMPLES.glob("*.csv"))[:2]],
    ]
    preview = build_preview(files)
    assert preview.file_count == 3
    assert preview.filename == "3 files"
    assert len(preview.file_previews) == 3

    commit_preview(preview)

    with connect() as conn:
        batch_count = conn.execute("SELECT COUNT(*) FROM import_batches").fetchone()[0]
        transaction_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert batch_count == 3
    assert transaction_count == preview.row_count


def test_web_preview_accepts_multiple_csv_files() -> None:
    reset_db()
    client = TestClient(app)
    response = client.post(
        "/imports/preview",
        files=[
            ("csv_file", (ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes(), "text/csv")),
            ("csv_file", ("Santander USD history_2026-03-09.csv", next(SAMPLES.glob("*USD*.csv")).read_bytes(), "text/csv")),
        ],
    )
    assert response.status_code == 200
    assert "Files:</strong> 2" in response.text
    assert "Alior Transactions.csv" in response.text
    assert "Santander USD history_2026-03-09.csv" in response.text


def test_alior_currency_conversion_rows_are_classified_as_exchange() -> None:
    reset_db()
    preview = build_preview(ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes())
    exchange_rows = [row for row in preview.rows if "PRZEWALUTOWAN" in row["description_clean"]]
    assert exchange_rows
    assert all(row["transaction_type"] == "exchange" for row in exchange_rows)
    assert all(row["vendor_canonical"] == "INTERNAL FX TRANSFER" for row in exchange_rows)
    assert all(row["category_id"] is not None for row in exchange_rows)


def test_alior_same_day_internal_currency_moves_are_promoted_to_exchange() -> None:
    reset_db()
    preview = build_preview(ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes())
    clustered_rows = [
        row
        for row in preview.rows
        if row["transaction_date"] == "2025-06-02"
        and row["description_clean"] == "PRZELEW WLASNY"
        and row["currency_original"] in {"PLN", "USD"}
    ]
    assert clustered_rows
    assert all(row["transaction_type"] == "exchange" for row in clustered_rows)
    assert all(row["vendor_canonical"] == "INTERNAL FX TRANSFER" for row in clustered_rows)
    assert all(row["category_id"] is not None for row in clustered_rows)


def test_duplicate_detection_across_reimport() -> None:
    reset_db()
    path = next(SAMPLES.glob("*USD*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)
    duplicate_preview = build_preview(path.name, path.read_bytes())
    assert duplicate_preview.duplicate_count == duplicate_preview.row_count


def test_dashboard_and_web_smoke() -> None:
    reset_db()
    path = next(SAMPLES.glob("*PLN*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)
    dashboard = get_dashboard_data()
    assert isinstance(dashboard.monthly_summary, list)

    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200


def test_duplicate_reimport_does_not_change_dashboard_sums() -> None:
    reset_db()
    upsert_manual_rates(__import__("datetime").date(2026, 3, 3), 3.95, 4.30)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 26), 3.56, 4.10)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 24), 4.00, 4.18)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 20), 4.01, 4.16)
    path = next(SAMPLES.glob("*USD*.csv"))

    first_preview = build_preview(path.name, path.read_bytes())
    commit_preview(first_preview)
    baseline = get_dashboard_data("PLN")

    duplicate_preview = build_preview(path.name, path.read_bytes())
    commit_preview(duplicate_preview)
    after_duplicate = get_dashboard_data("PLN")

    assert baseline.monthly_summary == after_duplicate.monthly_summary
    assert baseline.vendor_leaderboard == after_duplicate.vendor_leaderboard
    assert baseline.currency_summary == after_duplicate.currency_summary


def test_transactions_hide_duplicates_and_category_crud() -> None:
    reset_db()
    path = next(SAMPLES.glob("*USD*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)
    duplicate_preview = build_preview(path.name, path.read_bytes())
    commit_preview(duplicate_preview)

    client = TestClient(app)
    hidden = client.get("/transactions")
    shown = client.get("/transactions?include_duplicates=true")
    assert hidden.status_code == 200
    assert shown.status_code == 200
    assert hidden.text.count('id="transaction-') < shown.text.count('id="transaction-')

    create_category("Office", "Expenses")
    with connect() as conn:
        created = conn.execute(
            "SELECT category_id FROM categories WHERE category_name = 'Office'"
        ).fetchone()
    assert created is not None
    update_category(created[0], "Office Supplies", "Expenses")
    with connect() as conn:
        renamed = conn.execute(
            "SELECT category_name FROM categories WHERE category_id = ?",
            [created[0]],
        ).fetchone()
    assert renamed[0] == "Office Supplies"
    delete_category(created[0])
    with connect() as conn:
        deleted = conn.execute(
            "SELECT 1 FROM categories WHERE category_id = ?",
            [created[0]],
        ).fetchone()
    assert deleted is None


def test_efx_and_berian_are_classified_without_review() -> None:
    reset_db()
    upsert_manual_rates(__import__("datetime").date(2026, 3, 3), 3.95, 4.30)
    upsert_manual_rates(__import__("datetime").date(2026, 2, 26), 3.56, 4.10)
    path = next(SAMPLES.glob("*USD*.csv"))
    preview = build_preview(path.name, path.read_bytes())

    efx_rows = [row for row in preview.rows if "Transakcja eFX kurs" in row["description_raw"]]
    assert efx_rows
    assert all(row["vendor_canonical"] == "INTERNAL FX TRANSFER" for row in efx_rows)
    assert all(row["category_id"] is not None for row in efx_rows)
    assert all(row["transaction_type"] == "exchange" for row in efx_rows)

    berian_rows = [row for row in preview.rows if "BERIAN" in (row["vendor_canonical"] or row["description_raw"])]
    assert berian_rows
    assert all(row["vendor_canonical"] == "BERIAN" for row in berian_rows)
    assert any(row["category_id"] is not None for row in berian_rows)


def test_saved_rules_reclassify_existing_review_items() -> None:
    reset_db()
    path = next(SAMPLES.glob("*PLN*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)
    save_vendor_rule("JAMS", "JAMS DRUKARNIA")
    save_category_rule("JAMS", "vendor_or_description", "Food")
    reapply_rules_to_transactions()
    with connect() as conn:
        unresolved = conn.execute(
            """
            SELECT COUNT(*)
            FROM transactions
            WHERE upper(description_raw) LIKE '%JAMS%'
              AND (vendor_canonical IS NULL OR category_id IS NULL)
            """
        ).fetchone()[0]
    assert unresolved == 0


def test_fx_fetch_and_sample_import_helpers(monkeypatch) -> None:
    reset_db()
    from nmu_fin.services import bootstrap
    from nmu_fin.services.bootstrap import import_sample_history, list_import_batches, sample_transaction_dates
    from nmu_fin.services.fx import fetch_nbp_rates, list_rates

    fetched = fetch_nbp_rates(__import__("datetime").date(2026, 3, 3), __import__("datetime").date(2026, 3, 4))
    assert fetched >= 1
    assert list_rates()

    sample_dates = sample_transaction_dates()
    assert sample_dates
    assert min(sample_dates).isoformat() <= "2023-09-18"

    monkeypatch.setattr(bootstrap, "fetch_nbp_rates_for_dates", lambda _: 0)
    imported = import_sample_history()
    assert imported == 3
    batches = list_import_batches()
    assert len(batches) == 3


def test_exchange_transactions_are_excluded_from_default_cash_flow() -> None:
    reset_db()
    for rate_date, usd_pln, eur_pln in [
        (__import__("datetime").date(2026, 3, 3), 3.95, 4.30),
        (__import__("datetime").date(2026, 2, 26), 3.56, 4.10),
        (__import__("datetime").date(2026, 2, 19), 3.57, 4.16),
        (__import__("datetime").date(2026, 2, 10), 4.02, 4.20),
        (__import__("datetime").date(2026, 2, 9), 4.01, 4.19),
    ]:
        upsert_manual_rates(rate_date, usd_pln, eur_pln)
    for path in supported_sample_paths():
        preview = build_preview(path.name, path.read_bytes())
        commit_preview(preview)
    reapply_rules_to_transactions()

    default_dashboard = get_dashboard_data("PLN", include_transfers=False)
    inclusive_dashboard = get_dashboard_data("PLN", include_transfers=True)

    default_months = {row["year_month"]: row["net_cash_flow"] for row in default_dashboard.monthly_summary}
    inclusive_months = {row["year_month"]: row["net_cash_flow"] for row in inclusive_dashboard.monthly_summary}
    assert default_months != inclusive_months


def test_review_filters_by_currency_and_direction() -> None:
    reset_db()
    path = next(SAMPLES.glob("*USD*.csv"))
    preview = build_preview(path.name, path.read_bytes())
    commit_preview(preview)

    client = TestClient(app)
    response = client.get("/review?currency_filter=USD&direction_filter=expense")
    assert response.status_code == 200
    assert "currency_filter" in response.text


def test_duplicate_category_name_shows_message_instead_of_500() -> None:
    reset_db()
    client = TestClient(app)
    response = client.post(
        "/settings/categories",
        data={"category_name": "Income", "parent_category": ""},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert "message=" in response.headers["location"]


def test_review_category_options_are_bucketed() -> None:
    reset_db()
    options = get_category_options()
    assert "Sales" in options["income"]
    assert "Materials" in options["expense"]
    assert "Currency Exchange" in options["exchange"]


def test_selected_year_builds_annual_actuals_report() -> None:
    reset_db()
    from nmu_fin.services.bootstrap import import_sample_history

    import_sample_history()
    dashboard = get_dashboard_data("PLN", include_transfers=False, selected_year=2025)
    assert dashboard.annual_report is not None
    assert dashboard.annual_report["year"] == 2025
    assert dashboard.annual_report["months"][0] == "January"
    assert len(dashboard.annual_report["summary"]["income"]) == 12


def test_transactions_update_can_move_row_out_of_exchange_bucket() -> None:
    reset_db()
    client = TestClient(app)
    preview = build_preview(ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes())
    target = next(row for row in preview.rows if "PRZEWALUTOWAN" in row["description_clean"])
    commit_preview(preview)
    with connect() as conn:
        transaction_id = conn.execute(
            """
            SELECT id
            FROM transactions
            WHERE description_clean = ? AND amount_original = ? AND currency_original = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            [target["description_clean"], target["amount_original"], target["currency_original"]],
        ).fetchone()[0]
    response = client.post(
        f"/transactions/{transaction_id}",
        data={
            "vendor_canonical": "MIKOMAKO OY",
            "category_bucket": "income",
            "category_name": "Sales",
            "sort_by": "last_modified",
            "sort_dir": "desc",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    with connect() as conn:
        updated = conn.execute(
            """
            SELECT t.vendor_canonical, c.category_name, t.transaction_type
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE t.id = ?
            """,
            [transaction_id],
        ).fetchone()
    assert updated == ("MIKOMAKO OY", "Sales", "standard")


def test_reapply_rules_does_not_keep_income_category_on_expense_rows() -> None:
    reset_db()
    preview = build_preview(ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes())
    commit_preview(preview)
    save_category_rule("PAYPRO", "vendor_or_description", "Incomes")
    reapply_rules_to_transactions()
    with connect() as conn:
        mismatched = conn.execute(
            """
            SELECT COUNT(*)
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE t.vendor_canonical = 'PAYPRO'
              AND t.direction = 'expense'
              AND c.category_name = 'Incomes'
            """
        ).fetchone()[0]
    assert mismatched == 0


def test_transactions_update_merges_into_existing_vendor_name() -> None:
    reset_db()
    preview = build_preview(ALIOR_SAMPLE.name, ALIOR_SAMPLE.read_bytes())
    commit_preview(preview)
    with connect() as conn:
        target_vendor = conn.execute(
            """
            SELECT vendor_canonical
            FROM transactions
            WHERE vendor_canonical IS NOT NULL AND vendor_canonical <> ''
            GROUP BY 1
            HAVING COUNT(*) >= 1
            ORDER BY COUNT(*) DESC, vendor_canonical
            LIMIT 1
            """
        ).fetchone()[0]
        source_vendor = conn.execute(
            """
            SELECT vendor_canonical
            FROM transactions
            WHERE vendor_canonical IS NOT NULL AND vendor_canonical <> '' AND vendor_canonical <> ?
            GROUP BY 1
            HAVING COUNT(*) >= 1
            ORDER BY COUNT(*) ASC, vendor_canonical
            LIMIT 1
            """,
            [target_vendor],
        ).fetchone()[0]
        source_id = conn.execute(
            "SELECT id FROM transactions WHERE vendor_canonical = ? ORDER BY id DESC LIMIT 1",
            [source_vendor],
        ).fetchone()[0]
        target_count_before = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE vendor_canonical = ?",
            [target_vendor],
        ).fetchone()[0]
        source_count_before = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE vendor_canonical = ?",
            [source_vendor],
        ).fetchone()[0]
    client = TestClient(app)
    response = client.post(
        f"/transactions/{source_id}",
        data={
            "vendor_canonical": target_vendor,
            "previous_vendor_canonical": source_vendor,
            "category_bucket": "income",
            "category_name": "Wick Sales",
            "sort_by": "last_modified",
            "sort_dir": "desc",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    with connect() as conn:
        target_count_after = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE vendor_canonical = ?",
            [target_vendor],
        ).fetchone()[0]
        source_count_after = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE vendor_canonical = ?",
            [source_vendor],
        ).fetchone()[0]
        vendor_row_count = conn.execute(
            "SELECT COUNT(*) FROM vendors WHERE canonical_vendor = ?",
            [target_vendor],
        ).fetchone()[0]
        former_vendor_row_count = conn.execute(
            "SELECT COUNT(*) FROM vendors WHERE canonical_vendor = ?",
            [source_vendor],
        ).fetchone()[0]
    assert target_count_after == target_count_before + source_count_before
    assert source_count_after == 0
    assert vendor_row_count == 1
    assert former_vendor_row_count == 0
