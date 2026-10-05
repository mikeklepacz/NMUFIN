from __future__ import annotations

from datetime import date
from decimal import Decimal

import duckdb

from nmu_fin.services.cash_balances import _end_of_day, _matching_summary, build_bank_cash_report


def test_cash_uses_bank_balances_and_keeps_negative_balance_visible() -> None:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE accounts (account_id VARCHAR, bank_name VARCHAR, account_name VARCHAR, currency VARCHAR)")
    conn.execute("""CREATE TABLE transactions (
        id INTEGER, transaction_date DATE, posting_date DATE, account_id VARCHAR, currency_original VARCHAR,
        amount_original DOUBLE, balance DOUBLE, description_raw VARCHAR,
        transaction_type VARCHAR, bank_name VARCHAR, account_name VARCHAR, import_batch_id VARCHAR, status VARCHAR
    )""")
    conn.execute("CREATE TABLE fx_rates (rate_date DATE, from_currency VARCHAR, to_currency VARCHAR, rate DOUBLE)")
    conn.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?)", [
        ("PLN-A", "Bank", "PLN account", "PLN"),
        ("USD-B", "Bank", "USD account", "USD"),
        ("LEGACY", "Alior", "Mixed legacy", "MULTI"),
    ])
    reference = "00133328260105787430 - Transakcja eFX kurs: 4.0000000"
    conn.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready')", [
        (1, date(2025, 12, 31), date(2025, 12, 31), "PLN-A", "PLN", 100, 100, "opening", "standard", "Bank", "PLN account", "old"),
        (2, date(2025, 12, 31), date(2025, 12, 31), "USD-B", "USD", 50, 50, "opening", "standard", "Bank", "USD account", "old"),
        # The later ID is not the day's closing row; the balance chain decides.
        (3, date(2026, 1, 5), date(2026, 1, 5), "PLN-A", "PLN", -60, 80, "supplier", "standard", "Bank", "PLN account", "new"),
        (4, date(2026, 1, 4), date(2026, 1, 5), "PLN-A", "PLN", 40, 140, reference, "exchange", "Bank", "PLN account", "new"),
        (5, date(2026, 1, 5), date(2026, 1, 5), "USD-B", "USD", -10, 40, reference, "exchange", "Bank", "USD account", "new"),
        (6, date(2026, 1, 6), date(2026, 1, 6), "PLN-A", "PLN", -300, -220, "supplier", "standard", "Bank", "PLN account", "new"),
        (7, date(2026, 1, 6), date(2026, 1, 6), "LEGACY", "PLN", 10, 10, "old", "transfer", "Alior", "Mixed legacy", "new"),
        (8, date(2026, 1, 6), date(2026, 1, 6), "LEGACY", "USD", 10, 10, "old", "transfer", "Alior", "Mixed legacy", "new"),
    ])
    conn.executemany("INSERT INTO fx_rates VALUES (?, ?, ?, ?)", [
        (date(2025, 12, 31), "USD", "PLN", 4.0),
        (date(2026, 1, 5), "USD", "PLN", 4.0),
        (date(2026, 1, 6), "USD", "PLN", 4.0),
    ])

    report = build_bank_cash_report(conn, "PLN", 2026, 2026, None)

    assert report["as_of"] == "2026-01-06"
    assert report["total"] == -60.0
    assert {item["account_id"]: item["balance"] for item in report["accounts"]} == {
        "PLN-A": -220.0, "USD-B": 40.0,
    }
    assert report["daily"][-1] == {"date": "2026-01-06", "bank_balance": -60.0}
    assert not any(row["date"] == "2026-01-04" for row in report["daily"])
    assert report["matches"]["named_fx_pairs"] == 1
    assert report["matches"]["named_fx_needing_review"] == 0
    assert report["partial"] is True
    assert any("mixed-currency" in warning for warning in report["warnings"])
    conn.close()


def test_reference_matches_delayed_same_currency_transfer() -> None:
    rows = [
        {"date": date(2026, 2, 20), "account": "ALIOR:USD", "currency": "USD",
         "amount": -1000, "description": "przelew wlasny Ref dewiz.SW/260219OSW007210", "type": "transfer"},
        {"date": date(2026, 2, 24), "account": "ERSTE:USD", "currency": "USD",
         "amount": 1000, "description": "REF: 260219OSW007210 przelew wlasny", "type": "transfer"},
    ]
    summary = _matching_summary(rows, date(2026, 1, 1), date(2026, 12, 31))
    assert summary["referenced_own_pairs"] == 1
    assert summary["referenced_own_needing_review"] == 0


def test_fx_reference_with_wrong_bank_rate_needs_review() -> None:
    description = "00133328260105787430 - Transakcja eFX kurs: 4.0000000"
    rows = [
        {"date": date(2026, 1, 5), "account": "USD", "currency": "USD",
         "amount": -10, "description": description, "type": "exchange"},
        {"date": date(2026, 1, 5), "account": "PLN", "currency": "PLN",
         "amount": 30, "description": description, "type": "exchange"},
    ]
    summary = _matching_summary(rows, date(2026, 1, 1), date(2026, 12, 31))
    assert summary["named_fx_pairs"] == 0
    assert summary["named_fx_needing_review"] == 1


def test_fx_reference_accepts_bank_rounded_source_amount() -> None:
    description = "00133328260219787358 - Transakcja eFX kurs: 3.5681000"
    rows = [
        {"date": date(2026, 2, 19), "account": "USD", "currency": "USD",
         "amount": Decimal("-336.31"), "description": description, "type": "exchange"},
        {"date": date(2026, 2, 19), "account": "PLN", "currency": "PLN",
         "amount": Decimal("1200.00"), "description": description, "type": "exchange"},
    ]
    summary = _matching_summary(rows, date(2026, 1, 1), date(2026, 12, 31))
    assert summary["named_fx_pairs"] == 1
    assert summary["named_fx_needing_review"] == 0


def test_fx_round_trip_can_return_to_same_balance_with_verified_source_order() -> None:
    rows = [
        {"id": 10, "batch": "one", "amount": -1400, "balance": 655.40},
        {"id": 11, "batch": "one", "amount": 1400, "balance": 2055.40},
        {"id": 12, "batch": "one", "amount": -130.64, "balance": 655.40},
    ]
    assert _end_of_day(rows) == Decimal("655.40")
