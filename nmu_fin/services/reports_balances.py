from __future__ import annotations

from datetime import date

from ..config import SAMPLE_IMPORT_DIR
from ..db import connect
from ..parsers import parse_known_csv
from .fx import convert_amount_with_rates, get_rate_map_for_dates


def get_supported_sample_paths():
    for path in sorted(SAMPLE_IMPORT_DIR.glob("*.csv")):
        try:
            parse_known_csv(path.name, path.read_bytes())
        except ValueError:
            continue
        yield path


def get_month_end_balances(display_currency: str, year_start: int | None, year_end: int | None) -> list[dict]:
    if year_start is not None and year_end is not None and year_start > year_end:
        year_start, year_end = year_end, year_start
    month_end_rows: dict[tuple[str, str], dict] = {}
    transaction_dates: set[date] = set()
    for path in get_supported_sample_paths():
        parsed = parse_known_csv(path.name, path.read_bytes())
        for row in parsed.rows:
            if row.balance is None:
                continue
            transaction_date = row.transaction_date.date()
            if year_start is not None and transaction_date.year < year_start:
                continue
            if year_end is not None and transaction_date.year > year_end:
                continue
            year_month = transaction_date.strftime("%Y-%m")
            key = (parsed.account_id, year_month)
            candidate = {
                "account_id": parsed.account_id,
                "currency": parsed.currency,
                "transaction_date": transaction_date,
                "year_month": year_month,
                "balance": float(row.balance),
                "sort_key": (transaction_date, row.row_number),
            }
            if key not in month_end_rows or candidate["sort_key"] > month_end_rows[key]["sort_key"]:
                month_end_rows[key] = candidate
                transaction_dates.add(transaction_date)

    if not month_end_rows:
        return []

    with connect() as conn:
        rate_rows = get_rate_map_for_dates(conn, sorted(transaction_dates))
    rates_by_date: dict[date, dict[tuple[str, str], float]] = {}
    for rate_date, from_currency, to_currency, rate in rate_rows:
        rates_by_date.setdefault(rate_date, {})[(from_currency, to_currency)] = rate

    monthly_totals: dict[str, float] = {}
    for row in month_end_rows.values():
        converted = convert_amount_with_rates(
            row["transaction_date"],
            row["currency"],
            row["balance"],
            rates_by_date.get(row["transaction_date"], {}),
        )
        amount = {
            "USD": converted.amount_usd,
            "PLN": converted.amount_pln,
            "EUR": converted.amount_eur,
        }[display_currency]
        if amount is None:
            continue
        monthly_totals[row["year_month"]] = round(monthly_totals.get(row["year_month"], 0.0) + amount, 2)

    return [
        {"year_month": year_month, "bank_balance": monthly_totals[year_month]}
        for year_month in sorted(monthly_totals)
    ]
