"""Bank-reported account balances and reference-backed internal movements."""

from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
import re


MAX_REPORT_ROWS = 100_000
CENT = Decimal("0.01")
EFX_REFERENCE = re.compile(r"(\d{12,})\s*-\s*Transakcja\s+eFX", re.IGNORECASE)
EFX_RATE = re.compile(r"Transakcja\s+eFX\s+kurs:\s*([0-9.]+)", re.IGNORECASE)
OWN_REFERENCE = re.compile(r"(?<![A-Z0-9])(\d{6}[A-Z]{2,4}\d{5,})", re.IGNORECASE)


def _day(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _decimal(value: float | str) -> Decimal:
    return Decimal(str(value)).quantize(CENT)


def _end_of_day(rows: list[dict]) -> Decimal | None:
    """Find the final posted balance from the day's balance transition chain."""
    starts = [_decimal(row["balance"]) - _decimal(row["amount"]) for row in rows]
    candidates = {
        _decimal(row["balance"])
        for index, row in enumerate(rows)
        if not any(other != index and before == _decimal(row["balance"]) for other, before in enumerate(starts))
    }
    if len(candidates) == 1:
        return next(iter(candidates))
    # An FX round trip can revisit the same balance on one day. Within one
    # import batch, bank rows are newest-first; verify their balance chain.
    if len({row.get("batch") for row in rows}) == 1 and all(row.get("id") is not None for row in rows):
        ordered = sorted(rows, key=lambda row: int(row["id"]))
        if all(
            _decimal(current["balance"]) - _decimal(current["amount"]) == _decimal(older["balance"])
            for current, older in zip(ordered, ordered[1:])
        ):
            return _decimal(ordered[0]["balance"])
    return None


def _matching_summary(rows: list[dict], start: date, end: date) -> dict[str, int]:
    efx: dict[str, list[dict]] = defaultdict(list)
    own: dict[str, list[dict]] = defaultdict(list)
    other_exchanges = 0
    for row in rows:
        if not start <= row["date"] <= end:
            continue
        description = row["description"] or ""
        fx_reference = EFX_REFERENCE.search(description)
        if fx_reference:
            efx[fx_reference.group(1)].append(row)
            continue
        if row["type"] == "exchange":
            other_exchanges += 1
        if row["type"] not in ("exchange", "transfer"):
            continue
        own_reference = OWN_REFERENCE.search(description)
        if own_reference:
            own[own_reference.group(1).upper()].append(row)

    def valid_pair(legs: list[dict], cross_currency: bool) -> bool:
        if len(legs) != 2 or legs[0]["account"] == legs[1]["account"]:
            return False
        if legs[0]["amount"] * legs[1]["amount"] >= 0:
            return False
        if abs((legs[0]["date"] - legs[1]["date"]).days) > 7:
            return False
        if cross_currency:
            if legs[0]["currency"] == legs[1]["currency"]:
                return False
            rate_match = EFX_RATE.search(legs[0]["description"] or "")
            if not rate_match:
                return False
            rate = Decimal(rate_match.group(1))
            if rate <= 0:
                return False
            outgoing = abs(next(leg["amount"] for leg in legs if leg["amount"] < 0))
            incoming = next(leg["amount"] for leg in legs if leg["amount"] > 0)
            return min(abs(outgoing * rate - incoming), abs(outgoing / rate - incoming)) <= CENT
        return legs[0]["currency"] == legs[1]["currency"] and abs(legs[0]["amount"]) == abs(legs[1]["amount"])

    return {
        "named_fx_pairs": sum(valid_pair(legs, True) for legs in efx.values()),
        "named_fx_needing_review": sum(not valid_pair(legs, True) for legs in efx.values()),
        "referenced_own_pairs": sum(valid_pair(legs, False) for legs in own.values()),
        "referenced_own_needing_review": sum(not valid_pair(legs, False) for legs in own.values()),
        "other_exchange_legs": other_exchanges,
    }


def build_bank_cash_report(
    conn, display_currency: str, year_start: int | None, year_end: int | None, bank_filter: str | None
) -> dict:
    if display_currency not in {"PLN", "USD", "EUR"}:
        raise ValueError("Unsupported display currency")
    params: list = []
    end_sql = ""
    if year_end is not None:
        end_sql = "AND t.posting_date <= ?"
        params.append(date(year_end, 12, 31))
    account_rows = conn.execute("SELECT account_id, bank_name, account_name, currency FROM accounts").fetchall()
    accounts = {row[0]: {"bank": row[1], "name": row[2], "currency": row[3]} for row in account_rows}
    raw_rows = conn.execute(
        f"""
        SELECT t.id, t.posting_date, t.account_id, t.currency_original,
               t.amount_original, t.balance, t.description_raw, t.transaction_type,
               t.bank_name, t.account_name, t.import_batch_id
        FROM transactions t
        WHERE t.status <> 'duplicate' {end_sql}
        ORDER BY t.posting_date, t.account_id, t.id
        LIMIT {MAX_REPORT_ROWS + 1}
        """,
        params,
    ).fetchall()
    if len(raw_rows) > MAX_REPORT_ROWS:
        raise ValueError("Bank-balance report exceeds its row limit")
    rows = [
        {
            "id": row[0], "date": _day(row[1]), "account": row[2],
            "currency": row[3], "amount": _decimal(row[4]),
            "balance": row[5], "description": row[6], "type": row[7],
            "bank": row[8], "account_name": row[9], "batch": row[10],
        }
        for row in raw_rows
    ]
    if not rows:
        return {"total": None, "as_of": None, "accounts": [], "daily": [], "partial": True,
                "warnings": ["No imported bank transactions."], "matches": {}}

    selected = [row for row in rows if not bank_filter or row["bank"] == bank_filter]
    if not selected:
        return {"total": None, "as_of": None, "accounts": [], "daily": [], "partial": True,
                "warnings": ["No imported bank transactions for these filters."], "matches": {}}
    start = date(year_start, 1, 1) if year_start is not None else min(row["date"] for row in selected)
    last_imported_date = max(row["date"] for row in selected)
    end = min(date(year_end, 12, 31), last_imported_date) if year_end is not None else last_imported_date
    if start > end:
        return {"total": None, "as_of": None, "accounts": [], "daily": [], "partial": True,
                "warnings": ["No imported bank transactions for these filters."], "matches": {}}
    currencies_by_account: dict[str, set[str]] = defaultdict(set)
    for row in selected:
        currencies_by_account[row["account"]].add(row["currency"])
    ambiguous = {
        account for account, currencies in currencies_by_account.items()
        if len(currencies) != 1 or accounts.get(account, {}).get("currency") in ("MULTI", None)
    }

    transitions: dict[tuple[str, date], list[dict]] = defaultdict(list)
    for row in selected:
        if row["balance"] is not None and row["account"] not in ambiguous:
            transitions[row["account"], row["date"]].append(row)
    closing = {key: _end_of_day(day_rows) for key, day_rows in transitions.items()}
    no_balance = set(currencies_by_account) - {account for account, _ in closing} - ambiguous
    unresolved = {account for (account, day), value in closing.items() if value is None and start <= day <= end}

    rates_raw = conn.execute(
        "SELECT rate_date, from_currency, rate FROM fx_rates WHERE to_currency = ? ORDER BY from_currency, rate_date",
        [display_currency],
    ).fetchall()
    rates: dict[str, tuple[list[date], list[Decimal]]] = defaultdict(lambda: ([], []))
    for rate_date, currency, rate in rates_raw:
        rates[currency][0].append(_day(rate_date))
        rates[currency][1].append(Decimal(str(rate)))

    def converted(amount: Decimal, currency: str, on: date) -> Decimal | None:
        if currency == display_currency:
            return amount
        days, values = rates[currency]
        index = bisect_right(days, on) - 1
        if index < 0 or (on - days[index]).days > 7:
            return None
        return amount * values[index]

    balance_by_account: dict[str, Decimal] = {}
    last_date: dict[str, date] = {}
    gaps: set[str] = set()
    daily: list[dict] = []
    for account, day in sorted(closing, key=lambda key: (key[1], key[0])):
        if day >= start:
            continue
        value = closing[account, day]
        if value is not None:
            balance_by_account[account] = value
            last_date[account] = day
    day = start
    while day <= end:
        posted_today = False
        for account in currencies_by_account:
            key = account, day
            if key not in closing:
                continue
            posted_today = True
            value = closing[key]
            if value is None:
                balance_by_account.pop(account, None)
                continue
            if account in balance_by_account:
                movement = sum((row["amount"] for row in transitions[key]), Decimal("0"))
                if abs(balance_by_account[account] + movement - value) > CENT:
                    gaps.add(account)
            balance_by_account[account] = value
            last_date[account] = day
        values = []
        missing_rates = False
        for account, balance in balance_by_account.items():
            currency = next(iter(currencies_by_account[account]))
            value = converted(balance, currency, day)
            if value is None:
                missing_rates = True
            else:
                values.append(value)
        if day.weekday() < 5 or posted_today:
            daily.append({"date": day.isoformat(), "bank_balance": round(float(sum(values)), 2) if values and not missing_rates else None})
        day += timedelta(days=1)

    account_details = []
    for account, balance in sorted(balance_by_account.items()):
        currency = next(iter(currencies_by_account[account]))
        value = converted(balance, currency, end)
        account_details.append({
            "account_id": account, "name": accounts.get(account, {}).get("name") or account,
            "currency": currency, "balance": float(balance),
            "value": round(float(value), 2) if value is not None else None,
            "last_posted": last_date[account].isoformat(),
            "reconciliation_gap": account in gaps,
        })
    partial = bool(ambiguous or no_balance or unresolved or gaps or any(item["value"] is None for item in account_details))
    total = None
    if account_details and all(item["value"] is not None for item in account_details):
        total = round(float(sum(
            converted(balance, next(iter(currencies_by_account[account])), end)
            for account, balance in balance_by_account.items()
        )), 2)
    warnings = []
    if ambiguous:
        warnings.append(f"{len(ambiguous)} mixed-currency account identity needs source-account mapping; excluded from the displayed balance.")
    if no_balance:
        warnings.append(f"{len(no_balance)} account(s) have no bank-reported running balance; excluded from the displayed balance.")
    if unresolved:
        warnings.append(f"{len(unresolved)} account(s) have a day with no unique closing bank balance.")
    if gaps:
        warnings.append(f"{len(gaps)} account(s) have a bank-balance rollforward gap in this period.")
    if any(item["value"] is None for item in account_details):
        warnings.append("A currency rate is missing; no combined balance is shown.")
    return {
        "total": total, "as_of": end.isoformat(), "accounts": account_details,
        "daily": daily, "partial": partial, "warnings": warnings,
        "matches": _matching_summary(rows, start, end),
    }
