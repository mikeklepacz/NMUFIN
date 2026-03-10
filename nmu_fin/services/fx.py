from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from json import loads
from urllib.error import HTTPError
from urllib.request import urlopen

from ..db import connect


@dataclass(slots=True)
class ConvertedAmounts:
    amount_usd: float | None
    amount_pln: float | None
    amount_eur: float | None
    status: str


BASE_CURRENCIES = ("USD", "PLN", "EUR")


def upsert_manual_rates(rate_date: date, usd_pln: float, eur_pln: float) -> None:
    derived = {
        ("USD", "PLN"): usd_pln,
        ("PLN", "USD"): 1 / usd_pln,
        ("EUR", "PLN"): eur_pln,
        ("PLN", "EUR"): 1 / eur_pln,
        ("USD", "EUR"): usd_pln / eur_pln,
        ("EUR", "USD"): eur_pln / usd_pln,
        ("USD", "USD"): 1.0,
        ("PLN", "PLN"): 1.0,
        ("EUR", "EUR"): 1.0,
    }
    with connect() as conn:
        for (from_currency, to_currency), rate in derived.items():
            conn.execute(
                """
                INSERT INTO fx_rates(rate_date, from_currency, to_currency, rate)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(rate_date, from_currency, to_currency)
                DO UPDATE SET rate = excluded.rate
                """,
                [rate_date, from_currency, to_currency, rate],
            )


def get_rate_map(rate_date: date) -> dict[tuple[str, str], float]:
    with connect() as conn:
        rows = get_rate_map_for_dates(conn, [rate_date])
    return {(row[0], row[1]): row[2] for row in rows}


def get_rate_map_for_dates(conn, rate_dates: list[date]) -> list[tuple[str, str, str, float]]:
    if not rate_dates:
        return []
    placeholders = ",".join(["?"] * len(rate_dates))
    return conn.execute(
        f"""
        SELECT rate_date, from_currency, to_currency, rate
        FROM fx_rates
        WHERE rate_date IN ({placeholders})
        """,
        rate_dates,
    ).fetchall()


def convert_amount_with_rates(
    rate_date: date, currency: str, amount: float, rates: dict[tuple[str, str], float]
) -> ConvertedAmounts:
    if not rates:
        return ConvertedAmounts(None, None, None, "fx_pending")
    converted: dict[str, float | None] = {}
    for target in BASE_CURRENCIES:
        if currency == target:
            converted[target] = amount
            continue
        rate = rates.get((currency, target))
        converted[target] = round(amount * rate, 2) if rate else None
    status = "ready" if all(converted.values()) else "fx_pending"
    return ConvertedAmounts(
        amount_usd=converted["USD"],
        amount_pln=converted["PLN"],
        amount_eur=converted["EUR"],
        status=status,
    )


def convert_amount(rate_date: date, currency: str, amount: float) -> ConvertedAmounts:
    rate_rows = get_rate_map(rate_date)
    return convert_amount_with_rates(rate_date, currency, amount, rate_rows)


def list_rates(limit: int = 500) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT
                rate_date,
                MAX(CASE WHEN from_currency = 'USD' AND to_currency = 'PLN' THEN rate END) AS usd_pln,
                MAX(CASE WHEN from_currency = 'EUR' AND to_currency = 'PLN' THEN rate END) AS eur_pln
            FROM fx_rates
            GROUP BY 1
            ORDER BY rate_date DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    return [
        {"rate_date": row[0], "usd_pln": row[1], "eur_pln": row[2]}
        for row in rows
    ]


def get_rate_coverage() -> dict:
    with connect() as conn:
        row = conn.execute(
            """
            SELECT
                MIN(rate_date),
                MAX(rate_date),
                COUNT(DISTINCT rate_date)
            FROM fx_rates
            WHERE from_currency = 'USD' AND to_currency = 'PLN'
            """
        ).fetchone()
    return {
        "oldest_rate_date": row[0],
        "newest_rate_date": row[1],
        "stored_rate_days": row[2],
    }


def fetch_nbp_rates(start_date: date, end_date: date) -> int:
    return fetch_nbp_rates_for_dates(
        [start_date + timedelta(days=offset) for offset in range((end_date - start_date).days + 1)]
    )


def fetch_nbp_rates_for_dates(rate_dates: list[date]) -> int:
    if not rate_dates:
        return 0
    unique_dates = sorted(set(rate_dates))
    earliest = unique_dates[0] - timedelta(days=7)
    latest = unique_dates[-1]
    usd_history = fetch_nbp_history("usd", earliest, latest)
    eur_history = fetch_nbp_history("eur", earliest, latest)
    inserted = 0
    for current in unique_dates:
        usd_pln = find_last_available_rate(usd_history, current)
        eur_pln = find_last_available_rate(eur_history, current)
        if usd_pln and eur_pln:
            upsert_manual_rates(current, usd_pln, eur_pln)
            inserted += 1
    return inserted


def fetch_nbp_mid_rate(currency_code: str, target_date: date) -> float | None:
    for offset in range(0, 7):
        lookup_date = target_date - timedelta(days=offset)
        url = f"https://api.nbp.pl/api/exchangerates/rates/A/{currency_code}/{lookup_date.isoformat()}/?format=json"
        try:
            with urlopen(url) as response:
                payload = loads(response.read().decode("utf-8"))
                return float(payload["rates"][0]["mid"])
        except HTTPError as exc:
            if exc.code == 404:
                continue
            raise
    return None


def fetch_nbp_history(currency_code: str, start_date: date, end_date: date) -> dict[date, float]:
    history: dict[date, float] = {}
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=92), end_date)
        url = (
            "https://api.nbp.pl/api/exchangerates/rates/A/"
            f"{currency_code}/{current.isoformat()}/{chunk_end.isoformat()}/?format=json"
        )
        try:
            with urlopen(url) as response:
                payload = loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404:
                current = chunk_end + timedelta(days=1)
                continue
            raise
        for row in payload["rates"]:
            history[date.fromisoformat(row["effectiveDate"])] = float(row["mid"])
        current = chunk_end + timedelta(days=1)
    return history


def find_last_available_rate(history: dict[date, float], target_date: date) -> float | None:
    for offset in range(0, 7):
        lookup_date = target_date - timedelta(days=offset)
        if lookup_date in history:
            return history[lookup_date]
    return None
