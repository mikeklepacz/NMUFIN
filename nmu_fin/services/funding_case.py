from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from ..db import connect


@dataclass(slots=True)
class FundingCaseData:
    currency: str
    bank_filter: str | None
    burn_window_months: int
    cash_on_hand: float
    open_obligations: float
    due_30: float
    due_90: float
    monthly_burn: float
    monthly_revenue: float
    required_30: float
    required_90: float
    funding_gap_30: float
    funding_gap_90: float
    runway_months: float | None
    cash_by_currency: list[dict[str, Any]]
    obligations_by_currency: list[dict[str, Any]]
    obligations_ladder: list[dict[str, Any]]
    obligations_by_category: list[dict[str, Any]]
    monthly_trend: list[dict[str, Any]]
    recurring_profiles: list[dict[str, Any]]
    recurring_monthly_total: float
    recurring_next_90: float
    variable_monthly_avg: float
    variable_next_90: float
    variable_p90_monthly: float
    jit_buffer_90: float
    total_need_90_jit: float
    funding_gap_90_jit: float


RECURRING_KEYWORDS = (
    "RENT",
    "PHONE",
    "INTERNET",
    "INSURANCE",
    "HEALTH",
    "SALARY",
    "SUBSCRIPTION",
    "SOFTWARE",
    "LEASE",
    "UTILITY",
)

VARIABLE_KEYWORDS = (
    "MATERIAL",
    "YARN",
    "FABRIC",
    "PRODUCTION",
    "PACKAGING",
    "FREIGHT",
    "SHIPPING",
    "LOGISTICS",
)


def amount_column(currency: str) -> str:
    mapping = {"USD": "amount_usd", "PLN": "amount_pln", "EUR": "amount_eur"}
    return mapping.get(currency, "amount_pln")


def bank_filter_sql(bank_name: str | None) -> str:
    return "" if not bank_name else "AND bank_name = ?"


def apply_bank_filter(params: list, bank_name: str | None) -> list:
    if bank_name:
        params.append(bank_name)
    return params


def latest_fx_rates(conn) -> tuple[float | None, float | None]:
    latest_rate_date = conn.execute("SELECT MAX(rate_date) FROM fx_rates").fetchone()[0]
    if latest_rate_date is None:
        return None, None
    rows = conn.execute(
        """
        SELECT from_currency, to_currency, rate
        FROM fx_rates
        WHERE rate_date = ?
        """,
        [latest_rate_date],
    ).fetchall()
    usd_pln = None
    eur_pln = None
    for from_currency, to_currency, rate in rows:
        if from_currency == "USD" and to_currency == "PLN":
            usd_pln = float(rate)
        if from_currency == "EUR" and to_currency == "PLN":
            eur_pln = float(rate)
    return usd_pln, eur_pln


def convert_currency(amount: float, from_currency: str, to_currency: str, usd_pln: float | None, eur_pln: float | None) -> float | None:
    if from_currency == to_currency:
        return float(amount)
    if usd_pln is None or eur_pln is None:
        return None
    if from_currency == "PLN":
        amount_pln = float(amount)
    elif from_currency == "USD":
        amount_pln = float(amount) * usd_pln
    elif from_currency == "EUR":
        amount_pln = float(amount) * eur_pln
    else:
        return None
    if to_currency == "PLN":
        return amount_pln
    if to_currency == "USD":
        return amount_pln / usd_pln if usd_pln else None
    if to_currency == "EUR":
        return amount_pln / eur_pln if eur_pln else None
    return None


def sum_converted(rows: list[tuple[str, float]], target_currency: str, usd_pln: float | None, eur_pln: float | None) -> float:
    total = 0.0
    for currency, amount in rows:
        converted = convert_currency(float(amount), currency, target_currency, usd_pln, eur_pln)
        if converted is not None:
            total += converted
    return round(total, 2)


def get_bank_options() -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT bank_name
            FROM transactions
            WHERE bank_name IS NOT NULL AND bank_name <> ''
            ORDER BY 1
            """
        ).fetchall()
    return [row[0] for row in rows]


def get_funding_case_data(display_currency: str = "PLN", bank_filter: str | None = None, burn_window_months: int = 6) -> FundingCaseData:
    burn_window_months = max(3, min(24, int(burn_window_months)))
    amount_expr = amount_column(display_currency)
    today = date.today()
    bank_sql = bank_filter_sql(bank_filter)
    with connect() as conn:
        usd_pln, eur_pln = latest_fx_rates(conn)
        latest_balance_rows = conn.execute(
            f"""
            WITH ranked AS (
                SELECT
                    account_id,
                    currency_original,
                    balance,
                    transaction_date,
                    id,
                    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY transaction_date DESC, id DESC) AS rn
                FROM transactions
                WHERE status <> 'duplicate'
                  AND balance IS NOT NULL
                  {bank_sql}
            )
            SELECT currency_original, ROUND(SUM(balance), 2) AS total_balance
            FROM ranked
            WHERE rn = 1
            GROUP BY 1
            ORDER BY 1
            """,
            apply_bank_filter([], bank_filter),
        ).fetchall()
        payable_rows = conn.execute(
            """
            SELECT
                p.currency_original,
                p.amount_original,
                p.due_date,
                p.status,
                COALESCE(c.category_name, 'Uncategorized') AS category_name
            FROM payables p
            LEFT JOIN categories c ON c.category_id = p.category_id
            WHERE p.status IN ('open', 'overdue')
            """
        ).fetchall()
        trend_rows = conn.execute(
            f"""
            SELECT
                year_month,
                ROUND(SUM(CASE WHEN direction = 'income' THEN {amount_expr} ELSE 0 END), 2) AS income,
                ROUND(ABS(SUM(CASE WHEN direction = 'expense' THEN {amount_expr} ELSE 0 END)), 2) AS expenses,
                ROUND(SUM({amount_expr}), 2) AS net_cash_flow
            FROM transactions
            WHERE status <> 'duplicate'
              AND transaction_type = 'standard'
              AND {amount_expr} IS NOT NULL
              {bank_sql}
            GROUP BY 1
            ORDER BY year_month DESC
            LIMIT 12
            """,
            apply_bank_filter([], bank_filter),
        ).fetchall()
        expense_category_rows = conn.execute(
            f"""
            SELECT
                year_month,
                COALESCE(c.category_name, 'Uncategorized') AS category_name,
                ROUND(ABS(SUM({amount_expr})), 2) AS total_expense
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE t.status <> 'duplicate'
              AND t.direction = 'expense'
              AND t.transaction_type = 'standard'
              AND {amount_expr} IS NOT NULL
              {bank_sql}
            GROUP BY 1, 2
            ORDER BY year_month DESC
            LIMIT 240
            """,
            apply_bank_filter([], bank_filter),
        ).fetchall()

    cash_on_hand = sum_converted([(row[0], row[1]) for row in latest_balance_rows], display_currency, usd_pln, eur_pln)
    open_obligations = sum_converted([(row[0], row[1]) for row in payable_rows], display_currency, usd_pln, eur_pln)
    due_30_rows = [(row[0], row[1]) for row in payable_rows if (row[2] - today).days <= 30]
    due_90_rows = [(row[0], row[1]) for row in payable_rows if (row[2] - today).days <= 90]
    due_30 = sum_converted(due_30_rows, display_currency, usd_pln, eur_pln)
    due_90 = sum_converted(due_90_rows, display_currency, usd_pln, eur_pln)

    trend_desc = [{"year_month": row[0], "income": row[1] or 0.0, "expenses": row[2] or 0.0, "net_cash_flow": row[3] or 0.0} for row in trend_rows]
    trend = list(reversed(trend_desc))
    recent_for_burn = trend_desc[:burn_window_months]
    monthly_burn = round(sum(row["expenses"] for row in recent_for_burn) / len(recent_for_burn), 2) if recent_for_burn else 0.0
    monthly_revenue = round(sum(row["income"] for row in recent_for_burn) / len(recent_for_burn), 2) if recent_for_burn else 0.0
    required_30 = round(due_30 + monthly_burn, 2)
    required_90 = round(due_90 + monthly_burn * 3, 2)
    funding_gap_30 = round(max(0.0, required_30 - cash_on_hand), 2)
    funding_gap_90 = round(max(0.0, required_90 - cash_on_hand), 2)
    net_after_open = round(cash_on_hand - open_obligations, 2)
    runway_months = round(net_after_open / monthly_burn, 2) if monthly_burn > 0 else None

    ladder_buckets = [
        ("Overdue", lambda d: d < today),
        ("0-30 days", lambda d: 0 <= (d - today).days <= 30),
        ("31-60 days", lambda d: 31 <= (d - today).days <= 60),
        ("61-90 days", lambda d: 61 <= (d - today).days <= 90),
        ("90+ days", lambda d: (d - today).days > 90),
    ]
    obligations_ladder = []
    for bucket_name, check in ladder_buckets:
        bucket_rows = [(row[0], row[1]) for row in payable_rows if check(row[2])]
        obligations_ladder.append({"bucket": bucket_name, "amount": sum_converted(bucket_rows, display_currency, usd_pln, eur_pln)})

    category_map: dict[str, float] = {}
    for currency, amount, _, _, category_name in payable_rows:
        converted = convert_currency(float(amount), currency, display_currency, usd_pln, eur_pln)
        if converted is None:
            continue
        category_map[category_name] = round(category_map.get(category_name, 0.0) + converted, 2)
    obligations_by_category = [
        {"category_name": category_name, "amount": amount}
        for category_name, amount in sorted(category_map.items(), key=lambda item: item[1], reverse=True)
    ]

    month_keys = sorted({row[0] for row in trend_rows})
    month_count = len(month_keys) or 1
    monthly_by_category: dict[str, dict[str, float]] = {}
    for year_month, category_name, total_expense in expense_category_rows:
        if year_month not in month_keys:
            continue
        category_map_for_month = monthly_by_category.setdefault(category_name, {})
        category_map_for_month[year_month] = float(total_expense or 0.0)

    recurring_profiles: list[dict[str, Any]] = []
    recurring_monthly_total = 0.0
    monthly_variable_totals = {year_month: 0.0 for year_month in month_keys}
    for category_name, category_month_map in monthly_by_category.items():
        upper = category_name.upper()
        months_seen = len(category_month_map)
        avg_window = round(sum(category_month_map.get(ym, 0.0) for ym in month_keys) / month_count, 2) if month_count else 0.0
        last_month = max(category_month_map.keys()) if category_month_map else None
        last_amount = category_month_map[last_month] if last_month else 0.0
        keyword_recurring = any(token in upper for token in RECURRING_KEYWORDS)
        keyword_variable = any(token in upper for token in VARIABLE_KEYWORDS)
        is_recurring = keyword_recurring and not keyword_variable
        if is_recurring:
            recurring_profiles.append(
                {
                    "category_name": category_name,
                    "months_seen": months_seen,
                    "avg_monthly": avg_window,
                    "last_amount": round(last_amount, 2),
                    "projected_3m": round(avg_window * 3, 2),
                }
            )
            recurring_monthly_total += avg_window
        else:
            for ym in month_keys:
                monthly_variable_totals[ym] = round(monthly_variable_totals.get(ym, 0.0) + category_month_map.get(ym, 0.0), 2)

    recurring_profiles.sort(key=lambda row: row["avg_monthly"], reverse=True)
    recurring_monthly_total = round(recurring_monthly_total, 2)
    recurring_next_90 = round(recurring_monthly_total * 3, 2)
    variable_series = [value for _, value in sorted(monthly_variable_totals.items())]
    variable_monthly_avg = round(sum(variable_series) / len(variable_series), 2) if variable_series else 0.0
    variable_next_90 = round(variable_monthly_avg * 3, 2)
    if variable_series:
        ordered = sorted(variable_series)
        p90_idx = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.9))))
        variable_p90_monthly = round(ordered[p90_idx], 2)
    else:
        variable_p90_monthly = 0.0
    jit_buffer_90 = round(max(0.0, variable_p90_monthly - variable_monthly_avg), 2)
    total_need_90_jit = round(due_90 + recurring_next_90 + variable_next_90 + jit_buffer_90, 2)
    funding_gap_90_jit = round(max(0.0, total_need_90_jit - cash_on_hand), 2)

    return FundingCaseData(
        currency=display_currency,
        bank_filter=bank_filter,
        burn_window_months=burn_window_months,
        cash_on_hand=cash_on_hand,
        open_obligations=open_obligations,
        due_30=due_30,
        due_90=due_90,
        monthly_burn=monthly_burn,
        monthly_revenue=monthly_revenue,
        required_30=required_30,
        required_90=required_90,
        funding_gap_30=funding_gap_30,
        funding_gap_90=funding_gap_90,
        runway_months=runway_months,
        cash_by_currency=[{"currency_original": row[0], "amount": row[1]} for row in latest_balance_rows],
        obligations_by_currency=[
            {"currency_original": currency, "amount": round(sum(amount for ccy, amount, *_ in payable_rows if ccy == currency), 2)}
            for currency in sorted({row[0] for row in payable_rows})
        ],
        obligations_ladder=obligations_ladder,
        obligations_by_category=obligations_by_category,
        monthly_trend=trend,
        recurring_profiles=recurring_profiles,
        recurring_monthly_total=recurring_monthly_total,
        recurring_next_90=recurring_next_90,
        variable_monthly_avg=variable_monthly_avg,
        variable_next_90=variable_next_90,
        variable_p90_monthly=variable_p90_monthly,
        jit_buffer_90=jit_buffer_90,
        total_need_90_jit=total_need_90_jit,
        funding_gap_90_jit=funding_gap_90_jit,
    )
