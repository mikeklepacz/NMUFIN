from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

import plotly.express as px

from ..db import connect
from .reports_balances import get_month_end_balances
NON_OPERATIONAL_CATEGORIES = {
    "Currency Exchange",
    "Transfers",
    "WAGES",
    "ZUS",
    "Taxes",
    "Sales",
    "Wick Sales",
    "Pet Sales",
    "Incomes",
    "Investment",
    "VAT Return",
    "Cash",
}

NON_VENDOR_NAMES = {
    "INTERNAL TRANSFER",
    "INTERNAL FX TRANSFER",
    "UNKNOWN",
}


@dataclass(slots=True)
class DashboardData:
    yearly_totals: dict[str, float | int]
    monthly_summary: list[dict]
    category_breakdown: list[dict]
    category_share_rows: list[dict]
    vendor_leaderboard: list[dict]
    currency_summary: list[dict]
    income_client_leaderboard: list[dict]
    available_years: list[int]
    available_clients: list[str]
    available_banks: list[str]
    cash_flow_chart_html: str
    bank_balance_chart_html: str
    client_income_chart_html: str
    client_yoy_chart_html: str
    category_share_chart_html: str
    vendor_share_chart_html: str
    annual_report: dict | None
    cash_snapshot: dict[str, Any] = field(default_factory=dict)


def amount_column(currency: str) -> str:
    mapping = {"USD": "amount_usd", "PLN": "amount_pln", "EUR": "amount_eur"}
    return mapping[currency]


def get_latest_fx_reference(conn) -> tuple[float | None, float | None]:
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


def build_cash_snapshot(conn, display_currency: str, bank_filter: str | None) -> dict[str, Any]:
    bank_sql = bank_filter_sql(bank_filter)
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
    open_payables_rows = conn.execute(
        """
        SELECT currency_original, ROUND(SUM(amount_original), 2) AS total_amount
        FROM payables
        WHERE status IN ('open', 'overdue')
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    due_30_rows = conn.execute(
        """
        SELECT currency_original, ROUND(SUM(amount_original), 2) AS total_amount
        FROM payables
        WHERE status IN ('open', 'overdue')
          AND due_date <= CURRENT_DATE + INTERVAL '30 days'
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    amount_expr = amount_column(display_currency)
    burn_row = conn.execute(
        f"""
        WITH monthly_expense AS (
            SELECT
                year_month,
                ROUND(ABS(SUM({amount_expr})), 2) AS total_expense
            FROM transactions
            WHERE status <> 'duplicate'
              AND direction = 'expense'
              AND transaction_type = 'standard'
              AND {amount_expr} IS NOT NULL
              {bank_sql}
            GROUP BY 1
            ORDER BY year_month DESC
            LIMIT 6
        )
        SELECT ROUND(COALESCE(AVG(total_expense), 0), 2), COUNT(*)
        FROM monthly_expense
        """,
        apply_bank_filter([], bank_filter),
    ).fetchone()
    usd_pln, eur_pln = get_latest_fx_reference(conn)
    cash_on_hand = sum_converted(latest_balance_rows, display_currency, usd_pln, eur_pln)
    open_payables = sum_converted(open_payables_rows, display_currency, usd_pln, eur_pln)
    due_30 = sum_converted(due_30_rows, display_currency, usd_pln, eur_pln)
    monthly_burn = float(burn_row[0] or 0.0)
    months_used = int(burn_row[1] or 0)
    funding_gap_30 = round(max(0.0, (due_30 + monthly_burn) - cash_on_hand), 2)
    net_after_open = round(cash_on_hand - open_payables, 2)
    runway_months = round(net_after_open / monthly_burn, 2) if monthly_burn > 0 else None
    return {
        "cash_on_hand": cash_on_hand,
        "open_payables": open_payables,
        "due_30": due_30,
        "monthly_burn": monthly_burn,
        "burn_months_used": months_used,
        "required_30": round(due_30 + monthly_burn, 2),
        "funding_gap_30": funding_gap_30,
        "net_after_open": net_after_open,
        "runway_months": runway_months,
        "cash_by_currency": [{"currency_original": row[0], "amount": row[1]} for row in latest_balance_rows],
        "open_payables_by_currency": [{"currency_original": row[0], "amount": row[1]} for row in open_payables_rows],
    }


def year_range_filter_sql(year_start: int | None, year_end: int | None) -> str:
    if year_start is not None and year_end is not None:
        return "AND year BETWEEN ? AND ?"
    if year_start is not None:
        return "AND year >= ?"
    if year_end is not None:
        return "AND year <= ?"
    return ""


def apply_year_range_filter(params: list, year_start: int | None, year_end: int | None) -> list:
    if year_start is not None and year_end is not None:
        params.extend([year_start, year_end])
    elif year_start is not None:
        params.append(year_start)
    elif year_end is not None:
        params.append(year_end)
    return params


def bank_filter_sql(bank_name: str | None) -> str:
    return "" if not bank_name else "AND bank_name = ?"


def apply_bank_filter(params: list, bank_name: str | None) -> list:
    if bank_name:
        params.append(bank_name)
    return params


def combine_filters(year_start: int | None, year_end: int | None, bank_name: str | None) -> list:
    params: list = []
    apply_year_range_filter(params, year_start, year_end)
    apply_bank_filter(params, bank_name)
    return params


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


def figure_to_html(figure) -> str:
    figure.update_layout(
        autosize=True,
        margin=dict(l=48, r=48, t=24, b=24),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#fcfaf6",
        font=dict(family="IBM Plex Sans, Segoe UI, sans-serif", color="#1f2a28"),
        legend_title_text="",
        title=None,
    )
    figure.update_xaxes(showgrid=False, fixedrange=True, automargin=True)
    figure.update_yaxes(gridcolor="#ebe2d6", zerolinecolor="#d9cfbf", fixedrange=True, automargin=True)
    html = figure.to_html(
        full_html=False,
        include_plotlyjs="cdn",
        default_width="100%",
        config={
            "responsive": True,
            "displaylogo": False,
            "displayModeBar": False,
            "scrollZoom": False,
            "doubleClick": False,
            "showAxisDragHandles": False,
            "showAxisRangeEntryBoxes": False,
            "modeBarButtonsToRemove": [
                "zoom2d",
                "pan2d",
                "select2d",
                "lasso2d",
                "zoomIn2d",
                "zoomOut2d",
                "autoScale2d",
                "resetScale2d",
                "toggleSpikelines",
                "toImage",
            ],
        },
    )
    return html.replace(
        'style="height:420px; width:100%;"',
        'style="height:420px; width:min(100%, 1200px); margin:0 auto;"',
    )


def sales_category_filter_sql(alias: str = "t") -> str:
    return (
        f"AND COALESCE(c.category_name, '') IN ('Sales', 'Wick Sales', 'Pet Sales')"
        if alias == "t"
        else "AND COALESCE(c.category_name, '') IN ('Sales', 'Wick Sales', 'Pet Sales')"
    )


def simplify_client_name(name: str | None) -> str:
    if not name:
        return "UNKNOWN"
    simplified = " ".join(name.upper().split())
    simplified = re.sub(r"\s+ELIXIR\s+\d{2}-\d{2}-\d{4}$", "", simplified)
    simplified = re.sub(r"\s+US/HOPKINTON\.\d+\.\w+$", "", simplified)
    company_suffixes = [
        " LIMITED",
        " CORPORATION",
        " BV",
        " GMBH",
        " SP. Z O.O.",
        " SPÓŁKA Z OGRANICZONĄ ODPOWIEDZIALNOŚCIĄ",
        " D.O.O.",
    ]
    for suffix in company_suffixes:
        idx = simplified.find(suffix)
        if idx != -1:
            return simplified[: idx + len(suffix)].strip()
    address_markers = [" UL.", " AVENUE ", " STREET ", " ROAD ", " PLACE ", " AL."]
    for marker in address_markers:
        idx = simplified.find(marker)
        if idx != -1:
            return simplified[:idx].strip()
    digit_match = re.search(r"\s\d", simplified)
    if digit_match:
        simplified = simplified[: digit_match.start()].strip()
    tokens = simplified.split()
    if len(tokens) > 4:
        return " ".join(tokens[:4])
    return simplified


def operational_expense_filter(alias: str = "t", category_alias: str = "c") -> str:
    excluded = ", ".join(f"'{category}'" for category in sorted(NON_OPERATIONAL_CATEGORIES))
    return (
        f"AND {alias}.direction = 'expense' "
        f"AND COALESCE({category_alias}.parent_category, '') = 'Expenses' "
        f"AND COALESCE({category_alias}.category_name, '') NOT IN ({excluded}) "
        f"AND {alias}.transaction_type = 'standard'"
    )


def build_cash_flow_chart(monthly_rows) -> str:
    if monthly_rows.empty:
        return ""
    figure = px.line(
        monthly_rows,
        x="year_month",
        y=["income", "expenses", "net_cash_flow"],
        markers=True,
        color_discrete_map={
            "income": "#2f7d4d",
            "expenses": "#c65d2e",
            "net_cash_flow": "#1f4e8c",
        },
    )
    figure.update_traces(line=dict(width=3))
    figure.update_layout(
        height=420,
        showlegend=False,
    )
    figure.update_xaxes(title_text=None, domain=[0.08, 0.92])
    figure.update_yaxes(title_text=None)
    return figure_to_html(figure)


def build_pie_chart(rows, names: str, values: str, title: str, colors: list[str]) -> str:
    if rows.empty:
        return ""
    figure = px.pie(
        rows,
        names=names,
        values=values,
        hole=0.42,
        color_discrete_sequence=colors,
    )
    figure.update_traces(
        domain={"x": [0.32, 0.68], "y": [0.18, 0.9]},
        textposition="inside",
    )
    figure.update_layout(
        height=420,
        legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.08),
    )
    return figure_to_html(figure)


def build_horizontal_bar_chart(rows, x: str, y: str, colors: list[str]) -> str:
    if rows.empty:
        return ""
    figure = px.bar(
        rows.sort_values(x, ascending=True),
        x=x,
        y=y,
        orientation="h",
        color_discrete_sequence=colors,
    )
    figure.update_layout(height=420, showlegend=False)
    figure.update_xaxes(title_text=None, gridcolor="#ebe2d6", domain=[0.12, 0.9])
    figure.update_yaxes(title_text=None, automargin=True)
    return figure_to_html(figure)


def build_share_bar_chart(rows, label_col: str, value_col: str, colors: list[str]) -> str:
    if rows.empty:
        return ""
    share_rows = rows.copy()
    total = float(share_rows[value_col].sum())
    if total <= 0:
        return ""
    share_rows["share_pct"] = (share_rows[value_col] / total * 100).round(1)
    figure = px.bar(
        share_rows.sort_values("share_pct", ascending=True),
        x="share_pct",
        y=label_col,
        orientation="h",
        text="share_pct",
        color_discrete_sequence=colors,
    )
    figure.update_traces(texttemplate="%{text:.1f}%", textposition="outside", cliponaxis=False)
    figure.update_layout(height=420, showlegend=False)
    figure.update_xaxes(title_text="Share of operating expenses (%)", gridcolor="#ebe2d6")
    figure.update_yaxes(title_text=None, automargin=True)
    return figure_to_html(figure)


def build_share_rows(rows, label_col: str, value_col: str) -> list[dict]:
    if rows.empty:
        return []
    total = float(rows[value_col].sum())
    if total <= 0:
        return []
    share_rows = []
    for row in rows.sort_values(value_col, ascending=False).to_dict(orient="records"):
        share_rows.append(
            {
                "label": row[label_col],
                "total": row[value_col],
                "share_pct": round(float(row[value_col]) / total * 100, 1),
            }
        )
    return share_rows


def build_annual_report(conn, amount_expr: str, selected_year: int | None, bank_filter: str | None) -> dict | None:
    if selected_year is None:
        return None
    bank_sql = bank_filter_sql(bank_filter)
    summary_rows = conn.execute(
        f"""
        SELECT
            month,
            ROUND(SUM(CASE WHEN direction = 'income' THEN {amount_expr} ELSE 0 END), 2) AS income,
            ROUND(ABS(SUM(CASE WHEN direction = 'expense' THEN {amount_expr} ELSE 0 END)), 2) AS expenses,
            ROUND(SUM({amount_expr}), 2) AS net
        FROM transactions
        WHERE status <> 'duplicate'
          AND year = ?
          AND {amount_expr} IS NOT NULL
          {bank_sql}
          AND transaction_type NOT IN ('transfer', 'exchange')
        GROUP BY 1
        ORDER BY 1
        """,
        [selected_year, *apply_bank_filter([], bank_filter)],
    ).fetchall()
    if not summary_rows:
        return None

    month_map = {row[0]: {"income": row[1], "expenses": row[2], "net": row[3]} for row in summary_rows}
    summary = {
        "income": [month_map.get(month, {}).get("income") for month in range(1, 13)],
        "expenses": [month_map.get(month, {}).get("expenses") for month in range(1, 13)],
        "net": [month_map.get(month, {}).get("net") for month in range(1, 13)],
    }

    category_rows = conn.execute(
        f"""
        WITH category_totals AS (
            SELECT
                COALESCE(c.category_name, 'Uncategorized') AS category_name,
                month,
                ROUND(ABS(SUM(t.{amount_expr})), 2) AS total
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE t.status <> 'duplicate'
              AND t.year = ?
              AND t.{amount_expr} IS NOT NULL
              {bank_sql}
              {operational_expense_filter('t', 'c')}
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT
                category_name,
                SUM(total) AS year_total
            FROM category_totals
            GROUP BY 1
            ORDER BY year_total DESC
            LIMIT 12
        )
        SELECT c.category_name, c.month, c.total
        FROM category_totals c
        INNER JOIN ranked r ON r.category_name = c.category_name
        ORDER BY r.year_total DESC, c.category_name, c.month
        """,
        [selected_year, *apply_bank_filter([], bank_filter)],
    ).fetchall()

    category_map: dict[str, list[float | None]] = {}
    for category_name, month, total in category_rows:
        category_map.setdefault(category_name, [None] * 12)[month - 1] = total

    return {
        "year": selected_year,
        "months": ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"],
        "summary": summary,
        "rows": [{"label": label, "values": values} for label, values in category_map.items()],
    }


def build_client_income_chart(monthly_rows, focus_client: str | None) -> tuple[str, str]:
    if monthly_rows.empty:
        return "", ""

    chart_rows = monthly_rows
    if focus_client:
        chart_rows = chart_rows[chart_rows["client_name"] == focus_client]
        title = f"{focus_client} monthly income"
    else:
        top_clients = (
            monthly_rows.groupby("client_name", as_index=False)["income"]
            .sum()
            .sort_values("income", ascending=False)
            .head(5)["client_name"]
            .tolist()
        )
        chart_rows = monthly_rows[monthly_rows["client_name"].isin(top_clients)]
        title = "Top client income trends"

    line_chart = ""
    if not chart_rows.empty:
        figure = px.line(
            chart_rows,
            x="year_month",
            y="income",
            color="client_name",
            markers=True,
            color_discrete_sequence=["#1f4e8c", "#2f7d4d", "#b64d57", "#8d6cab", "#c68a2e"],
        )
        figure.update_traces(line=dict(width=3))
        figure.update_layout(
            height=420,
            legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.12, yanchor="top"),
        )
        figure.update_xaxes(title_text=None, domain=[0.08, 0.92])
        figure.update_yaxes(title_text=None)
        line_chart = figure_to_html(figure)

    yoy_chart = ""
    if focus_client and monthly_rows["year"].nunique() > 1:
        yoy_rows = monthly_rows[monthly_rows["client_name"] == focus_client].copy()
        if not yoy_rows.empty:
            yoy_rows["month_label"] = yoy_rows["month"].map(
                {
                    1: "Jan",
                    2: "Feb",
                    3: "Mar",
                    4: "Apr",
                    5: "May",
                    6: "Jun",
                    7: "Jul",
                    8: "Aug",
                    9: "Sep",
                    10: "Oct",
                    11: "Nov",
                    12: "Dec",
                }
            )
            figure = px.line(
                yoy_rows,
                x="month_label",
                y="income",
                color="year",
                markers=True,
                category_orders={"month_label": ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]},
                color_discrete_sequence=["#1f4e8c", "#2f7d4d", "#b64d57", "#c68a2e"],
            )
            figure.update_traces(line=dict(width=3))
            figure.update_layout(
                height=420,
                legend=dict(orientation="h", x=0.5, xanchor="center", y=-0.12, yanchor="top"),
            )
            figure.update_xaxes(title_text=None, domain=[0.08, 0.92])
            figure.update_yaxes(title_text=None)
            yoy_chart = figure_to_html(figure)

    return line_chart, yoy_chart


def build_bank_balance_chart(balance_rows: list[dict], display_currency: str) -> str:
    if not balance_rows:
        return ""
    import pandas as pd

    rows = pd.DataFrame(balance_rows)
    figure = px.line(
        rows,
        x="year_month",
        y="bank_balance",
        markers=True,
        color_discrete_sequence=["#8d6cab"],
    )
    figure.update_traces(line=dict(width=3), fill="tozeroy")
    figure.update_layout(height=420, showlegend=False)
    figure.update_xaxes(title_text=None, domain=[0.08, 0.92])
    figure.update_yaxes(title_text=None)
    return figure_to_html(figure)


def get_dashboard_data(
    display_currency: str = "PLN",
    include_transfers: bool = False,
    year_start: int | None = None,
    year_end: int | None = None,
    focus_client: str | None = None,
    bank_filter: str | None = None,
    selected_year: int | None = None,
) -> DashboardData:
    if selected_year is not None and year_start is None and year_end is None:
        year_start = selected_year
        year_end = selected_year
    if year_start is not None and year_end is not None and year_start > year_end:
        year_start, year_end = year_end, year_start
    amount_expr = amount_column(display_currency)
    transfer_filter = "AND transaction_type <> 'exchange'" if include_transfers else "AND transaction_type NOT IN ('transfer', 'exchange')"
    year_filter = year_range_filter_sql(year_start, year_end)
    bank_sql = bank_filter_sql(bank_filter)
    selected_year = year_start if year_start is not None and year_start == year_end else None
    with connect() as conn:
        annual_report = build_annual_report(conn, amount_expr, selected_year, bank_filter)
        available_years = [
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT year
                FROM transactions
                WHERE status <> 'duplicate'
                ORDER BY year
                """
            ).fetchall()
        ]
        monthly_rows = conn.execute(
            f"""
            SELECT
                year_month,
                year,
                month,
                ROUND(SUM(CASE WHEN direction = 'income' THEN {amount_expr} ELSE 0 END), 2) AS income,
                ROUND(ABS(SUM(CASE WHEN direction = 'expense' THEN {amount_expr} ELSE 0 END)), 2) AS expenses,
                ROUND(SUM({amount_expr}), 2) AS net_cash_flow
            FROM transactions
            WHERE status <> 'duplicate' AND {amount_expr} IS NOT NULL {transfer_filter} {year_filter} {bank_sql}
            GROUP BY 1, 2, 3
            ORDER BY 1
            """,
            combine_filters(year_start, year_end, bank_filter),
        ).df()
        summary_row = conn.execute(
            f"""
            SELECT
                ROUND(SUM(CASE WHEN direction = 'income' THEN {amount_expr} ELSE 0 END), 2) AS total_income,
                ROUND(ABS(SUM(CASE WHEN direction = 'expense' THEN {amount_expr} ELSE 0 END)), 2) AS total_expenses,
                ROUND(SUM({amount_expr}), 2) AS total_net,
                COUNT(DISTINCT year_month) AS months_covered
            FROM transactions
            WHERE status <> 'duplicate' AND {amount_expr} IS NOT NULL {transfer_filter} {year_filter} {bank_sql}
            """,
            combine_filters(year_start, year_end, bank_filter),
        ).fetchone()
        category_rows = conn.execute(
            f"""
            SELECT c.category_name, ROUND(ABS(SUM(t.{amount_expr})), 2) AS total
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE t.status <> 'duplicate'
              AND t.{amount_expr} IS NOT NULL
              {transfer_filter}
              {year_filter}
              {bank_sql}
              {operational_expense_filter('t', 'c')}
            GROUP BY 1
            HAVING SUM(t.{amount_expr}) <> 0
            ORDER BY total DESC
            """
            ,
            combine_filters(year_start, year_end, bank_filter),
        ).df()
        vendor_rows = conn.execute(
            f"""
            SELECT COALESCE(vendor_canonical, 'UNKNOWN') AS vendor, ROUND(ABS(SUM({amount_expr})), 2) AS total
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE t.status <> 'duplicate'
              AND t.{amount_expr} IS NOT NULL
              {transfer_filter}
              {year_filter}
              {bank_sql}
              {operational_expense_filter('t', 'c')}
              AND COALESCE(vendor_canonical, 'UNKNOWN') NOT IN ('INTERNAL TRANSFER', 'INTERNAL FX TRANSFER', 'UNKNOWN')
            GROUP BY 1
            ORDER BY total DESC
            LIMIT 10
            """,
            combine_filters(year_start, year_end, bank_filter),
        ).df()
        client_rows = conn.execute(
            f"""
            SELECT
                COALESCE(vendor_canonical, 'UNKNOWN') AS raw_client_name,
                year_month,
                year,
                month,
                ROUND(SUM({amount_expr}), 2) AS income
            FROM transactions
            LEFT JOIN categories c ON c.category_id = transactions.category_id
            WHERE status <> 'duplicate'
              AND direction = 'income'
              AND {amount_expr} IS NOT NULL
              {transfer_filter}
              {year_filter}
              {bank_sql}
              AND COALESCE(c.category_name, '') IN ('Sales', 'Wick Sales', 'Pet Sales')
            GROUP BY 1, 2, 3, 4
            HAVING SUM({amount_expr}) <> 0
            ORDER BY 2, 1
            """,
            combine_filters(year_start, year_end, bank_filter),
        ).df()
        client_leaderboard_rows = conn.execute(
            f"""
            SELECT
                COALESCE(vendor_canonical, 'UNKNOWN') AS raw_client_name,
                ROUND(SUM({amount_expr}), 2) AS total
            FROM transactions
            LEFT JOIN categories c ON c.category_id = transactions.category_id
            WHERE status <> 'duplicate'
              AND direction = 'income'
              AND {amount_expr} IS NOT NULL
              {transfer_filter}
              {year_filter}
              {bank_sql}
              AND COALESCE(c.category_name, '') IN ('Sales', 'Wick Sales', 'Pet Sales')
            GROUP BY 1
            HAVING SUM({amount_expr}) <> 0
            ORDER BY total DESC
            LIMIT 12
            """,
            combine_filters(year_start, year_end, bank_filter),
        ).df()
        available_clients = [
            row[0]
            for row in conn.execute(
                f"""
                SELECT DISTINCT COALESCE(vendor_canonical, 'UNKNOWN')
                FROM transactions
                LEFT JOIN categories c ON c.category_id = transactions.category_id
                WHERE status <> 'duplicate'
                  AND direction = 'income'
                  AND {amount_expr} IS NOT NULL
                  {transfer_filter}
                  {bank_sql}
                  AND COALESCE(c.category_name, '') IN ('Sales', 'Wick Sales', 'Pet Sales')
                ORDER BY 1
                """
            ,
                apply_bank_filter([], bank_filter),
            ).fetchall()
        ]
        currency_rows = conn.execute(
            f"""
            SELECT currency_original, ROUND(SUM(amount_original), 2) AS total
            FROM transactions
            WHERE status <> 'duplicate'
              {year_filter}
              {bank_sql}
            GROUP BY 1
            ORDER BY 1
            """,
            combine_filters(year_start, year_end, bank_filter),
        ).fetchall()

    if not client_rows.empty:
        client_rows["client_name"] = client_rows["raw_client_name"].map(simplify_client_name)
        client_rows = (
            client_rows.groupby(["client_name", "year_month", "year", "month"], as_index=False)["income"]
            .sum()
            .sort_values(["year_month", "client_name"])
        )
    if not client_leaderboard_rows.empty:
        client_leaderboard_rows["client_name"] = client_leaderboard_rows["raw_client_name"].map(simplify_client_name)
        client_leaderboard_rows = (
            client_leaderboard_rows.groupby("client_name", as_index=False)["total"]
            .sum()
            .sort_values("total", ascending=False)
            .head(12)
        )
    available_clients = sorted({simplify_client_name(name) for name in available_clients if simplify_client_name(name) != "UNKNOWN"})

    if focus_client and focus_client not in available_clients:
        focus_client = None

    monthly_avg_net = round((summary_row[2] or 0) / summary_row[3], 2) if summary_row[3] else 0.0
    yearly_totals = {
        "income": summary_row[0] or 0.0,
        "expenses": summary_row[1] or 0.0,
        "net": summary_row[2] or 0.0,
        "months_covered": summary_row[3] or 0,
        "avg_monthly_net": monthly_avg_net,
    }

    balance_rows = get_month_end_balances(display_currency, year_start, year_end)
    cash_flow_chart_html = build_cash_flow_chart(monthly_rows)
    bank_balance_chart_html = build_bank_balance_chart(balance_rows, display_currency)
    client_income_chart_html, client_yoy_chart_html = build_client_income_chart(client_rows, focus_client)
    category_share_rows = build_share_rows(category_rows.head(8), "category_name", "total")
    category_share_chart_html = ""
    vendor_share_chart_html = build_horizontal_bar_chart(
        vendor_rows.head(8),
        "total",
        "vendor",
        ["#1f4e8c"],
    )

    return DashboardData(
        yearly_totals=yearly_totals,
        monthly_summary=monthly_rows.to_dict(orient="records"),
        category_breakdown=category_rows.to_dict(orient="records"),
        category_share_rows=category_share_rows,
        vendor_leaderboard=vendor_rows.to_dict(orient="records"),
        currency_summary=[{"currency_original": row[0], "total": row[1]} for row in currency_rows],
        income_client_leaderboard=client_leaderboard_rows.to_dict(orient="records"),
        available_years=available_years,
        available_clients=available_clients,
        available_banks=get_bank_options(),
        cash_flow_chart_html=cash_flow_chart_html,
        bank_balance_chart_html=bank_balance_chart_html,
        client_income_chart_html=client_income_chart_html,
        client_yoy_chart_html=client_yoy_chart_html,
        category_share_chart_html=category_share_chart_html,
        vendor_share_chart_html=vendor_share_chart_html,
        annual_report=annual_report,
    )


def get_transactions(limit: int = 500, include_duplicates: bool = False) -> list[dict]:
    duplicate_filter = "" if include_duplicates else "WHERE t.status <> 'duplicate'"
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                t.id, t.transaction_date, t.account_name, COALESCE(t.vendor_canonical, 'UNKNOWN'),
                COALESCE(c.category_name, 'Uncategorized'), t.currency_original, t.amount_original, t.status,
                COALESCE(t.description_en, t.description_raw)
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            {duplicate_filter}
            ORDER BY t.transaction_date DESC, t.id DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    return [
        {
            "id": row[0],
            "transaction_date": row[1],
            "account_name": row[2],
            "vendor": row[3],
            "category": row[4],
            "currency": row[5],
            "amount_original": row[6],
            "status": row[7],
            "description": row[8],
        }
        for row in rows
    ]
