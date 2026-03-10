from __future__ import annotations

from ..db import connect


def get_review_queue(
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    vendor_filter: str | None = None,
    transaction_filter: str | None = None,
    sort_by: str = "date_new",
    limit: int = 200,
) -> list[dict]:
    filters = ["(t.needs_review = TRUE OR t.status = 'needs_review')"]
    params: list[str] = []
    order_by = {
        "date_old": "t.transaction_date ASC, t.id ASC",
        "largest": "ABS(t.amount_original) DESC, t.transaction_date DESC, t.id DESC",
        "smallest": "ABS(t.amount_original) ASC, t.transaction_date DESC, t.id DESC",
    }.get(sort_by, "t.transaction_date DESC, t.id DESC")
    if currency_filter:
        filters.append("t.currency_original = ?")
        params.append(currency_filter)
    if direction_filter:
        filters.append("t.direction = ?")
        params.append(direction_filter)
    if vendor_filter:
        filters.append(
            "upper(COALESCE(t.vendor_canonical, '') || ' ' || COALESCE(t.vendor_raw, '') || ' ' || COALESCE(t.description_raw, '')) LIKE ?"
        )
        params.append(f"%{vendor_filter.upper()}%")
    if transaction_filter:
        filters.append(
            "upper(COALESCE(t.description_en, t.description_raw, '')) LIKE ?"
        )
        params.append(f"%{transaction_filter.upper()}%")
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                t.id, t.transaction_date, COALESCE(t.description_en, t.description_raw), t.vendor_raw, t.vendor_canonical,
                c.category_name, t.amount_original, t.currency_original, t.status,
                t.amount_usd, t.amount_pln, t.amount_eur, t.direction, t.transaction_type
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            WHERE {' AND '.join(filters)}
            ORDER BY {order_by}
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
    return [
        {
            "id": row[0],
            "transaction_date": row[1],
            "description_raw": row[2],
            "vendor_raw": row[3],
            "vendor_canonical": row[4],
            "category_name": row[5],
            "amount_original": row[6],
            "currency_original": row[7],
            "status": row[8],
            "direction": row[12],
            "category_bucket": (
                "exchange" if row[13] == "exchange" else "income" if row[12] == "income" else "expense"
            ),
            "review_reason": build_review_reason(
                vendor_canonical=row[4],
                category_name=row[5],
                amount_usd=row[9],
                amount_pln=row[10],
                amount_eur=row[11],
            ),
        }
        for row in rows
    ]


def get_review_totals(
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    vendor_filter: str | None = None,
    transaction_filter: str | None = None,
) -> list[dict]:
    filters = ["(needs_review = TRUE OR status = 'needs_review')"]
    params: list[str] = []
    if currency_filter:
        filters.append("currency_original = ?")
        params.append(currency_filter)
    if direction_filter:
        filters.append("direction = ?")
        params.append(direction_filter)
    if vendor_filter:
        filters.append(
            "upper(COALESCE(vendor_canonical, '') || ' ' || COALESCE(vendor_raw, '') || ' ' || COALESCE(description_raw, '')) LIKE ?"
        )
        params.append(f"%{vendor_filter.upper()}%")
    if transaction_filter:
        filters.append(
            "upper(COALESCE(description_en, description_raw, '')) LIKE ?"
        )
        params.append(f"%{transaction_filter.upper()}%")
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT currency_original, ROUND(SUM(amount_original), 2) AS total_amount
            FROM transactions
            WHERE {' AND '.join(filters)}
            GROUP BY 1
            ORDER BY 1
            """,
            params,
        ).fetchall()
    return [{"currency": row[0], "total_amount": row[1]} for row in rows]


def get_review_vendor_groups(
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    vendor_filter: str | None = None,
    transaction_filter: str | None = None,
    limit: int = 15,
) -> list[dict]:
    filters = ["(needs_review = TRUE OR status = 'needs_review')"]
    params: list[str | int] = []
    if currency_filter:
        filters.append("currency_original = ?")
        params.append(currency_filter)
    if direction_filter:
        filters.append("direction = ?")
        params.append(direction_filter)
    if vendor_filter:
        filters.append(
            "upper(COALESCE(vendor_canonical, '') || ' ' || COALESCE(vendor_raw, '') || ' ' || COALESCE(description_raw, '')) LIKE ?"
        )
        params.append(f"%{vendor_filter.upper()}%")
    if transaction_filter:
        filters.append(
            "upper(COALESCE(description_en, description_raw, '')) LIKE ?"
        )
        params.append(f"%{transaction_filter.upper()}%")
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                COALESCE(NULLIF(vendor_canonical, ''), NULLIF(vendor_raw, ''), 'UNKNOWN') AS review_vendor,
                COUNT(*) AS review_count,
                COUNT(*) FILTER (WHERE direction = 'income') AS income_count,
                COUNT(*) FILTER (WHERE direction = 'expense') AS expense_count
            FROM transactions
            WHERE {' AND '.join(filters)}
            GROUP BY 1
            ORDER BY review_count DESC, review_vendor
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
    return [
        {
            "vendor_name": row[0],
            "review_count": row[1],
            "income_count": row[2],
            "expense_count": row[3],
        }
        for row in rows
    ]


def get_categories() -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT category_name FROM categories WHERE category_name NOT IN ('Income', 'Expenses') ORDER BY category_name"
        ).fetchall()
    return [row[0] for row in rows]


def get_category_options() -> dict[str, list[str]]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT category_name, parent_category
            FROM categories
            ORDER BY category_name
            """
        ).fetchall()
    options = {"income": [], "expense": [], "exchange": []}
    for category_name, parent_category in rows:
        if parent_category == "Income":
            options["income"].append(category_name)
        elif parent_category == "Expenses":
            options["expense"].append(category_name)
        elif parent_category == "Exchange":
            options["exchange"].append(category_name)
    return options


def get_transaction_status_counts() -> dict[str, int]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT status, COUNT(*)
            FROM transactions
            GROUP BY 1
            """
        ).fetchall()
    counts = {row[0]: row[1] for row in rows}
    return {
        "ready": counts.get("ready", 0),
        "needs_review": counts.get("needs_review", 0),
        "duplicate": counts.get("duplicate", 0),
    }


def build_review_reason(
    vendor_canonical: str | None,
    category_name: str | None,
    amount_usd: float | None,
    amount_pln: float | None,
    amount_eur: float | None,
) -> str:
    reasons: list[str] = []
    if not vendor_canonical:
        reasons.append("Vendor missing")
    if not category_name:
        reasons.append("Category missing")
    if amount_usd is None or amount_pln is None or amount_eur is None:
        reasons.append("FX rates missing")
    return ", ".join(reasons) if reasons else "Needs review"
