from __future__ import annotations

from datetime import date
from urllib.parse import urlencode

from ..db import connect


SORT_COLUMNS = {
    "last_modified": "t.updated_at",
    "date": "t.transaction_date",
    "bank": "COALESCE(t.bank_name, '')",
    "vendor": "COALESCE(t.vendor_canonical, '')",
    "category": "COALESCE(c.category_name, '')",
    "amount": "ABS(t.amount_original)",
}


def build_transaction_filters(
    include_duplicates: bool = False,
    bank_filter: str | None = None,
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    status_filter: str | None = None,
    vendor_filter: str | None = None,
    category_filter: str | None = None,
) -> tuple[str, list[str | int]]:
    filters = []
    params: list[str | int] = []
    if not include_duplicates:
        filters.append("t.status <> 'duplicate'")
    if bank_filter:
        filters.append("t.bank_name = ?")
        params.append(bank_filter)
    if currency_filter:
        filters.append("t.currency_original = ?")
        params.append(currency_filter)
    if direction_filter:
        filters.append("t.direction = ?")
        params.append(direction_filter)
    if status_filter:
        filters.append("t.status = ?")
        params.append(status_filter)
    if vendor_filter:
        filters.append(
            "upper(COALESCE(t.vendor_canonical, '') || ' ' || COALESCE(t.vendor_raw, '') || ' ' || COALESCE(t.description_raw, '') || ' ' || COALESCE(t.description_en, '')) LIKE ?"
        )
        params.append(f"%{vendor_filter.upper()}%")
    if category_filter:
        filters.append("c.category_name = ?")
        params.append(category_filter)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    return where_clause, params


def get_transaction_vendor_options() -> list[str]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT vendor_name
            FROM (
                SELECT DISTINCT canonical_vendor AS vendor_name
                FROM vendors
                WHERE canonical_vendor IS NOT NULL AND canonical_vendor <> ''
                UNION
                SELECT DISTINCT COALESCE(vendor_canonical, '')
                FROM transactions
                WHERE vendor_canonical IS NOT NULL AND vendor_canonical <> ''
            )
            ORDER BY 1
            """
        ).fetchall()
    return [row[0] for row in rows]


def list_transactions(
    include_duplicates: bool = False,
    bank_filter: str | None = None,
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    status_filter: str | None = None,
    vendor_filter: str | None = None,
    category_filter: str | None = None,
    sort_by: str = "last_modified",
    sort_dir: str = "desc",
    limit: int = 200,
) -> list[dict]:
    where_clause, params = build_transaction_filters(
        include_duplicates=include_duplicates,
        bank_filter=bank_filter,
        currency_filter=currency_filter,
        direction_filter=direction_filter,
        status_filter=status_filter,
        vendor_filter=vendor_filter,
        category_filter=category_filter,
    )
    order_column = SORT_COLUMNS.get(sort_by, SORT_COLUMNS["last_modified"])
    order_dir = "ASC" if sort_dir == "asc" else "DESC"
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                t.id, t.transaction_date, t.updated_at, COALESCE(t.description_en, t.description_raw),
                COALESCE(t.bank_name, ''), t.account_name, COALESCE(t.vendor_canonical, ''), COALESCE(c.category_name, ''),
                t.currency_original, t.amount_original, t.status, t.direction, t.transaction_type, COALESCE(c.parent_category, '')
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            {where_clause}
            ORDER BY {order_column} {order_dir}, t.id DESC
            LIMIT ?
            """,
            [*params, limit],
        ).fetchall()
    return [
        {
            "id": row[0],
            "transaction_date": row[1],
            "updated_at": row[2],
            "description": row[3],
            "bank_name": row[4],
            "account_name": row[5],
            "vendor": row[6],
            "category": row[7],
            "currency": row[8],
            "amount_original": row[9],
            "status": row[10],
            "direction": row[11],
            "transaction_type": row[12],
            "category_bucket": (
                "exchange"
                if row[13] == "Exchange" or row[12] == "exchange"
                else "income"
                if row[13] == "Income"
                else "expense"
            ),
        }
        for row in rows
    ]


def get_transaction_metrics(
    include_duplicates: bool = False,
    bank_filter: str | None = None,
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    status_filter: str | None = None,
    vendor_filter: str | None = None,
    category_filter: str | None = None,
) -> dict:
    where_clause, params = build_transaction_filters(
        include_duplicates=include_duplicates,
        bank_filter=bank_filter,
        currency_filter=currency_filter,
        direction_filter=direction_filter,
        status_filter=status_filter,
        vendor_filter=vendor_filter,
        category_filter=category_filter,
    )
    with connect() as conn:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(*) FILTER (WHERE t.status = 'ready') AS ready_count,
                COUNT(*) FILTER (WHERE t.status = 'needs_review') AS needs_review_count,
                COUNT(*) FILTER (WHERE t.status = 'duplicate') AS duplicate_count,
                ROUND(COALESCE(SUM(t.amount_usd), 0), 2) AS net_usd,
                ROUND(COALESCE(SUM(t.amount_pln), 0), 2) AS net_pln,
                ROUND(COALESCE(SUM(t.amount_eur), 0), 2) AS net_eur,
                ROUND(COALESCE(SUM(ABS(t.amount_usd)), 0), 2) AS volume_usd,
                ROUND(COALESCE(SUM(ABS(t.amount_pln)), 0), 2) AS volume_pln,
                ROUND(COALESCE(SUM(ABS(t.amount_eur)), 0), 2) AS volume_eur,
                ROUND(COALESCE(ABS(SUM(CASE WHEN t.direction = 'expense' THEN t.amount_usd ELSE 0 END)), 0), 2) AS expense_total_usd,
                ROUND(COALESCE(ABS(SUM(CASE WHEN t.direction = 'expense' THEN t.amount_pln ELSE 0 END)), 0), 2) AS expense_total_pln,
                ROUND(COALESCE(ABS(SUM(CASE WHEN t.direction = 'expense' THEN t.amount_eur ELSE 0 END)), 0), 2) AS expense_total_eur,
                MIN(t.transaction_date) AS min_date,
                MAX(t.transaction_date) AS max_date
            FROM transactions t
            LEFT JOIN categories c ON c.category_id = t.category_id
            {where_clause}
            """,
            params,
        ).fetchone()
    min_date: date | None = row[13]
    max_date: date | None = row[14]
    months_span = 0
    if min_date and max_date:
        months_span = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1
    avg_monthly_usd = round((row[10] or 0.0) / months_span, 2) if months_span else 0.0
    avg_monthly_pln = round((row[11] or 0.0) / months_span, 2) if months_span else 0.0
    avg_monthly_eur = round((row[12] or 0.0) / months_span, 2) if months_span else 0.0
    return {
        "row_count": row[0] or 0,
        "ready_count": row[1] or 0,
        "needs_review_count": row[2] or 0,
        "duplicate_count": row[3] or 0,
        "net_usd": row[4] or 0.0,
        "net_pln": row[5] or 0.0,
        "net_eur": row[6] or 0.0,
        "volume_usd": row[7] or 0.0,
        "volume_pln": row[8] or 0.0,
        "volume_eur": row[9] or 0.0,
        "avg_monthly_spend_usd": avg_monthly_usd,
        "avg_monthly_spend_pln": avg_monthly_pln,
        "avg_monthly_spend_eur": avg_monthly_eur,
        "months_span": months_span,
        "period_start": min_date,
        "period_end": max_date,
    }


def update_transaction(
    transaction_id: int,
    vendor_canonical: str,
    category_name: str,
    category_bucket: str | None = None,
    previous_vendor_canonical: str | None = None,
) -> None:
    with connect() as conn:
        category = conn.execute(
            "SELECT category_id, parent_category FROM categories WHERE category_name = ?",
            [category_name],
        ).fetchone()
        row = conn.execute(
            "SELECT amount_original, amount_usd, amount_pln, amount_eur FROM transactions WHERE id = ?",
            [transaction_id],
        ).fetchone()
        category_id = category[0] if category else None
        parent_category = category[1] if category else None
        selected_bucket = (
            "exchange"
            if parent_category == "Exchange"
            else "income"
            if parent_category == "Income"
            else "expense"
        )
        if category_bucket and category_bucket != selected_bucket:
            category_id = None
        transaction_type = (
            "exchange"
            if selected_bucket == "exchange"
            else "transfer"
            if category_name == "Transfers"
            else "standard"
        )
        direction = "income" if row[0] > 0 else "expense"
        status = "ready" if vendor_canonical.strip() and category_id and None not in row[1:4] else "needs_review"
        canonical_vendor = vendor_canonical.strip()
        conn.execute(
            """
            UPDATE transactions
            SET vendor_canonical = ?, category_id = ?, transaction_type = ?, direction = ?, status = ?, needs_review = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            [canonical_vendor, category_id, transaction_type, direction, status, status == "needs_review", transaction_id],
        )
        if previous_vendor_canonical and previous_vendor_canonical.strip() and previous_vendor_canonical.strip() != canonical_vendor:
            previous_vendor = previous_vendor_canonical.strip()
            target_vendor_exists = conn.execute(
                "SELECT 1 FROM vendors WHERE canonical_vendor = ?",
                [canonical_vendor],
            ).fetchone()
            conn.execute(
                """
                UPDATE transactions
                SET vendor_canonical = ?, updated_at = CURRENT_TIMESTAMP
                WHERE vendor_canonical = ?
                """,
                [canonical_vendor, previous_vendor],
            )
            conn.execute(
                """
                UPDATE vendor_rules
                SET canonical_vendor = ?
                WHERE canonical_vendor = ?
                """,
                [canonical_vendor, previous_vendor],
            )
            if target_vendor_exists:
                conn.execute(
                    "DELETE FROM vendors WHERE canonical_vendor = ?",
                    [previous_vendor],
                )
            else:
                conn.execute(
                    """
                    UPDATE vendors
                    SET vendor_name = ?, canonical_vendor = ?
                    WHERE canonical_vendor = ?
                    """,
                    [canonical_vendor, canonical_vendor, previous_vendor],
                )


def transaction_query_string(**kwargs: str | bool | int | None) -> str:
    params = {key: value for key, value in kwargs.items() if value not in (None, "", False)}
    return urlencode(params)
