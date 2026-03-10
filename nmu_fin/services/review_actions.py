from __future__ import annotations

from ..db import connect


def next_review_focus_id(
    transaction_id: int,
    currency_filter: str | None = None,
    direction_filter: str | None = None,
    vendor_filter: str | None = None,
    transaction_filter: str | None = None,
    sort_by: str = "date_new",
) -> int | None:
    filters = ["needs_review = TRUE OR status = 'needs_review'"]
    params: list[str] = []
    order_by = {
        "date_old": "transaction_date ASC, id ASC",
        "largest": "ABS(amount_original) DESC, transaction_date DESC, id DESC",
        "smallest": "ABS(amount_original) ASC, transaction_date DESC, id DESC",
    }.get(sort_by, "transaction_date DESC, id DESC")
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
        filters.append("upper(COALESCE(description_en, description_raw, '')) LIKE ?")
        params.append(f"%{transaction_filter.upper()}%")
    with connect() as conn:
        review_ids = [
            row[0]
            for row in conn.execute(
                """
                SELECT id
                FROM transactions
                WHERE {where_clause}
                ORDER BY {order_by}
                """
                .format(where_clause=" AND ".join(f"({item})" for item in filters), order_by=order_by),
                params,
            ).fetchall()
        ]
    if transaction_id not in review_ids:
        return None
    current_index = review_ids.index(transaction_id)
    fallback_index = min(current_index + 1, len(review_ids) - 1)
    return review_ids[fallback_index] if len(review_ids) > 1 else None


def delete_transaction(transaction_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM transactions WHERE id = ?", [transaction_id])


def build_review_redirect(
    transaction_id: int | None,
    currency_filter: str | None,
    direction_filter: str | None,
    vendor_filter: str | None,
    transaction_filter: str | None,
    sort_by: str = "date_new",
    limit: int = 200,
) -> str:
    query = []
    if transaction_id:
        query.append(f"focus_id={transaction_id}")
    if currency_filter:
        query.append(f"currency_filter={currency_filter}")
    if direction_filter:
        query.append(f"direction_filter={direction_filter}")
    if vendor_filter:
        query.append(f"vendor_filter={vendor_filter}")
    if transaction_filter:
        query.append(f"transaction_filter={transaction_filter}")
    if sort_by != "date_new":
        query.append(f"sort_by={sort_by}")
    if limit != 200:
        query.append(f"limit={limit}")
    return f"/review?{'&'.join(query)}" if query else "/review"
