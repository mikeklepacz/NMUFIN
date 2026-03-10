from __future__ import annotations

from datetime import date
from typing import Any

from ..db import connect


FIXED_COST_KEYWORDS = ("RENT", "INSURANCE", "HEALTH", "WAGE", "SALARY", "MATERIAL", "YARN")


def get_vendor_options() -> list[str]:
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


def list_payables(status: str = "open") -> list[dict[str, Any]]:
    mark_overdue_payables()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT
                p.payable_id,
                p.vendor_canonical,
                COALESCE(c.category_name, ''),
                p.currency_original,
                p.amount_original,
                p.due_date,
                p.note,
                p.status,
                p.linked_transaction_id,
                p.paid_at
            FROM payables p
            LEFT JOIN categories c ON c.category_id = p.category_id
            WHERE (? = 'all' OR p.status = ?)
            ORDER BY
                CASE p.status WHEN 'overdue' THEN 0 WHEN 'open' THEN 1 WHEN 'paid' THEN 2 ELSE 3 END,
                p.due_date,
                p.payable_id
            """,
            [status, status],
        ).fetchall()
    return [
        {
            "payable_id": row[0],
            "vendor_canonical": row[1],
            "category_name": row[2],
            "currency_original": row[3],
            "amount_original": row[4],
            "due_date": row[5],
            "note": row[6] or "",
            "status": row[7],
            "linked_transaction_id": row[8],
            "paid_at": row[9],
        }
        for row in rows
    ]


def create_payable(
    vendor_canonical: str,
    category_name: str | None,
    currency_original: str,
    amount_original: float,
    due_date: date,
    note: str | None,
) -> None:
    cleaned_vendor = vendor_canonical.strip()
    if not cleaned_vendor:
        return
    with connect() as conn:
        category_id = None
        if category_name and category_name.strip():
            row = conn.execute(
                "SELECT category_id FROM categories WHERE category_name = ?",
                [category_name.strip()],
            ).fetchone()
            category_id = row[0] if row else None
        if category_id is None:
            row = conn.execute(
                """
                SELECT t.category_id
                FROM transactions t
                WHERE UPPER(COALESCE(t.vendor_canonical, '')) = UPPER(?)
                  AND t.category_id IS NOT NULL
                GROUP BY t.category_id
                ORDER BY COUNT(*) DESC, MAX(t.updated_at) DESC
                LIMIT 1
                """,
                [cleaned_vendor],
            ).fetchone()
            if row:
                category_id = row[0]
        conn.execute(
            """
            INSERT INTO payables(vendor_canonical, category_id, currency_original, amount_original, due_date, note, status)
            VALUES (?, ?, ?, ?, ?, ?, 'open')
            """,
            [
                cleaned_vendor,
                category_id,
                currency_original.strip().upper(),
                abs(float(amount_original)),
                due_date,
                (note or "").strip() or None,
            ],
        )


def delete_payable(payable_id: int) -> None:
    with connect() as conn:
        conn.execute("DELETE FROM payables WHERE payable_id = ?", [payable_id])


def mark_payable_paid(payable_id: int) -> None:
    with connect() as conn:
        conn.execute(
            """
            UPDATE payables
            SET status = 'paid', paid_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE payable_id = ?
            """,
            [payable_id],
        )


def mark_overdue_payables() -> int:
    with connect() as conn:
        conn.execute(
            """
            UPDATE payables
            SET status = 'overdue', updated_at = CURRENT_TIMESTAMP
            WHERE status = 'open' AND due_date < CURRENT_DATE
            """
        )
        return conn.execute("SELECT COUNT(*) FROM payables WHERE status = 'overdue'").fetchone()[0]


def backfill_payable_categories() -> int:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT payable_id, vendor_canonical
            FROM payables
            WHERE category_id IS NULL
            """
        ).fetchall()
        updated = 0
        for payable_id, vendor in rows:
            inferred = conn.execute(
                """
                SELECT t.category_id
                FROM transactions t
                WHERE UPPER(COALESCE(t.vendor_canonical, '')) = UPPER(?)
                  AND t.category_id IS NOT NULL
                GROUP BY t.category_id
                ORDER BY COUNT(*) DESC, MAX(t.updated_at) DESC
                LIMIT 1
                """,
                [vendor],
            ).fetchone()
            if not inferred:
                continue
            conn.execute(
                """
                UPDATE payables
                SET category_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE payable_id = ?
                """,
                [inferred[0], payable_id],
            )
            updated += 1
    return updated


def reconcile_open_payables_for_transactions(transaction_ids: list[int]) -> int:
    if not transaction_ids:
        return 0
    with connect() as conn:
        return _reconcile_open_payables_for_transactions(conn, transaction_ids)


def _reconcile_open_payables_for_transactions(conn, transaction_ids: list[int]) -> int:
    matches = 0
    placeholders = ", ".join("?" for _ in transaction_ids)
    tx_rows = conn.execute(
        f"""
        SELECT id, transaction_date, COALESCE(vendor_canonical, ''), category_id, currency_original, amount_original
        FROM transactions
        WHERE id IN ({placeholders})
          AND status <> 'duplicate'
          AND amount_original < 0
          AND COALESCE(vendor_canonical, '') <> ''
        ORDER BY transaction_date, id
        """,
        transaction_ids,
    ).fetchall()
    for tx_id, tx_date, vendor, category_id, currency, amount in tx_rows:
        payable = conn.execute(
            """
            SELECT payable_id
            FROM payables
            WHERE status IN ('open', 'overdue')
              AND vendor_canonical = ?
              AND currency_original = ?
              AND ABS(amount_original - ?) <= 0.01
              AND (? IS NULL OR category_id IS NULL OR category_id = ?)
            ORDER BY due_date, payable_id
            LIMIT 1
            """,
            [vendor, currency, abs(float(amount)), category_id, category_id],
        ).fetchone()
        if not payable:
            continue
        conn.execute(
            """
            UPDATE payables
            SET status = 'paid',
                linked_transaction_id = ?,
                paid_at = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE payable_id = ?
            """,
            [tx_id, tx_date, payable[0]],
        )
        matches += 1
    return matches


def get_payables_summary() -> dict[str, Any]:
    mark_overdue_payables()
    with connect() as conn:
        totals = conn.execute(
            """
            SELECT status, currency_original, ROUND(SUM(amount_original), 2)
            FROM payables
            GROUP BY 1, 2
            ORDER BY 1, 2
            """
        ).fetchall()
        due_30 = conn.execute(
            """
            SELECT currency_original, ROUND(SUM(amount_original), 2)
            FROM payables
            WHERE status IN ('open', 'overdue')
              AND due_date <= CURRENT_DATE + INTERVAL '30 days'
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
    return {
        "totals": [{"status": row[0], "currency_original": row[1], "amount_original": row[2]} for row in totals],
        "due_30": [{"currency_original": row[0], "amount_original": row[1]} for row in due_30],
    }


def get_fixed_cost_trends() -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(
            """
            WITH monthly AS (
                SELECT
                    COALESCE(c.category_name, 'Uncategorized') AS category_name,
                    t.year_month,
                    ROUND(ABS(SUM(t.amount_pln)), 2) AS monthly_total
                FROM transactions t
                LEFT JOIN categories c ON c.category_id = t.category_id
                WHERE t.status <> 'duplicate'
                  AND t.amount_pln IS NOT NULL
                  AND t.direction = 'expense'
                  AND t.transaction_type = 'standard'
                GROUP BY 1, 2
            ),
            ranked AS (
                SELECT
                    category_name,
                    COUNT(*) AS months_seen,
                    ROUND(AVG(monthly_total), 2) AS avg_monthly_pln,
                    ROUND(MAX(monthly_total), 2) AS max_monthly_pln,
                    ROUND(MIN(monthly_total), 2) AS min_monthly_pln
                FROM monthly
                GROUP BY 1
            )
            SELECT category_name, months_seen, avg_monthly_pln, min_monthly_pln, max_monthly_pln
            FROM ranked
            ORDER BY avg_monthly_pln DESC
            """
        ).fetchall()
    output: list[dict[str, Any]] = []
    for category_name, months_seen, avg_monthly, min_monthly, max_monthly in rows:
        upper = (category_name or "").upper()
        if not any(token in upper for token in FIXED_COST_KEYWORDS):
            continue
        output.append(
            {
                "category_name": category_name,
                "months_seen": months_seen,
                "avg_monthly_pln": avg_monthly,
                "projected_next_3m_pln": round((avg_monthly or 0) * 3, 2),
                "min_monthly_pln": min_monthly,
                "max_monthly_pln": max_monthly,
            }
        )
    return output
