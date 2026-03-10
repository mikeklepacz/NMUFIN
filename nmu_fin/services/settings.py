from __future__ import annotations

from duckdb import ConstraintException

from ..db import connect


def get_category_settings() -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT
                c.category_id,
                c.category_name,
                c.parent_category,
                COUNT(DISTINCT t.id) AS transaction_count,
                COUNT(DISTINCT r.rule_id) AS rule_count
            FROM categories c
            LEFT JOIN transactions t ON t.category_id = c.category_id
            LEFT JOIN category_rules r ON r.assigned_category = c.category_name
            GROUP BY 1, 2, 3
            ORDER BY c.parent_category NULLS FIRST, c.category_name
            """
        ).fetchall()
    return [
        {
            "category_id": row[0],
            "category_name": row[1],
            "parent_category": row[2] or "",
            "transaction_count": row[3],
            "rule_count": row[4],
            "locked": row[1] in {"Income", "Expenses", "Exchange"},
        }
        for row in rows
    ]


def get_saved_rules() -> dict[str, list[dict]]:
    with connect() as conn:
        vendor_rows = conn.execute(
            """
            SELECT rule_id, match_pattern, canonical_vendor, created_at
            FROM vendor_rules
            ORDER BY created_at DESC, rule_id DESC
            """
        ).fetchall()
        category_rows = conn.execute(
            """
            SELECT rule_id, match_pattern, match_type, assigned_category, created_at
            FROM category_rules
            ORDER BY created_at DESC, rule_id DESC
            """
        ).fetchall()
    return {
        "vendor_rules": [
            {
                "rule_id": row[0],
                "match_pattern": row[1],
                "canonical_vendor": row[2],
                "created_at": row[3],
            }
            for row in vendor_rows
        ],
        "category_rules": [
            {
                "rule_id": row[0],
                "match_pattern": row[1],
                "match_type": row[2],
                "assigned_category": row[3],
                "created_at": row[4],
            }
            for row in category_rows
        ],
    }


def clear_saved_rules() -> None:
    with connect() as conn:
        conn.execute("DELETE FROM vendor_rules")
        conn.execute("DELETE FROM category_rules")


def create_category(category_name: str, parent_category: str | None) -> str | None:
    normalized_name = category_name.strip()
    if not normalized_name:
        return "Category name is required."
    with connect() as conn:
        try:
            conn.execute(
                """
                INSERT INTO categories(category_name, parent_category)
                VALUES (?, ?)
                """,
                [normalized_name, parent_category or None],
            )
        except ConstraintException:
            return f'Category "{normalized_name}" already exists.'
    return None


def update_category(category_id: int, category_name: str, parent_category: str | None) -> str | None:
    normalized_name = category_name.strip()
    if not normalized_name:
        return "Category name is required."
    with connect() as conn:
        current = conn.execute(
            "SELECT category_name FROM categories WHERE category_id = ?",
            [category_id],
        ).fetchone()
        if not current:
            return "Category was not found."
        old_name = current[0]
        try:
            conn.execute(
                """
                UPDATE categories
                SET category_name = ?, parent_category = ?
                WHERE category_id = ?
                """,
                [normalized_name, parent_category or None, category_id],
            )
        except ConstraintException:
            return f'Category "{normalized_name}" already exists.'
        conn.execute(
            """
            UPDATE category_rules
            SET assigned_category = ?
            WHERE assigned_category = ?
            """,
            [normalized_name, old_name],
        )
    return None


def delete_category(category_id: int) -> None:
    with connect() as conn:
        category = conn.execute(
            "SELECT category_name FROM categories WHERE category_id = ?",
            [category_id],
        ).fetchone()
        if not category or category[0] in {"Income", "Expenses", "Exchange", "Uncategorized"}:
            return
        uncategorized = conn.execute(
            "SELECT category_id FROM categories WHERE category_name = 'Uncategorized'"
        ).fetchone()
        replacement_id = uncategorized[0] if uncategorized else None
        conn.execute(
            "UPDATE transactions SET category_id = ? WHERE category_id = ?",
            [replacement_id, category_id],
        )
        conn.execute(
            "DELETE FROM category_rules WHERE assigned_category = ?",
            [category[0]],
        )
        conn.execute(
            "DELETE FROM categories WHERE category_id = ?",
            [category_id],
        )
