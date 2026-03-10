from __future__ import annotations

from dataclasses import dataclass
import re

from ..db import connect
from ..normalization import clean_text, infer_vendor

GENERIC_VENDOR_TOKENS = {
    "LTD",
    "LIMITED",
    "LLC",
    "INC",
    "GMBH",
    "BV",
    "SP",
    "ZO",
    "OO",
    "SPOLKA",
    "KELI",
    "KEL",
    "FEE",
    "FEES",
    "FX",
    "TRANSFER",
    "PAYMENT",
    "CHARGE",
}

INTERNAL_EXCHANGE_VENDORS = {
    "INTERNAL TRANSFER",
    "INTERNAL FX TRANSFER",
}


@dataclass(slots=True)
class RuleMatch:
    vendor_canonical: str | None
    category_name: str | None


def load_rule_sets(conn) -> tuple[list[tuple[str, str]], list[tuple[str, str, str]], dict[str, tuple[int, str | None]]]:
    vendor_rules = conn.execute(
        "SELECT match_pattern, canonical_vendor FROM vendor_rules ORDER BY length(match_pattern) DESC"
    ).fetchall()
    category_rules = conn.execute(
        "SELECT match_pattern, match_type, assigned_category FROM category_rules ORDER BY length(match_pattern) DESC"
    ).fetchall()
    categories = {
        row[1]: (row[0], row[2])
        for row in conn.execute("SELECT category_id, category_name, parent_category FROM categories").fetchall()
    }
    return vendor_rules, category_rules, categories


def is_category_compatible(
    category_parent: str | None,
    direction: str,
    transaction_type: str,
    vendor_canonical: str | None = None,
) -> bool:
    if category_parent == "Exchange":
        if transaction_type == "exchange":
            return True
        return clean_text(vendor_canonical) in INTERNAL_EXCHANGE_VENDORS
    if category_parent == "Income":
        return transaction_type != "exchange" and direction == "income"
    if category_parent == "Expenses":
        return transaction_type != "exchange" and direction == "expense"
    return True


def rule_matches(match_pattern: str, match_type: str, description_clean: str, vendor_guess: str | None) -> bool:
    vendor_value = clean_text(vendor_guess)
    description_value = clean_text(description_clean)
    pattern = clean_text(match_pattern)
    if not pattern:
        return False
    if match_type == "vendor_exact":
        return vendor_value == pattern
    if match_type == "vendor_contains":
        return bool(vendor_value) and pattern in vendor_value
    if match_type == "description":
        return pattern in description_value
    return pattern in description_value or (bool(vendor_value) and pattern in vendor_value)


def apply_rules_with_sets(
    description_clean: str,
    vendor_guess: str | None,
    vendor_rules: list[tuple[str, str]],
    category_rules: list[tuple[str, str, str]],
) -> RuleMatch:
    resolved_vendor = clean_text(vendor_guess) or None
    for match_pattern, canonical_vendor in vendor_rules:
        if rule_matches(match_pattern, "vendor_or_description", description_clean, resolved_vendor):
            resolved_vendor = canonical_vendor
            break

    category_name = None
    for match_pattern, match_type, assigned_category in category_rules:
        if rule_matches(match_pattern, match_type, description_clean, resolved_vendor):
            category_name = assigned_category
            break
    return RuleMatch(vendor_canonical=resolved_vendor, category_name=category_name)


def apply_rules(description_clean: str, vendor_guess: str | None) -> RuleMatch:
    with connect() as conn:
        vendor_rules, category_rules, _ = load_rule_sets(conn)
    return apply_rules_with_sets(description_clean, vendor_guess, vendor_rules, category_rules)


def ensure_vendor(canonical_vendor: str) -> None:
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO vendors(vendor_name, canonical_vendor)
            SELECT ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM vendors WHERE canonical_vendor = ?
            )
            """,
            [canonical_vendor, canonical_vendor, canonical_vendor],
        )


def get_category_id(category_name: str | None) -> int | None:
    if not category_name:
        return None
    with connect() as conn:
        row = conn.execute(
            "SELECT category_id FROM categories WHERE category_name = ?",
            [category_name],
        ).fetchone()
    return row[0] if row else None


def save_vendor_rule(match_pattern: str, canonical_vendor: str) -> None:
    normalized_pattern = clean_text(match_pattern)
    normalized_vendor = clean_text(canonical_vendor)
    if not normalized_pattern or not normalized_vendor:
        return
    ensure_vendor(normalized_vendor)
    with connect() as conn:
        conn.execute(
            """
            INSERT INTO vendor_rules(match_pattern, canonical_vendor)
            VALUES (?, ?)
            ON CONFLICT(match_pattern) DO UPDATE SET canonical_vendor = excluded.canonical_vendor
            """,
            [normalized_pattern, normalized_vendor],
        )


def save_category_rule(match_pattern: str, match_type: str, category_name: str) -> None:
    normalized_pattern = clean_text(match_pattern)
    if not normalized_pattern or not category_name.strip():
        return
    with connect() as conn:
        existing = conn.execute(
            """
            SELECT rule_id
            FROM category_rules
            WHERE match_pattern = ? AND match_type = ?
            """,
            [normalized_pattern, match_type],
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE category_rules
                SET assigned_category = ?
                WHERE rule_id = ?
                """,
                [category_name, existing[0]],
            )
        else:
            conn.execute(
                """
                INSERT INTO category_rules(match_pattern, match_type, assigned_category)
                VALUES (?, ?, ?)
                """,
                [normalized_pattern, match_type, category_name],
            )


def derive_vendor_rule_pattern(
    vendor_canonical: str,
    vendor_raw: str | None,
    description_raw: str | None,
) -> str:
    normalized_vendor = clean_text(vendor_canonical)
    source_text = " ".join(part for part in [clean_text(vendor_raw), clean_text(description_raw)] if part)
    tokens = re.findall(r"[A-Z0-9]{4,}", normalized_vendor)
    for token in tokens:
        if token in GENERIC_VENDOR_TOKENS:
            continue
        if token in source_text:
            return token
    for token in tokens:
        if token not in GENERIC_VENDOR_TOKENS:
            return token
    if source_text:
        # Fallback to the full observed text so recurring bank message lines can still match deterministically.
        return source_text
    return normalized_vendor


def create_repeat_rule_for_transaction(
    transaction_id: int,
    vendor_canonical: str,
    category_name: str,
) -> None:
    normalized_vendor = clean_text(vendor_canonical)
    if not normalized_vendor or not category_name.strip():
        return
    with connect() as conn:
        row = conn.execute(
            """
            SELECT vendor_raw, description_raw
            FROM transactions
            WHERE id = ?
            """,
            [transaction_id],
        ).fetchone()
    vendor_raw = row[0] if row else None
    description_raw = row[1] if row else None
    save_vendor_rule(derive_vendor_rule_pattern(normalized_vendor, vendor_raw, description_raw), normalized_vendor)
    save_category_rule(normalized_vendor, "vendor_exact", category_name)


def reapply_rules_to_transactions() -> int:
    with connect() as conn:
        vendor_rules, category_rules, categories = load_rule_sets(conn)
        rows = conn.execute(
            """
            SELECT
                id,
                description_clean,
                vendor_canonical,
                vendor_raw,
                direction,
                transaction_type,
                amount_usd,
                amount_pln,
                amount_eur,
                category_id
            FROM transactions
            """
        ).fetchall()
        updated = 0
        for row in rows:
            existing_vendor = clean_text(row[2])
            vendor_guess = existing_vendor or infer_vendor(row[1], row[3])
            match = apply_rules_with_sets(row[1], vendor_guess, vendor_rules, category_rules)
            resolved_vendor = match.vendor_canonical or existing_vendor
            category = categories.get(match.category_name) if match.category_name else None
            category_id = row[9]
            transaction_type = row[5]
            if category and is_category_compatible(category[1], row[4], transaction_type, resolved_vendor):
                category_id = category[0]
                if category[1] == "Exchange":
                    transaction_type = "exchange"
            status = "ready" if resolved_vendor and category_id and None not in row[6:9] else "needs_review"
            conn.execute(
                """
                UPDATE transactions
                SET vendor_canonical = ?, category_id = ?, transaction_type = ?, status = ?, needs_review = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [resolved_vendor, category_id, transaction_type, status, status == "needs_review", row[0]],
            )
            updated += 1
    return updated
