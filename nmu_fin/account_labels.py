from __future__ import annotations


def compact_account_number(account_number: str | None) -> str:
    digits = "".join(character for character in account_number or "" if character.isdigit())
    return digits[-4:]


def build_account_label(
    bank_name: str | None,
    currency: str | None,
    account_number: str | None = None,
) -> str:
    bank = (bank_name or "").strip()
    currency_code = (currency or "").strip()
    if currency_code == "MULTI":
        return f"{bank} multi-currency".strip() or "Multi-currency account"
    last_four = compact_account_number(account_number)
    if bank and currency_code and last_four:
        return f"{bank} {currency_code} ({last_four})"
    if bank and currency_code:
        return f"{bank} {currency_code}"
    if bank and last_four:
        return f"{bank} ({last_four})"
    if currency_code:
        return currency_code
    return bank or "Unknown account"
