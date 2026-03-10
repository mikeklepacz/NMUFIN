from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

EXCHANGE_MARKERS = ("TRANSAKCJA EFX", "PRZEWALUTOWAN", "KONWERTACJA")


@dataclass(slots=True)
class NormalizedValues:
    description_clean: str
    vendor_canonical: str | None
    transaction_type: str
    direction: str
    dedupe_hash: str


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    upper = value.upper().replace("_", " ")
    return re.sub(r"\s+", " ", upper).strip()


def is_exchange_description(description_clean: str) -> bool:
    return any(marker in description_clean for marker in EXCHANGE_MARKERS)


def infer_vendor(description: str, vendor_raw: str | None) -> str | None:
    for source in [vendor_raw, description]:
        cleaned = clean_text(source)
        if not cleaned:
            continue
        if "MONTHLY CARD FEE" in cleaned:
            return "BANK FEES"
        if "KARTA" in cleaned and ("OPLATA" in cleaned or "OPŁATA" in cleaned):
            return "BANK FEES"
        if "PROWIZJA ZA PRZEWALUTOW" in cleaned and "DOT.KARTY" in cleaned:
            return "BANK FEES"
        if "FEE/COMMISSION CHARGE" in cleaned and "FOREIGN/CURRENCY TRANSFER" in cleaned:
            return "FX FEE"
        if "PROWIZJA ZA POLECENIE WYPLATY" in cleaned:
            return "FX FEE"
        if "DISBURSEMENT OF CREDIT" in cleaned or "KREDYT" in cleaned or "POZYCZK" in cleaned:
            return "BANK LOAN"
        if is_exchange_description(description):
            return "INTERNAL FX TRANSFER"
        if "NATURAL MATERIALS UNLIMITED" in cleaned:
            return "INTERNAL TRANSFER"
        if "PAYPRO" in cleaned or "PRZELEWY24" in cleaned:
            return "PAYPRO"
        if "BERIAN" in cleaned:
            return "BERIAN"
        if "ORLEN" in cleaned:
            return "ORLEN"
        if "ALLEGRO" in cleaned:
            return "ALLEGRO"
        if "AMAZON" in cleaned:
            return "AMAZON"
        if "ABELLIO" in cleaned:
            return "ABELLIO"
        if "STRIPE" in cleaned:
            return "STRIPE"
        if "MRPEASY" in cleaned:
            return "MRPEASY"
        if "SOPHIE STR" in cleaned:
            return "SOPHIE STRUBING"
        if "JAMS DRUKARNIA" in cleaned:
            return "JAMS DRUKARNIA"
        if "CENTRUM US" in cleaned:
            return "BANK FEES"
    return clean_text(vendor_raw) or None


def infer_transaction_type(description_clean: str) -> str:
    if is_exchange_description(description_clean):
        return "exchange"
    markers = ["PRZELEW WLASNY", "TRANSAKCJA EFX", "REFUND", "ZWROT", "TRANSFER"]
    if any(marker in description_clean for marker in markers):
        return "transfer"
    return "standard"


def infer_direction(amount_original: Decimal) -> str:
    return "income" if amount_original > 0 else "expense"


def build_dedupe_hash(
    transaction_date: date,
    posting_date: date,
    account_id: str,
    amount_original: Decimal,
    currency_original: str,
    description_clean: str,
) -> str:
    payload = "|".join(
        [
            transaction_date.isoformat(),
            posting_date.isoformat(),
            account_id,
            f"{amount_original:.2f}",
            currency_original,
            description_clean,
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_row(
    transaction_date: date,
    posting_date: date,
    account_id: str,
    description: str,
    vendor_raw: str | None,
    amount_original: Decimal,
    currency_original: str,
) -> NormalizedValues:
    description_clean = clean_text(description)
    vendor_canonical = infer_vendor(description_clean, vendor_raw)
    transaction_type = infer_transaction_type(description_clean)
    direction = infer_direction(amount_original)
    dedupe_hash = build_dedupe_hash(
        transaction_date=transaction_date,
        posting_date=posting_date,
        account_id=account_id,
        amount_original=amount_original,
        currency_original=currency_original,
        description_clean=description_clean,
    )
    return NormalizedValues(
        description_clean=description_clean,
        vendor_canonical=vendor_canonical,
        transaction_type=transaction_type,
        direction=direction,
        dedupe_hash=dedupe_hash,
    )
