from __future__ import annotations

from collections.abc import Callable
import json
import os
import re
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ..db import connect


OPENAI_URL = "https://api.openai.com/v1/responses"
MODEL = "gpt-4.1-mini"
DESCRIPTION_PROMPT = (
    "Translate each bank transaction description to concise natural English. "
    "Return strict JSON object mapping each original string to its English translation. "
    "Keep merchant names, ids, masked card numbers, invoice numbers, account numbers, exchange rates, "
    "currencies, and amounts unchanged where possible."
)
OPERATION_TYPE_PROMPT = (
    "Translate each bank operation type to concise banking English. "
    "Return strict JSON object mapping each original string to its English translation. "
    "Preserve short labels and do not add explanation."
)
TRANSLATION_BATCH_SIZE = 25
TRANSLATION_TIMEOUT_SECONDS = 12
ProgressCallback = Callable[[str, int, int], None]

LOCAL_OPERATION_TYPE_MAP = {
    "fees": "Fees",
    "przelew na rachunki w innym banku": "Transfer to accounts in another bank",
    "przelew własny": "Own transfer",
    "przelew wewnątrz banku": "Transfer within the bank",
    "przelew": "Transfer",
    "płacę z alior bank": "Pay with Alior Bank",
    "pobranie opłaty okresowej": "Periodic fee charge",
    "przelew zagraniczny/walutowy zwykły": "Standard foreign/currency transfer",
    "przelew transgraniczny": "Cross-border transfer",
    "polecenie wypłaty": "Payment order",
    "transakcja kartą debetową": "Debit card transaction",
    "sp przelew elixir - zus": "ELIXIR transfer - ZUS",
    "repayment": "Repayment",
    "sp przelew na rachunek organu podatkowego": "Transfer to tax authority account",
    "prowizja kredytowa - od kwoty kredytu (kwartalna)": "Loan commission - credit amount (quarterly)",
    "immediate express elixir": "Immediate Express Elixir",
    "opłata rocznicowa za gwarancję bgk": "Anniversary fee for BGK guarantee",
    "zwrot przelewu": "Transfer return",
    "opłata za gwarancję bgk": "Fee for BGK guarantee",
    "disbursement of credit": "Disbursement of credit",
    "korekta odsetek zapadłych": "Overdue interest adjustment",
    "spłata zajęcia egzekucyjnego": "Repayment of enforcement seizure",
}

CONCATENATED_TOKEN_FIXES = [
    (r"(?<=\S)(Kurs:)", r" \1"),
    (r"(?<=\S)(Kwota)", r" \1"),
    (r"(?<=\S)(Ref dewiz\.)", r" \1"),
    (r"(?<=\S)(Koszty:)", r" \1"),
    (r"(?<=\S)(Odsetki:)", r" \1"),
    (r"(?<=\S)(Kapitał:)", r" \1"),
    (r"(?<=\S)(contactnaturalmaterials\.pl)", r" \1"),
]

DESCRIPTION_REGEX_REPLACEMENTS = [
    (r"REF\.?\s*PROWIZJA DODATK\.DO POLECENIA WYPŁATY WYCHODZ\.\s*TRYB PILNY", "REF. Additional fee for outgoing payment order urgent mode"),
    (r"REF\.?\s*PROWIZJA DODATK\.DO POLECENIA WYPŁATY WYCHODZ\.\s*TRYB EKSPRES", "REF. Additional fee for outgoing payment order express mode"),
    (r"REF\.?\s*PROWIZJA ZA POLECENIE WYPŁATY WYCHODZĄCE", "REF. Fee for outgoing payment order"),
    (r"REF\.?\s*PROWIZJA ZA POLECENIE WYPŁATY PRZYCHODZĄCE", "REF. Fee for incoming payment order"),
    (r"POLECENIE WYPŁATY SEPA WYCHODZĄCE EOG - USŁUGI BANK\.EL\.", "Outgoing SEPA payment order within EEA - electronic banking services"),
    (r"POBRANIE OPŁATY/PROWIZJI", "Fee/commission charge"),
    (r"PROWIZJA ZA PRZEWALUTOW[.,]?", "Currency conversion fee"),
    (r"PRZELEWY24 ZWROT", "Przelewy24 refund"),
    (r"ZWROT DO PŁATNOŚCI", "Refund for payment"),
    (r"ZWROT Z PODATKU VAT", "VAT tax refund"),
    (r"ZWROT POŻYCZKI", "Loan repayment"),
    (r"ZWROT PRZELEWU", "Transfer return"),
    (r"PRZELEW WŁASNY|PRZELEW WLASNY", "Own transfer"),
    (r"PRZELEW ŚRODKÓW", "Funds transfer"),
    (r"PRZELEW PROFORMA", "Proforma transfer"),
    (r"PRZELEW", "Transfer"),
    (r"WYNAGRODZENIE", "Salary"),
    (r"ZALICZKA", "Advance payment"),
    (r"WYPŁATY", "Withdrawals"),
    (r"WYPŁATA|WYPLATA", "Withdrawal"),
    (r"PRZEWALUTOWANIE ŚRODKÓW|PRZEWALUTOWANIE SRODKOW", "Funds currency conversion"),
    (r"PRZEWALUTOWANIE", "Currency conversion"),
    (r"TRANSAKCJA EFX", "eFX transaction"),
    (r"PŁATNOŚĆ KARTĄ", "Card payment"),
    (r"WYPŁATA Z BANKOMATU KARTĄ", "ATM cash withdrawal by card"),
    (r"OPŁATA ZA PROWADZENIE RACHUNKU", "Account maintenance fee"),
    (r"OPŁATA MIESIĘCZNA ZA KARTĘ", "Monthly card fee"),
    (r"OPŁATA ZA WYPŁATĘ GOTÓWKI Z BANKOMATU", "ATM cash withdrawal fee"),
    (r"ZA WYPŁATĘ GOTÓWKI Z BANKOMATU", "ATM cash withdrawal fee"),
    (r"OPŁATA ZA PRZELEW ELIXIR - ODDZIAŁ", "ELIXIR transfer fee - branch"),
    (r"OPŁATA ZA PRZELEW ELIXIR", "ELIXIR transfer fee"),
    (r"OPŁATA ZA PRZELEW24", "Przelewy24 fee"),
    (r"OP[ŁL]ATA ZA PRZELEW ZAGRANICZNY/WALUTOWY", "Fee for foreign/currency transfer"),
    (r"OP[ŁL]ATA ZA TRANSFER ZAGRANICZNY/WALUTOWY", "Fee for foreign/currency transfer"),
    (r"OPŁATA ZA PRZELEW ZAGRANICZNY/WALUTOWY", "Fee for foreign/currency transfer"),
    (r"PROWIZJA ZA REALIZACJĘ ZAJĘCIA", "Fee for execution of seizure"),
    (r"PROWIZJA KREDYTOWA - OD KWOTY KREDYTU \(KWARTALNA\)", "Loan commission - credit amount (quarterly)"),
    (r"SPŁATA ZALEGŁOŚCI", "Arrears repayment"),
    (r"SPŁATA ZAJĘCIA EGZEKUCYJNEGO", "Repayment of enforcement seizure"),
    (r"KOREKTA ODSETEK ZAPADŁYCH", "Overdue interest adjustment"),
    (r"OPŁATA ROCZNICOWA ZA GWARANCJĘ BGK", "Anniversary fee for BGK guarantee"),
    (r"OPŁATA ZA GWARANCJĘ BGK", "Fee for BGK guarantee"),
    (r"Prowadzenie rachunku pomocniczego", "Auxiliary account maintenance"),
    (r"Prowadzenie rachunku", "Account maintenance"),
    (r"Karta debetowa", "Debit card"),
    (r"Opłata dodatkowa za kartę", "Additional card fee"),
    (r"Kody autoryzacyjne SMS", "SMS authorization codes"),
    (r"Pakiet Gold w Alior ?Business|Pakiet Gold w AliorBusiness", "Gold package in Alior Business"),
    (r"w Alior Business", "in Alior Business"),
    (r"Rechnung Nr\.", "Invoice No."),
    (r"DOT\.", "regarding"),
    (r"Potracono", "Deducted"),
    (r"Koszty:", "Costs:"),
    (r"Ref dewiz\.", "FX ref."),
    (r"Kurs:", "Rate:"),
    (r"Kwota", "Amount"),
    (r"Odsetki:", "Interest:"),
    (r"Kapitał:", "Principal:"),
    (r"dot\.karty", "for card"),
]


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def clean_key(text: str) -> str:
    return normalize_space(text).casefold()


def ai_translation_fallback_enabled() -> bool:
    return os.environ.get("NMU_FIN_ENABLE_AI_TRANSLATION_FALLBACK") == "1"


def translation_enabled() -> bool:
    return (
        ai_translation_fallback_enabled()
        and bool(os.environ.get("OPENAI_API_KEY"))
        and os.environ.get("NMU_FIN_DISABLE_AI_TRANSLATION") != "1"
    )


def scoped_translation_key(scope: str, text: str) -> str:
    return f"{scope}::{text}"


def get_cached_translations(texts: list[str]) -> dict[str, str]:
    if not texts:
        return {}
    placeholders = ",".join(["?"] * len(texts))
    with connect() as conn:
        rows = conn.execute(
            f"SELECT source_text, translated_text FROM translation_cache WHERE source_text IN ({placeholders})",
            texts,
        ).fetchall()
    return {row[0]: row[1] for row in rows}


def persist_translations(scope: str, translations: dict[str, str]) -> None:
    if not translations:
        return
    with connect() as conn:
        for source_text, translated_text in translations.items():
            conn.execute(
                """
                INSERT INTO translation_cache(source_text, translated_text)
                VALUES (?, ?)
                ON CONFLICT(source_text) DO UPDATE SET translated_text = excluded.translated_text
                """,
                [scoped_translation_key(scope, source_text), translated_text],
            )


def local_translate_operation_type(text: str) -> str:
    return LOCAL_OPERATION_TYPE_MAP.get(clean_key(text), text)


def local_translate_description(text: str) -> str:
    translated = normalize_space(text)
    for pattern, replacement in CONCATENATED_TOKEN_FIXES:
        translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
    for pattern, replacement in DESCRIPTION_REGEX_REPLACEMENTS:
        translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
    translated = re.sub(r"(Principal|Interest):\s*na\s+", r"\1: as of ", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bod (\d{2}\.\d{2}\.\d{4}) do (\d{2}\.\d{2}\.\d{4})\b", r"from \1 to \2", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bszt\.\s*(\d+)", r"qty. \1", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bna prośbę banku odbiorcy\b", "at the beneficiary bank's request", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bczęść proformy\b", "part of proforma", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bprzychodzące\b", "incoming", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bwychodzące\b", "outgoing", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\btryb pilny\b", "urgent mode", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\btryb ekspres\b", "express mode", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\s+([,;:/])", r"\1", translated)
    return normalize_space(translated)


def local_translate_text(text: str, scope: str) -> str:
    if scope == "operation_type":
        return local_translate_operation_type(text)
    if scope == "description":
        return local_translate_description(text)
    return text


def request_translations(texts: list[str], prompt: str) -> dict[str, str]:
    body = json.dumps(
        {
            "model": MODEL,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": prompt}]},
                {"role": "user", "content": [{"type": "input_text", "text": json.dumps(texts, ensure_ascii=False)}]},
            ],
        }
    ).encode("utf-8")
    request = Request(
        OPENAI_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=TRANSLATION_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        text = payload["output"][0]["content"][0]["text"]
        return json.loads(text)
    except (TimeoutError, HTTPError, URLError, json.JSONDecodeError, KeyError):
        return {text: text for text in texts}


def translate_texts(
    texts: list[str],
    *,
    scope: str = "description",
    prompt: str = DESCRIPTION_PROMPT,
) -> dict[str, str]:
    unique = [text for text in dict.fromkeys(texts) if text]
    if not unique:
        return {}
    scoped_keys = {text: scoped_translation_key(scope, text) for text in unique}
    cached = get_cached_translations(list(scoped_keys.values()))

    resolved: dict[str, str] = {}
    local_updates: dict[str, str] = {}
    unresolved: list[str] = []

    for text in unique:
        local_translation = local_translate_text(text, scope)
        if local_translation != text:
            resolved[text] = local_translation
            local_updates[text] = local_translation
            continue
        cached_translation = cached.get(scoped_keys[text])
        if cached_translation and cached_translation != text:
            resolved[text] = cached_translation
            continue
        unresolved.append(text)

    persist_translations(scope, local_updates)

    if unresolved and translation_enabled():
        ai_updates: dict[str, str] = {}
        for start in range(0, len(unresolved), TRANSLATION_BATCH_SIZE):
            batch = unresolved[start : start + TRANSLATION_BATCH_SIZE]
            translated = request_translations(batch, prompt)
            for source_text in batch:
                ai_updates[source_text] = translated.get(source_text, source_text)
        persist_translations(scope, ai_updates)
        resolved.update(ai_updates)

    for text in unresolved:
        resolved.setdefault(text, cached.get(scoped_keys[text], text))

    return resolved


def translate_operation_types(texts: list[str]) -> dict[str, str]:
    return translate_texts(texts, scope="operation_type", prompt=OPERATION_TYPE_PROMPT)


def combine_display_description(
    description_raw: str,
    description_en: str,
    operation_type_raw: str | None,
    operation_type_en: str | None,
) -> str:
    if not operation_type_raw and not operation_type_en:
        return description_en
    if operation_type_en and description_en and operation_type_en.lower() in description_en.lower():
        return description_en
    if operation_type_en and description_en and operation_type_en.lower().startswith("fee") and description_en.lower().startswith("fee"):
        return description_en
    parts = [part for part in [operation_type_en or operation_type_raw, description_en] if part]
    return " | ".join(parts) if parts else description_raw


def notify_progress(progress: ProgressCallback | None, stage: str, total_rows: int, processed_rows: int) -> None:
    if progress is not None:
        progress(stage, total_rows, processed_rows)


def backfill_transaction_translations(
    batch_size: int = 25,
    import_batch_id: str | None = None,
    progress: ProgressCallback | None = None,
) -> int:
    filters = ["description_translation_pending = TRUE"]
    params: list[str] = []
    if import_batch_id:
        filters.append("import_batch_id = ?")
        params.append(import_batch_id)
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT id, description_raw, operation_type_raw
            FROM transactions
            WHERE {' AND '.join(filters)}
            """,
            params,
        ).fetchall()
    total_rows = len(rows)
    notify_progress(progress, "Translating imported descriptions", total_rows, 0)
    operation_type_translations = translate_operation_types([row[2] for row in rows if row[2]])
    total = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        mapping = translate_texts([row[1] for row in batch], scope="description")
        with connect() as conn:
            for transaction_id, description_raw, operation_type_raw in batch:
                conn.execute(
                    """
                    UPDATE transactions
                    SET description_en = ?, operation_type_en = ?, description_translation_pending = FALSE
                    WHERE id = ?
                    """,
                    [
                        combine_display_description(
                            description_raw,
                            mapping.get(description_raw, description_raw),
                            operation_type_raw,
                            operation_type_translations.get(operation_type_raw, operation_type_raw),
                        ),
                        operation_type_translations.get(operation_type_raw, operation_type_raw),
                        transaction_id,
                    ],
                )
        total += len(batch)
        notify_progress(progress, "Translating imported descriptions", total_rows, total)
    return total


def rebuild_all_transaction_translations(
    batch_size: int = 200,
    progress: ProgressCallback | None = None,
) -> int:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, description_raw, operation_type_raw
            FROM transactions
            ORDER BY id
            """
        ).fetchall()
    total_rows = len(rows)
    notify_progress(progress, "Rebuilding translations", total_rows, 0)
    operation_type_translations = translate_operation_types([row[2] for row in rows if row[2]])
    total = 0
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        description_translations = translate_texts([row[1] for row in batch], scope="description")
        with connect() as conn:
            for transaction_id, description_raw, operation_type_raw in batch:
                conn.execute(
                    """
                    UPDATE transactions
                    SET description_en = ?, operation_type_en = ?, description_translation_pending = FALSE
                    WHERE id = ?
                    """,
                    [
                        combine_display_description(
                            description_raw,
                            description_translations.get(description_raw, description_raw),
                            operation_type_raw,
                            operation_type_translations.get(operation_type_raw, operation_type_raw),
                        ),
                        operation_type_translations.get(operation_type_raw, operation_type_raw),
                        transaction_id,
                    ],
                )
        total += len(batch)
        notify_progress(progress, "Rebuilding translations", total_rows, total)
    return total
