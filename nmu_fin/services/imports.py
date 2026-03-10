from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

from ..account_labels import build_account_label
from ..db import connect
from ..normalization import normalize_row
from ..parsers import ParsedFile, parse_known_csv
from .rules import apply_rules, get_category_id, is_category_compatible
from .fx import convert_amount_with_rates, get_rate_map_for_dates
from .payables import _reconcile_open_payables_for_transactions
from .translation import translate_operation_types, translate_texts

ProgressCallback = Callable[[str, int, int], None]
UploadSource = tuple[str, bytes]


@dataclass(slots=True)
class FileImportPreview:
    filename: str
    parser_name: str
    account_id: str
    account_name: str
    row_count: int
    duplicate_count: int
    ready_count: int
    needs_review_count: int
    fx_pending_count: int
    rows: list[dict]
    parsed_file: ParsedFile


@dataclass(slots=True)
class ImportPreview:
    preview_id: str
    filename: str
    parser_name: str
    account_id: str
    account_name: str
    row_count: int
    duplicate_count: int
    ready_count: int
    needs_review_count: int
    fx_pending_count: int
    rows: list[dict]
    parsed_file: ParsedFile | None
    file_previews: list[FileImportPreview]
    filenames: list[str]
    file_count: int


def build_display_description(
    parsed_file: ParsedFile,
    description_raw: str,
    description_display: str,
    operation_type_raw: str | None,
    operation_type_en: str | None,
) -> str:
    if parsed_file.parser_name != "alior_business_v1":
        return description_display
    if operation_type_en and description_display and operation_type_en.lower() in description_display.lower():
        return description_display
    parts = [part for part in [operation_type_en, description_display] if part]
    if parts:
        return " | ".join(parts)
    fallback = [part for part in [operation_type_raw, description_raw] if part]
    return " | ".join(fallback) if fallback else description_raw


def preview_description_targets(parsed: ParsedFile) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for row in parsed.rows:
        if not row.description or row.description in seen:
            continue
        seen.add(row.description)
        unique.append(row.description)
    return unique


def notify_progress(progress: ProgressCallback | None, stage: str, total_rows: int, processed_rows: int) -> None:
    if progress is not None:
        progress(stage, total_rows, processed_rows)


def build_transaction_account_name(parsed_file: ParsedFile, currency_original: str) -> str:
    return build_account_label(parsed_file.bank_name, currency_original, parsed_file.account_number)


def coerce_upload_sources(filename_or_files: str | list[UploadSource], content: bytes | None) -> list[UploadSource]:
    if isinstance(filename_or_files, str):
        if content is None:
            raise ValueError("CSV content is required.")
        return [(filename_or_files, content)]
    files = [(filename, payload) for filename, payload in filename_or_files if filename]
    if not files:
        raise ValueError("At least one CSV file is required.")
    return files


def load_preview_context(parsed_files: list[ParsedFile]) -> tuple[set[str], dict[date, dict[tuple[str, str], float]]]:
    unique_dates = sorted({row.transaction_date.date() for parsed in parsed_files for row in parsed.rows})
    with connect() as conn:
        existing_hashes = {
            row[0]
            for row in conn.execute("SELECT dedupe_hash FROM transactions").fetchall()
        }
        rate_rows = get_rate_map_for_dates(conn, unique_dates)
    rates_by_date: dict[date, dict[tuple[str, str], float]] = {}
    for rate_date, from_currency, to_currency, rate in rate_rows:
        rates_by_date.setdefault(rate_date, {})[(from_currency, to_currency)] = rate
    return existing_hashes, rates_by_date


def build_file_preview(
    filename: str,
    parsed: ParsedFile,
    *,
    existing_hashes: set[str],
    rates_by_date: dict[date, dict[tuple[str, str], float]],
    translate_preview_text: bool,
    progress: ProgressCallback | None,
    total_rows: int,
    processed_offset: int,
) -> FileImportPreview:
    description_translations: dict[str, str] = {}
    operation_type_translations: dict[str | None, str | None] = {}
    if translate_preview_text:
        notify_progress(progress, f"Translating preview text: {filename}", total_rows, processed_offset)
        description_translations = translate_texts(preview_description_targets(parsed), scope="description")
        operation_type_translations = translate_operation_types(
            [row.operation_type_raw for row in parsed.rows if row.operation_type_raw]
        )

    rows: list[dict] = []
    duplicate_count = 0
    ready_count = 0
    needs_review_count = 0
    fx_pending_count = 0
    notify_progress(progress, f"Preparing preview rows: {filename}", total_rows, processed_offset)
    for file_index, parsed_row in enumerate(parsed.rows, start=1):
        normalized = normalize_row(
            transaction_date=parsed_row.transaction_date.date(),
            posting_date=parsed_row.posting_date.date(),
            account_id=parsed.account_id,
            description=parsed_row.description,
            vendor_raw=parsed_row.vendor_raw,
            amount_original=parsed_row.amount_original,
            currency_original=parsed_row.currency_original,
        )
        converted = convert_amount_with_rates(
            rate_date=parsed_row.transaction_date.date(),
            currency=parsed_row.currency_original,
            amount=float(parsed_row.amount_original),
            rates=rates_by_date.get(parsed_row.transaction_date.date(), {}),
        )
        rule_match = apply_rules(normalized.description_clean, normalized.vendor_canonical)
        vendor_canonical = rule_match.vendor_canonical or normalized.vendor_canonical
        category_id = get_category_id(rule_match.category_name)
        if category_id is not None:
            category_parent = {
                "Incomes": "Income",
                "Income": "Income",
                "Expenses": "Expenses",
                "Exchange": "Exchange",
            }.get(rule_match.category_name)
            if not is_category_compatible(category_parent, normalized.direction, normalized.transaction_type):
                category_id = None
        is_duplicate = normalized.dedupe_hash in existing_hashes
        if not is_duplicate:
            existing_hashes.add(normalized.dedupe_hash)
        status = (
            "duplicate"
            if is_duplicate
            else "ready"
            if vendor_canonical and category_id is not None and None not in (converted.amount_usd, converted.amount_pln, converted.amount_eur)
            else "needs_review"
        )
        duplicate_count += int(is_duplicate)
        ready_count += int(status == "ready")
        needs_review_count += int(status == "needs_review")
        fx_pending_count += int(converted.status == "fx_pending")
        rows.append(
            {
                "source_filename": filename,
                "bank_name": parsed.bank_name,
                "row_number": parsed_row.row_number,
                "transaction_date": parsed_row.transaction_date.date().isoformat(),
                "posting_date": parsed_row.posting_date.date().isoformat(),
                "description_raw": parsed_row.description,
                "description_en": build_display_description(
                    parsed,
                    parsed_row.description,
                    description_translations.get(parsed_row.description, parsed_row.description),
                    parsed_row.operation_type_raw,
                    operation_type_translations.get(parsed_row.operation_type_raw, parsed_row.operation_type_raw),
                ),
                "description_clean": normalized.description_clean,
                "operation_type_raw": parsed_row.operation_type_raw,
                "operation_type_en": operation_type_translations.get(
                    parsed_row.operation_type_raw,
                    parsed_row.operation_type_raw,
                ),
                "description_translation_pending": False,
                "vendor_raw": parsed_row.vendor_raw,
                "vendor_canonical": vendor_canonical,
                "amount_original": float(parsed_row.amount_original),
                "currency_original": parsed_row.currency_original,
                "balance": float(parsed_row.balance) if parsed_row.balance is not None else None,
                "amount_usd": converted.amount_usd,
                "amount_pln": converted.amount_pln,
                "amount_eur": converted.amount_eur,
                "transaction_id": parsed_row.transaction_id,
                "dedupe_hash": normalized.dedupe_hash,
                "direction": normalized.direction,
                "transaction_type": normalized.transaction_type,
                "category_id": category_id,
                "status": status,
                "raw_payload": parsed_row.raw_payload,
            }
        )
        overall_processed = processed_offset + file_index
        if overall_processed == total_rows or overall_processed % 25 == 0:
            notify_progress(progress, f"Preparing preview rows: {filename}", total_rows, overall_processed)

    return FileImportPreview(
        filename=filename,
        parser_name=parsed.parser_name,
        account_id=parsed.account_id,
        account_name=parsed.account_name,
        row_count=len(rows),
        duplicate_count=duplicate_count,
        ready_count=ready_count,
        needs_review_count=needs_review_count,
        fx_pending_count=fx_pending_count,
        rows=rows,
        parsed_file=parsed,
    )


def merge_preview_rows(file_previews: list[FileImportPreview]) -> list[dict]:
    combined_rows = [row for file_preview in file_previews for row in file_preview.rows]
    return sorted(
        combined_rows,
        key=lambda row: (row["transaction_date"], row["source_filename"], row["row_number"]),
        reverse=True,
    )


def build_preview(
    filename_or_files: str | list[UploadSource],
    content: bytes | None = None,
    progress: ProgressCallback | None = None,
    translate_preview_text: bool = False,
) -> ImportPreview:
    upload_sources = coerce_upload_sources(filename_or_files, content)
    parsed_files = [parse_known_csv(filename, payload) for filename, payload in upload_sources]
    total_rows = sum(len(parsed.rows) for parsed in parsed_files)
    existing_hashes, rates_by_date = load_preview_context(parsed_files)

    file_previews: list[FileImportPreview] = []
    processed_offset = 0
    for (filename, _), parsed in zip(upload_sources, parsed_files, strict=True):
        file_preview = build_file_preview(
            filename,
            parsed,
            existing_hashes=existing_hashes,
            rates_by_date=rates_by_date,
            translate_preview_text=translate_preview_text,
            progress=progress,
            total_rows=total_rows,
            processed_offset=processed_offset,
        )
        file_previews.append(file_preview)
        processed_offset += file_preview.row_count

    row_count = sum(file_preview.row_count for file_preview in file_previews)
    duplicate_count = sum(file_preview.duplicate_count for file_preview in file_previews)
    ready_count = sum(file_preview.ready_count for file_preview in file_previews)
    needs_review_count = sum(file_preview.needs_review_count for file_preview in file_previews)
    fx_pending_count = sum(file_preview.fx_pending_count for file_preview in file_previews)
    filenames = [file_preview.filename for file_preview in file_previews]
    single_file_preview = file_previews[0] if len(file_previews) == 1 else None

    return ImportPreview(
        preview_id=str(uuid.uuid4()),
        filename=single_file_preview.filename if single_file_preview else f"{len(file_previews)} files",
        parser_name=single_file_preview.parser_name if single_file_preview else "multiple",
        account_id=single_file_preview.account_id if single_file_preview else "multiple",
        account_name=single_file_preview.account_name if single_file_preview else "",
        row_count=row_count,
        duplicate_count=duplicate_count,
        ready_count=ready_count,
        needs_review_count=needs_review_count,
        fx_pending_count=fx_pending_count,
        rows=merge_preview_rows(file_previews),
        parsed_file=single_file_preview.parsed_file if single_file_preview else None,
        file_previews=file_previews,
        filenames=filenames,
        file_count=len(file_previews),
    )


def commit_preview(preview: ImportPreview, progress: ProgressCallback | None = None) -> str:
    batch_ids: list[str] = []
    inserted_transaction_ids: list[int] = []
    total_rows = preview.row_count
    with connect() as conn:
        existing_vendors = {
            row[0]
            for row in conn.execute("SELECT canonical_vendor FROM vendors").fetchall()
        }
        processed_rows = 0
        notify_progress(progress, "Importing rows", total_rows, 0)
        for file_number, file_preview in enumerate(preview.file_previews, start=1):
            batch_id = str(uuid.uuid4())
            batch_ids.append(batch_id)
            conn.execute(
                """
                INSERT INTO accounts(account_id, bank_name, account_name, account_number, currency)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    bank_name = excluded.bank_name,
                    account_name = excluded.account_name,
                    account_number = excluded.account_number,
                    currency = excluded.currency
                """,
                [
                    file_preview.account_id,
                    file_preview.parsed_file.bank_name,
                    file_preview.account_name,
                    file_preview.parsed_file.account_number,
                    file_preview.parsed_file.currency,
                ],
            )
            conn.execute(
                """
                INSERT INTO import_batches(import_batch_id, source_filename, parser_name, account_id, row_count, duplicate_count, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    batch_id,
                    file_preview.filename,
                    file_preview.parser_name,
                    file_preview.account_id,
                    file_preview.row_count,
                    file_preview.duplicate_count,
                    "imported",
                ],
            )
            stage = (
                "Importing rows"
                if preview.file_count == 1
                else f"Importing {file_number}/{preview.file_count}: {file_preview.filename}"
            )
            for row in file_preview.rows:
                conn.execute(
                    "INSERT INTO raw_import_rows(import_batch_id, row_number, raw_payload) VALUES (?, ?, ?)",
                    [batch_id, row["row_number"], json.dumps(row["raw_payload"])],
                )
                if row["vendor_canonical"] and row["vendor_canonical"] not in existing_vendors:
                    conn.execute(
                        "INSERT INTO vendors(vendor_name, canonical_vendor) VALUES (?, ?)",
                        [row["vendor_canonical"], row["vendor_canonical"]],
                    )
                    existing_vendors.add(row["vendor_canonical"])
                transaction_date = datetime.fromisoformat(row["transaction_date"]).date()
                posting_date = datetime.fromisoformat(row["posting_date"]).date()
                inserted = conn.execute(
                    """
                    INSERT INTO transactions(
                        transaction_id, transaction_date, posting_date, account_id, bank_name, account_name,
                        description_raw, description_en, description_clean, vendor_raw, vendor_canonical,
                        amount_original, currency_original, balance, amount_usd, amount_pln, amount_eur,
                        operation_type_raw, operation_type_en,
                        description_translation_pending,
                        category_id, direction, year, month, year_month, import_batch_id,
                        dedupe_hash, status, transaction_type, needs_review, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    RETURNING id
                    """,
                    [
                        row["transaction_id"],
                        transaction_date,
                        posting_date,
                        file_preview.account_id,
                        file_preview.parsed_file.bank_name,
                        build_transaction_account_name(file_preview.parsed_file, row["currency_original"]),
                        row["description_raw"],
                        row["description_en"],
                        row["description_clean"],
                        row["vendor_raw"],
                        row["vendor_canonical"],
                        row["amount_original"],
                        row["currency_original"],
                        row["balance"],
                        row["amount_usd"],
                        row["amount_pln"],
                        row["amount_eur"],
                        row["operation_type_raw"],
                        row["operation_type_en"],
                        row["description_translation_pending"],
                        row["category_id"],
                        row["direction"],
                        transaction_date.year,
                        transaction_date.month,
                        transaction_date.strftime("%Y-%m"),
                        batch_id,
                        row["dedupe_hash"],
                        row["status"],
                        row["transaction_type"],
                        row["status"] == "needs_review",
                        datetime.now(),
                    ],
                ).fetchone()
                if inserted:
                    inserted_transaction_ids.append(inserted[0])
                processed_rows += 1
                if processed_rows == total_rows or processed_rows % 25 == 0:
                    notify_progress(progress, stage, total_rows, processed_rows)
        if inserted_transaction_ids:
            notify_progress(progress, "Reconciling payables", total_rows, total_rows)
            _reconcile_open_payables_for_transactions(conn, inserted_transaction_ids)
    return ",".join(batch_ids)


def reprice_pending_transactions(rate_date: date) -> int:
    with connect() as conn:
        rate_rows = get_rate_map_for_dates(conn, [rate_date])
        rates = {
            (from_currency, to_currency): rate
            for _, from_currency, to_currency, rate in rate_rows
        }
        rows = conn.execute(
            """
            SELECT id, transaction_date, currency_original, amount_original, vendor_canonical, category_id, transaction_type
            FROM transactions
            WHERE transaction_date = ? AND status = 'needs_review'
            """,
            [rate_date],
        ).fetchall()
        updated = 0
        for row in rows:
            converted = convert_amount_with_rates(row[1], row[2], row[3], rates)
            if converted.status == "fx_pending":
                continue
            status = "needs_review" if not row[4] or not row[5] else "ready"
            conn.execute(
                """
                UPDATE transactions
                SET amount_usd = ?, amount_pln = ?, amount_eur = ?, status = ?, needs_review = ?
                WHERE id = ?
                """,
                [converted.amount_usd, converted.amount_pln, converted.amount_eur, status, status == "needs_review", row[0]],
            )
            updated += 1
    return updated
