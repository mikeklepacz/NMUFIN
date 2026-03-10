from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .account_labels import build_account_label


@dataclass(slots=True)
class ParsedRow:
    row_number: int
    posting_date: datetime
    transaction_date: datetime
    description: str
    operation_type_raw: str | None
    vendor_raw: str | None
    counterparty_account: str | None
    amount_original: Decimal
    currency_original: str
    balance: Decimal | None
    transaction_id: str | None
    raw_payload: dict[str, Any]


@dataclass(slots=True)
class ParsedFile:
    parser_name: str
    bank_name: str
    account_id: str
    account_name: str
    account_number: str
    currency: str
    opening_date: datetime
    closing_date: datetime
    rows: list[ParsedRow]


def parse_decimal(value: str | None) -> Decimal | None:
    if value is None:
        return None
    cleaned = value.strip().replace(" ", "").replace("\xa0", "").replace('"', "")
    if not cleaned:
        return None
    return Decimal(cleaned.replace(",", "."))


def parse_date(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%d-%m-%Y")


def parse_iso_date(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y%m%d")


def detect_parser(filename: str, content: bytes) -> str:
    sample = content.decode("utf-8-sig", errors="ignore").splitlines()
    if not sample:
        raise ValueError("Uploaded file is empty.")
    first = next(csv.reader([sample[0]]))
    if len(first) == 9 and first[4] in {"USD", "PLN", "EUR"}:
        return "ing_pl_business_v1"
    if sample[0].startswith(
        "Posting Date;Effective Date;Counterparty Name;Account Holder’s Name;Payment Title (line 1)"
    ):
        return "alior_business_v1"
    raise ValueError(f"No parser matched file: {filename}")


def parse_known_csv(filename: str, content: bytes) -> ParsedFile:
    parser_name = detect_parser(filename, content)
    if parser_name == "ing_pl_business_v1":
        bank_name = "Santander"
        rows = list(csv.reader(content.decode("utf-8-sig").splitlines()))
        header = rows[0]
        account_number = header[2].lstrip("'")
        parsed_rows: list[ParsedRow] = []
        for index, row in enumerate(rows[1:], start=1):
            vendor_raw = row[3].strip() or None
            transaction_id = row[2].strip() or None
            parsed_rows.append(
                ParsedRow(
                    row_number=index,
                    posting_date=parse_date(row[0]),
                    transaction_date=parse_date(row[1]),
                    description=row[2].strip(),
                    operation_type_raw=None,
                    vendor_raw=vendor_raw,
                    counterparty_account=row[4].strip() or None,
                    amount_original=parse_decimal(row[5]) or Decimal("0"),
                    currency_original=header[4].strip(),
                    balance=parse_decimal(row[6]),
                    transaction_id=transaction_id,
                    raw_payload={
                        "row": row,
                        "filename": filename,
                    },
                )
            )
        return ParsedFile(
            parser_name=parser_name,
            bank_name=bank_name,
            account_id=account_number.replace(" ", ""),
            account_name=build_account_label(bank_name, header[4].strip(), account_number),
            account_number=account_number,
            currency=header[4].strip(),
            opening_date=parse_date(header[1]),
            closing_date=datetime.strptime(header[0].strip(), "%Y-%m-%d"),
            rows=parsed_rows,
        )

    if parser_name == "alior_business_v1":
        rows = list(csv.reader(content.decode("utf-8-sig").splitlines(), delimiter=";"))
        header = rows[0]
        data_rows = rows[1:]
        parsed_rows = []
        for index, row in enumerate(data_rows, start=1):
            title_parts = [part.strip() for part in row[4:8] if part.strip()]
            description = " ".join(title_parts) if title_parts else row[8].strip()
            parsed_rows.append(
                ParsedRow(
                    row_number=index,
                    posting_date=parse_iso_date(row[0]),
                    transaction_date=parse_iso_date(row[1]),
                    description=description,
                    operation_type_raw=row[8].strip() or None,
                    vendor_raw=row[2].strip() or None,
                    counterparty_account=None,
                    amount_original=parse_decimal(row[9]) or Decimal("0"),
                    currency_original=row[10].strip(),
                    balance=parse_decimal(row[11]),
                    transaction_id=None,
                    raw_payload={
                        "row": row,
                        "filename": filename,
                        "header": header,
                    },
                )
            )
        opening_date = min(row.posting_date for row in parsed_rows) if parsed_rows else datetime.now()
        closing_date = max(row.posting_date for row in parsed_rows) if parsed_rows else datetime.now()
        return ParsedFile(
            parser_name=parser_name,
            bank_name="Alior",
            account_id=f"ALIOR:{Path(filename).stem.upper()}",
            account_name=build_account_label("Alior", "MULTI"),
            account_number="",
            currency="MULTI",
            opening_date=opening_date,
            closing_date=closing_date,
            rows=parsed_rows,
        )

    raise ValueError(f"Unsupported parser: {parser_name}")
