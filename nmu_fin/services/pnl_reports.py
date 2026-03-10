from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

from ..config import SAMPLE_IMPORT_DIR


MONTHS = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]


@dataclass(slots=True)
class PnlSheet:
    year: int
    months: list[str]
    summary: dict[str, list[float | None]]
    matrix_rows: list[dict]


def parse_currency(value: str) -> float | None:
    cleaned = (value or "").strip().replace('"', "").replace("zł", "").replace(" ", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_number(value: str) -> float | None:
    cleaned = (value or "").strip().replace('"', "").replace("zł", "").replace(" ", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def discover_pnl_paths(sample_dir: Path = SAMPLE_IMPORT_DIR) -> dict[int, Path]:
    by_year: dict[int, Path] = {}
    for path in sorted(sample_dir.glob("P&L*.csv")):
        match = re.search(r"(20\d{2})", path.name)
        if match:
            by_year[int(match.group(1))] = path
    return by_year


def load_pnl_sheet(year: int | None = None, sample_dir: Path = SAMPLE_IMPORT_DIR) -> PnlSheet | None:
    paths = discover_pnl_paths(sample_dir)
    if not paths:
        return None
    target_year = year if year in paths else max(paths)
    path = paths[target_year]
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))

    summary = {
        "cash": [parse_currency(value) for value in rows[0][1:13]],
        "income": [parse_currency(value) for value in rows[1][1:13]],
        "expenses": [parse_currency(value) for value in rows[2][1:13]],
        "net": [parse_currency(value) for value in rows[3][1:13]],
    }

    month_row_index = next(
        (index for index, row in enumerate(rows) if row and len(row) > 1 and row[1:13] == MONTHS),
        None,
    )
    if month_row_index is None:
        return PnlSheet(year=target_year, months=MONTHS, summary=summary, matrix_rows=[])

    matrix_rows: list[dict] = []
    group_header_pending = True
    for row in rows[month_row_index + 1 :]:
        label = (row[0] if row else "").strip()
        values = [parse_number(value) for value in row[1:13]]
        if label in {"Income", "Expenses"} and any(value is not None for value in values):
            break
        if not label and not any(value is not None for value in values):
            group_header_pending = True
            continue
        if not label:
            continue
        is_group = group_header_pending
        matrix_rows.append(
            {
                "label": label,
                "values": values,
                "is_group": is_group,
            }
        )
        group_header_pending = False

    return PnlSheet(
        year=target_year,
        months=MONTHS,
        summary=summary,
        matrix_rows=matrix_rows,
    )
