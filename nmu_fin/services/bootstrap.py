from __future__ import annotations

from pathlib import Path

from ..config import SAMPLE_IMPORT_DIR
from ..db import connect
from ..parsers import parse_known_csv
from .fx import fetch_nbp_rates_for_dates
from .imports import build_preview, commit_preview


def list_import_batches(limit: int = 20) -> list[dict]:
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT source_filename, imported_at, row_count, duplicate_count, status
            FROM import_batches
            ORDER BY imported_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
    return [
        {
            "source_filename": row[0],
            "imported_at": row[1],
            "row_count": row[2],
            "duplicate_count": row[3],
            "status": row[4],
        }
        for row in rows
    ]


def import_sample_history(sample_dir: Path = SAMPLE_IMPORT_DIR) -> int:
    fetch_nbp_rates_for_dates(sample_transaction_dates(sample_dir))
    imported = 0
    for path in sorted(sample_dir.glob("*.csv")):
        try:
            preview = build_preview(path.name, path.read_bytes())
        except ValueError:
            continue
        commit_preview(preview)
        imported += 1
    return imported


def sample_transaction_dates(sample_dir: Path = SAMPLE_IMPORT_DIR) -> list:
    dates = set()
    for path in sorted(sample_dir.glob("*.csv")):
        try:
            parsed = parse_known_csv(path.name, path.read_bytes())
        except ValueError:
            continue
        for row in parsed.rows:
            dates.add(row.transaction_date.date())
    return sorted(dates)
