from __future__ import annotations

import hashlib
import hmac
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
DATABASE_PATH = DATA_DIR / "nmu_fin.duckdb"
SAMPLE_IMPORT_DIR = ROOT_DIR / "Banks Import" / "Santander Bank"
DEFAULT_CATEGORIES = [
    ("Income", None),
    ("Sales", "Income"),
    ("Refunds", "Income"),
    ("Transfers", "Income"),
    ("Expenses", None),
    ("Exchange", None),
    ("Currency Exchange", "Exchange"),
    ("Materials", "Expenses"),
    ("Fuel", "Expenses"),
    ("Software", "Expenses"),
    ("Travel", "Expenses"),
    ("Food", "Expenses"),
    ("Taxes", "Expenses"),
    ("Subscriptions", "Expenses"),
    ("Personal", "Expenses"),
    ("Household", "Expenses"),
    ("Uncategorized", "Expenses"),
]


def get_database_path() -> Path:
    override = os.environ.get("NMU_FIN_DB_PATH")
    return Path(override) if override else DATABASE_PATH


def get_app_password() -> str:
    return os.environ.get("NMU_FIN_APP_PASSWORD", "Hemp12#$")


def get_app_secret() -> str:
    return os.environ.get("NMU_FIN_APP_SECRET", hashlib.sha256(str(get_database_path()).encode("utf-8")).hexdigest())


def sign_auth_token(payload: str) -> str:
    secret = get_app_secret().encode("utf-8")
    return hmac.new(secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()
