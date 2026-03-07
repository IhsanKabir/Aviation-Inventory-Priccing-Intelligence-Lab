from __future__ import annotations

import os
from urllib.parse import quote_plus
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    # Optional dependency: allow runtime without python-dotenv.
    def load_dotenv(*args, **kwargs):
        return False


def get_database_url(
    fallback: str = "postgresql+psycopg2://postgres@localhost:5432/Playwright_API_Calling",
) -> str:
    """
    Resolve DB URL from:
    1) AIRLINE_DB_URL
    2) DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD
    3) fallback
    """
    load_dotenv()
    url = os.getenv("AIRLINE_DB_URL", "").strip()
    if url:
        return url

    host = os.getenv("DB_HOST", "").strip()
    port = os.getenv("DB_PORT", "").strip()
    name = os.getenv("DB_NAME", "").strip()
    user = os.getenv("DB_USER", "").strip()
    pwd = os.getenv("DB_PASSWORD", "").strip()

    if host and port and name and user:
        user_q = quote_plus(user)
        if pwd:
            pwd_q = quote_plus(pwd)
            return f"postgresql+psycopg2://{user_q}:{pwd_q}@{host}:{port}/{name}"
        return f"postgresql+psycopg2://{user_q}@{host}:{port}/{name}"

    return fallback
