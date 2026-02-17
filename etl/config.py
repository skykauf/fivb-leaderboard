import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass
class DbConfig:
    """Postgres connection for ETL and dbt."""

    url: str  # e.g. postgresql+psycopg2://user:pass@localhost:5432/fivb_leaderboard


def get_db_config() -> DbConfig:
    """
    Read Postgres connection from environment.

    Expected env var:
      DATABASE_URL  e.g. postgresql+psycopg2://user:pass@localhost:5432/fivb_leaderboard
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL must be set in environment.")
    return DbConfig(url=url)
