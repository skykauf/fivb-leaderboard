"""
Streamlit dashboard for browsing FIVB Leaderboard Postgres tables.

Run from project root with:
  streamlit run streamlit_app.py

Requires DATABASE_URL in .env (see .env.example).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path when running streamlit run streamlit_app.py
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st
import pandas as pd
from sqlalchemy import text

from etl.db import get_engine


# Schemas we care about (order matches project layout)
SCHEMAS = ("raw", "staging", "core", "mart")

# Default row limit for table preview
DEFAULT_ROW_LIMIT = 10_000


@st.cache_resource
def _engine():
    try:
        return get_engine()
    except Exception as e:
        st.error(f"Cannot connect to Postgres: {e}. Set DATABASE_URL in .env (see .env.example).")
        return None


@st.cache_data(ttl=60)
def _tables(schema: str) -> list[str]:
    engine = _engine()
    if engine is None:
        return []
    q = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=60)
def _row_count(schema: str, table: str) -> int | None:
    engine = _engine()
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            r = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar()
            return int(r)
    except Exception:
        return None


def main() -> None:
    st.set_page_config(page_title="FIVB Leaderboard – Postgres", layout="wide")
    st.title("FIVB Leaderboard – Postgres tables")

    engine = _engine()
    if engine is None:
        return

    # Sidebar: schema + table picker
    with st.sidebar:
        st.subheader("Table browser")
        schema = st.selectbox("Schema", options=SCHEMAS, index=0)
        table_names = _tables(schema)
        if not table_names:
            st.info(f"No tables in schema `{schema}`. Run ETL/dbt to populate.")
            table_name = None
        else:
            table_name = st.selectbox("Table", options=table_names, index=0)
        st.divider()
        limit = st.number_input(
            "Max rows to load",
            min_value=100,
            max_value=500_000,
            value=DEFAULT_ROW_LIMIT,
            step=1000,
            help="Large limits may be slow.",
        )

    if not table_name:
        st.caption("Select a schema that has tables (e.g. run `python -m etl.load_raw` then `dbt run`).")
        return

    full_name = f'"{schema}"."{table_name}"'
    count = _row_count(schema, table_name)
    if count is not None:
        st.caption(f"Table {full_name} — {count:,} rows")

    try:
        sql = text(f'SELECT * FROM {full_name} LIMIT :lim')
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"lim": limit})
    except Exception as e:
        st.error(f"Query failed: {e}")
        return

    st.dataframe(df, use_container_width=True, height=500)

    with st.expander("Export"):
        st.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"{schema}_{table_name}.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
