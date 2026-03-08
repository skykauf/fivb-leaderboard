"""
Streamlit dashboard for table exploration and raw column stats.

Run from project root with:
  streamlit run streamlit_app.py

Requires DATABASE_URL in .env (see .env.example).

For team/player performance and over-time charts, use dash_helpers in a separate app or tab.
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
        AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema}).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=60)
def _raw_column_stats():
    """Raw schema column statistics (null %, distinct count, min/max)."""
    try:
        from scripts.raw_column_stats import get_raw_column_stats
        engine = _engine()
        if engine is None:
            return None
        return get_raw_column_stats(engine)
    except Exception:
        return None


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


def _render_table_browser(engine) -> None:
    with st.sidebar:
        st.subheader("Table browser")
        schema = st.selectbox("Schema", options=SCHEMAS, index=0)
        table_names = _tables(schema)
        if not table_names:
            hint = "Run ETL/dbt to populate."
            if schema == "mart":
                hint = "Run `dbt run --select mart` to build mart models (e.g. champion_mart)."
            st.info(f"No tables or views in schema `{schema}`. {hint}")
            table_name = None
        else:
            table_name = st.selectbox("Table / view", options=table_names, index=0)
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
        st.caption("Select a schema that has tables or views (e.g. run `python -m etl.load_raw` then `dbt run`).")
        return

    full_name = f'"{schema}"."{table_name}"'
    count = _row_count(schema, table_name)
    if count is not None:
        st.caption(f"{full_name} — {count:,} rows")

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


def _render_raw_stats_tab(engine) -> None:
    st.subheader("Raw table column statistics")
    st.caption("Summary stats for every column in schema `raw` (null %, distinct count, min/max for numeric/date). Run pipeline first.")

    stats = _raw_column_stats()
    if stats is None:
        st.warning("Could not load raw stats. Ensure DATABASE_URL is set and raw tables exist (run pipeline).")
        return
    rows = [s for s in stats if s.get("column")]
    if not rows:
        st.info("No raw tables or columns found. Run `python -m etl.load_raw` to populate raw schema.")
        return

    df = pd.DataFrame(rows)
    df = df.rename(columns={"distinct_count": "distinct"})
    tables = ["All"] + sorted(df["table"].unique().tolist())
    selected = st.selectbox("Filter by table", options=tables, index=0)
    if selected != "All":
        df = df[df["table"] == selected]
    st.dataframe(df, use_container_width=True, height=500)
    with st.expander("Export"):
        st.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="raw_column_stats.csv",
            mime="text/csv",
            key="raw_stats_csv",
        )


def main() -> None:
    st.set_page_config(page_title="FIVB Leaderboard – Postgres", layout="wide")
    st.title("FIVB Leaderboard")

    engine = _engine()
    if engine is None:
        return

    tab_browser, tab_raw_stats = st.tabs(["Table browser", "Raw table stats"])

    with tab_browser:
        _render_table_browser(engine)

    with tab_raw_stats:
        _render_raw_stats_tab(engine)


if __name__ == "__main__":
    main()
