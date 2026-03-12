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

# PostgreSQL data_type -> category for filter UI
_PG_NUMERIC = {"integer", "bigint", "smallint", "numeric", "real", "double precision"}
_PG_DATETIME = {"timestamp with time zone", "timestamp without time zone", "date"}
_PG_STRING = {"character varying", "varchar", "text", "character", "char"}


def _pg_type_category(data_type: str) -> str:
    if data_type in _PG_NUMERIC:
        return "numeric"
    if data_type in _PG_DATETIME:
        return "datetime"
    if data_type in _PG_STRING:
        return "string"
    return "other"


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


@st.cache_data(ttl=60)
def _table_columns(schema: str, table: str) -> list[tuple[str, str]]:
    """Return (column_name, data_type) for table. data_type is PostgreSQL type name."""
    engine = _engine()
    if engine is None:
        return []
    q = text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table
        ORDER BY ordinal_position
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema, "table": table}).fetchall()
    return [(r[0], r[1]) for r in rows]


def _column_min_max(engine, schema: str, table: str, columns: list[str]) -> dict[str, tuple[float | None, float | None] | tuple[str | None, str | None]]:
    """Get min/max for given columns in one query. Returns dict keyed by column; value is (min, max) as float or date string."""
    if not columns:
        return {}
    full_name = f'"{schema}"."{table}"'
    selects = []
    for i, c in enumerate(columns):
        qc = f'"{c}"'
        selects.append(f"min({qc}) as c{i}_min")
        selects.append(f"max({qc}) as c{i}_max")
    sql = text(f"SELECT {', '.join(selects)} FROM {full_name}")
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
    result = {}
    for i, c in enumerate(columns):
        lo, hi = row[i * 2], row[i * 2 + 1]
        if lo is not None and hi is not None:
            if hasattr(lo, "isoformat"):  # date/datetime
                result[c] = (lo.isoformat()[:10], hi.isoformat()[:10])
            else:
                result[c] = (float(lo), float(hi))
        else:
            result[c] = (None, None)
    return result


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

    # Column metadata for filter UI (no data load yet)
    table_columns = _table_columns(schema, table_name)
    column_names = [t[0] for t in table_columns]
    type_map = {t[0]: t[1] for t in table_columns}
    if not column_names:
        st.warning("Could not load column list for this table.")
        return

    # Filter UI: collect filter criteria (applied in SQL below)
    with st.expander("Filter by columns", expanded=False):
        filter_columns = st.multiselect(
            "Columns to filter on",
            options=column_names,
            default=[],
            key=f"filter_cols_{schema}_{table_name}",
        )
        if not filter_columns:
            st.caption("Select one or more columns above to add filters. Filters apply to the database query.")
        min_max_cols = [
            c for c in filter_columns
            if _pg_type_category(type_map.get(c, "")) in ("numeric", "datetime")
        ]
        min_max = _column_min_max(engine, schema, table_name, min_max_cols) if min_max_cols else {}

        where_parts: list[str] = []
        params: dict = {"lim": limit}
        n_display_cols = max(1, min(3, len(filter_columns)))
        cols = st.columns(n_display_cols)

        for i, col_name in enumerate(filter_columns):
            qc = f'"{col_name}"'
            cat = _pg_type_category(type_map.get(col_name, ""))
            key_prefix = f"filter_{schema}_{table_name}_{col_name}"
            with cols[i % len(cols)]:
                if cat == "string":
                    val = st.text_input(
                        f"**{col_name}** (contains)",
                        key=key_prefix,
                        placeholder="substring...",
                    )
                    if val:
                        p = f"_p_{i}_like"
                        params[p] = f"%{val}%"
                        where_parts.append(f"{qc}::text ILIKE :{p}")
                elif cat == "numeric":
                    mm = min_max.get(col_name)
                    if mm and mm[0] is not None and mm[1] is not None:
                        c_min, c_max = mm[0], mm[1]
                        if c_min > c_max:
                            c_min, c_max = c_max, c_min
                        step = (c_max - c_min) / 100 if c_max > c_min else 1.0
                        lo = st.number_input(
                            f"**{col_name}** min",
                            key=f"{key_prefix}_min",
                            value=float(c_min),
                            min_value=float(c_min),
                            max_value=float(c_max),
                            step=step,
                        )
                        hi = st.number_input(
                            f"**{col_name}** max",
                            key=f"{key_prefix}_max",
                            value=float(c_max),
                            min_value=float(c_min),
                            max_value=float(c_max),
                            step=step,
                        )
                        p_lo, p_hi = f"_p_{i}_min", f"_p_{i}_max"
                        params[p_lo], params[p_hi] = lo, hi
                        where_parts.append(f"({qc} >= :{p_lo} AND {qc} <= :{p_hi})")
                    else:
                        st.caption(f"**{col_name}** — no numeric range")
                elif cat == "datetime":
                    mm = min_max.get(col_name)
                    if mm and mm[0] is not None and mm[1] is not None:
                        d_min, d_max = mm[0], mm[1]
                        lo_d = st.date_input(
                            f"**{col_name}** from",
                            key=f"{key_prefix}_from",
                            value=pd.to_datetime(d_min).date(),
                            min_value=pd.to_datetime(d_min).date(),
                            max_value=pd.to_datetime(d_max).date(),
                        )
                        hi_d = st.date_input(
                            f"**{col_name}** to",
                            key=f"{key_prefix}_to",
                            value=pd.to_datetime(d_max).date(),
                            min_value=pd.to_datetime(d_min).date(),
                            max_value=pd.to_datetime(d_max).date(),
                        )
                        p_lo, p_hi = f"_p_{i}_from", f"_p_{i}_to"
                        params[p_lo], params[p_hi] = lo_d, hi_d
                        where_parts.append(f"({qc}::date >= :{p_lo} AND {qc}::date <= :{p_hi})")
                    else:
                        st.caption(f"**{col_name}** — no date range")
                else:
                    val = st.text_input(
                        f"**{col_name}** (equals)",
                        key=key_prefix,
                        placeholder="exact...",
                    )
                    if val:
                        p = f"_p_{i}_eq"
                        params[p] = val.strip().lower()
                        where_parts.append(f"LOWER(TRIM({qc}::text)) = :{p}")

    # Run query with WHERE so the data pull is filtered
    try:
        if where_parts:
            where_sql = " AND ".join(where_parts)
            sql = text(f'SELECT * FROM {full_name} WHERE {where_sql} LIMIT :lim')
        else:
            sql = text(f'SELECT * FROM {full_name} LIMIT :lim')
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
    except Exception as e:
        st.error(f"Query failed: {e}")
        return

    if filter_columns and where_parts:
        st.caption(f"Showing {len(df):,} rows (filtered in database).")

    st.dataframe(df, width="stretch", height=500)

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
    # Coerce min/max to string so Streamlit's Arrow serialization doesn't fail on mixed types (dates vs ints)
    for col in ("min", "max"):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: "" if pd.isna(x) else str(x))
    tables = ["All"] + sorted(df["table"].unique().tolist())
    selected = st.selectbox("Filter by table", options=tables, index=0)
    if selected != "All":
        df = df[df["table"] == selected]
    st.dataframe(df, width="stretch", height=500)
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
