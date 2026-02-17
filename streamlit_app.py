"""
Minimal Streamlit app to query Postgres models and display with Plotly Express.
Dynamically discovers schemas and tables from the database.
Uses DATABASE_URL from .env (same as ETL). Run: streamlit run streamlit_app.py
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


# System schemas to hide from the schema picker (and any pg_* internal schema)
def _is_system_schema(name: str) -> bool:
    return name in ("pg_catalog", "information_schema", "pg_toast") or name.startswith(
        "pg_temp"
    )


@st.cache_resource
def get_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        st.error(
            "Set DATABASE_URL in .env (e.g. postgresql+psycopg2://user:pass@localhost:5432/fivb_leaderboard)"
        )
        st.stop()
    return create_engine(url, future=True)


@st.cache_data(ttl=60)
def get_schemas(_engine) -> list[str]:
    """Discover all non-system schemas in the database."""
    q = text("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name")
    with _engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    return [r[0] for r in rows if not _is_system_schema(r[0])]


@st.cache_data(ttl=60)
def get_tables(_engine, schema: str) -> list[str]:
    """Discover all tables (and views) in a schema."""
    q = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = :schema
          AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY table_name
        """
    )
    with _engine.connect() as conn:
        rows = conn.execute(q, {"schema": schema}).fetchall()
    return [r[0] for r in rows]


def load_table(engine, schema: str, table: str, limit: int = 10_000) -> pd.DataFrame:
    q = text(f'SELECT * FROM "{schema}"."{table}" LIMIT :n')
    return pd.read_sql(q, engine, params={"n": limit})


def get_mart_schema_and_tables(_engine) -> tuple[str | None, list[tuple[str, str]]]:
    """Return (mart_schema, [(schema, table_name), ...]) for mart layer, or (None, [])."""
    schemas = get_schemas(_engine)
    for schema in schemas:
        if schema == "mart":
            tables = get_tables(_engine, schema)
            mart_tables = [(schema, t) for t in tables if t.startswith("mart_")]
            if mart_tables:
                return schema, mart_tables
    for schema in schemas:
        tables = get_tables(_engine, schema)
        mart_tables = [(schema, t) for t in tables if t.startswith("mart_")]
        if mart_tables:
            return schema, mart_tables
    return None, []


def render_mart_graph_tab(engine):
    """Mart layer tab: pick a mart table and build a chart with configurable axes."""
    mart_schema, mart_tables = get_mart_schema_and_tables(engine)
    if not mart_tables:
        st.info(
            "No mart layer found. Expect a schema (e.g. `mart`) with tables named `mart_*`. "
            "Run dbt to build the mart models."
        )
        return

    # Schema might vary (e.g. mart vs fivb_leaderboard_mart)
    schema = mart_tables[0][0]
    table_options = [f"{s}.{t}" for s, t in mart_tables]
    selected = st.sidebar.selectbox(
        "Mart table",
        table_options,
        key="mart_table",
    )
    schema, table = next((s, t) for s, t in mart_tables if f"{s}.{t}" == selected)
    limit = st.sidebar.number_input(
        "Max rows",
        min_value=100,
        max_value=100_000,
        value=5000,
        step=500,
        key="mart_limit",
    )

    with st.spinner("Loading mart data..."):
        df = load_table(engine, schema, table, limit=limit)

    if df.empty:
        st.warning(f"No rows in `{schema}`.`{table}`.")
        return

    st.sidebar.metric("Rows", len(df))
    numeric_cols = df.select_dtypes(include=["number", "bool"]).columns.tolist()
    object_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    date_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()
    all_cols = df.columns.tolist()
    cat_cols = object_cols + date_cols

    st.subheader(f"Mart: `{schema}`.`{table}`")
    st.caption(
        "Configure the chart below, then view the table under the chart if needed."
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        chart_type = st.selectbox(
            "Chart type",
            ["Bar", "Line", "Scatter"],
            key="mart_chart_type",
        )
    with col2:
        x_col = st.selectbox("X axis", [None] + all_cols, key="mart_x")
    with col3:
        if chart_type == "Bar" and x_col:
            y_options = ["Count"] + numeric_cols
        else:
            y_options = numeric_cols
        y_col = st.selectbox("Y axis", [None] + y_options, key="mart_y")

    color_col = st.selectbox(
        "Color by (optional)",
        [None] + cat_cols,
        key="mart_color",
    )
    max_points = st.slider(
        "Max points in chart", 50, 5000, 1000, 50, key="mart_max_points"
    )

    if not x_col or not y_col:
        st.info("Select X and Y to build the chart.")
        st.dataframe(df.head(500), use_container_width=True, height=300)
        return

    # Build plot dataframe
    if chart_type == "Bar" and y_col == "Count":
        group_cols = [x_col] + ([color_col] if color_col else [])
        plot_df = df.groupby(group_cols, dropna=False).size().reset_index(name="Count")
        plot_df = plot_df.head(max_points)
    else:
        cols = [x_col, y_col]
        if color_col:
            cols.append(color_col)
        plot_df = df[cols].dropna().head(max_points)
        if chart_type == "Line" and x_col in plot_df.columns:
            if plot_df[x_col].dtype.kind in "M" or str(plot_df[x_col].dtype).startswith(
                "datetime"
            ):
                plot_df = plot_df.sort_values(x_col)

    if plot_df.empty:
        st.warning("No data left after filtering. Adjust axes or max points.")
        st.dataframe(df.head(500), use_container_width=True, height=300)
        return

    use_color = color_col and color_col in plot_df.columns
    try:
        if chart_type == "Bar":
            if y_col == "Count":
                fig = px.bar(
                    plot_df,
                    x=x_col,
                    y="Count",
                    color=color_col if use_color else None,
                    title=f"Count by {x_col}",
                )
            else:
                fig = px.bar(
                    plot_df,
                    x=x_col,
                    y=y_col,
                    color=color_col if use_color else None,
                    title=f"{y_col} by {x_col}",
                )
        elif chart_type == "Line":
            fig = px.line(
                plot_df,
                x=x_col,
                y=y_col,
                color=color_col if use_color else None,
                title=f"{y_col} over {x_col}",
            )
        else:
            fig = px.scatter(
                plot_df,
                x=x_col,
                y=y_col,
                color=color_col if use_color else None,
                title=f"{y_col} vs {x_col}",
            )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"Chart error: {e}")
        st.dataframe(plot_df.head(100), use_container_width=True)

    with st.expander("View data table"):
        st.dataframe(df, use_container_width=True, height=300)


def main():
    st.set_page_config(page_title="FIVB Leaderboard", page_icon="🏐", layout="wide")
    st.title("FIVB Beach Pro Tour — Data explorer")
    st.caption(
        "Dynamically discovers schemas and tables from Postgres. Set `DATABASE_URL` in `.env`."
    )

    engine = get_engine()

    # One mode selector so the sidebar only shows controls for the active view
    view = st.sidebar.radio(
        "View",
        ["Data explorer", "Mart visualizations"],
        key="view_mode",
    )
    st.sidebar.divider()

    if view == "Mart visualizations":
        render_mart_graph_tab(engine)
    else:
        _render_data_explorer_tab(engine)


def _render_data_explorer_tab(engine):
    schemas = get_schemas(engine)
    if not schemas:
        st.warning("No user schemas found in the database.")
        return

    schema = st.sidebar.selectbox("Schema", schemas, index=0, key="schema")
    tables = get_tables(engine, schema)
    if not tables:
        st.warning(f"No tables or views in schema `{schema}`.")
        return

    table = st.sidebar.selectbox("Table / view", tables, index=0, key="table")
    limit = st.sidebar.number_input(
        "Max rows", min_value=100, max_value=100_000, value=5000, step=500
    )

    with st.spinner("Loading..."):
        df = load_table(engine, schema, table, limit=limit)

    if df.empty:
        st.warning(f"No rows in `{table}`.")
        return

    st.sidebar.metric("Rows", len(df))

    # Table view
    st.subheader(f"`{schema}`.`{table}`")
    st.dataframe(df, use_container_width=True, height=300)

    # Plotly Express chart based on table
    st.subheader("Chart")
    numeric_cols = df.select_dtypes(include=["number", "bool"]).columns.tolist()
    object_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    date_cols = df.select_dtypes(include=["datetime64"]).columns.tolist()
    all_cat = object_cols + date_cols

    chart_ok = False
    if table == "dim_tournament" and "country" in df.columns:
        by_country = df["country"].value_counts().reset_index()
        by_country.columns = ["country", "count"]
        fig = px.bar(
            by_country.head(25), x="country", y="count", title="Tournaments by country"
        )
        st.plotly_chart(fig, use_container_width=True)
        chart_ok = True
    elif table == "dim_tournament" and "season" in df.columns:
        by_season = df["season"].value_counts().sort_index().reset_index()
        by_season.columns = ["season", "count"]
        fig = px.bar(by_season, x="season", y="count", title="Tournaments by season")
        st.plotly_chart(fig, use_container_width=True)
        chart_ok = True
    elif (
        table == "fact_ranking_snapshot"
        and "rank" in df.columns
        and "points" in df.columns
    ):
        color_col = "ranking_type" if "ranking_type" in df.columns else None
        fig = px.scatter(
            df.head(500), x="rank", y="points", color=color_col, title="Rank vs points"
        )
        st.plotly_chart(fig, use_container_width=True)
        chart_ok = True
    elif table == "fact_match" and "tournament_id" in df.columns:
        by_tournament = df.groupby("tournament_id").size().reset_index(name="matches")
        by_tournament = by_tournament.nlargest(30, "matches")
        fig = px.bar(
            by_tournament,
            x="tournament_id",
            y="matches",
            title="Matches per tournament (top 30)",
        )
        st.plotly_chart(fig, use_container_width=True)
        chart_ok = True
    elif table == "dim_team" and "country" in df.columns:
        by_country = df["country"].value_counts().reset_index()
        by_country.columns = ["country", "count"]
        fig = px.bar(
            by_country.head(25), x="country", y="count", title="Teams by country"
        )
        st.plotly_chart(fig, use_container_width=True)
        chart_ok = True

    if not chart_ok and numeric_cols and all_cat:
        x_col = st.selectbox("X axis", [None] + all_cat, key="x")
        y_col = st.selectbox("Y axis", [None] + numeric_cols, key="y")
        if x_col and y_col:
            fig = px.bar(df.head(500), x=x_col, y=y_col, title=f"{y_col} by {x_col}")
            st.plotly_chart(fig, use_container_width=True)
    elif not chart_ok and len(numeric_cols) >= 2:
        x_col = st.selectbox("X axis", numeric_cols, key="x2")
        y_col = st.selectbox(
            "Y axis", [c for c in numeric_cols if c != x_col], key="y2"
        )
        if x_col and y_col:
            fig = px.scatter(
                df.head(500), x=x_col, y=y_col, title=f"{y_col} vs {x_col}"
            )
            st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
