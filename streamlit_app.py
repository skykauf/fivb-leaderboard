"""
Streamlit dashboard for browsing FIVB Leaderboard Postgres tables and team performance.

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
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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


@st.cache_data(ttl=60)
def _team_list() -> list[str]:
    """Distinct team display names from core (for Performance tab)."""
    engine = _engine()
    if engine is None:
        return []
    try:
        q = text("""
            SELECT DISTINCT team_display_name
            FROM core.dim_team_tournaments
            WHERE team_display_name IS NOT NULL
            ORDER BY team_display_name
        """)
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


@st.cache_data(ttl=60)
def _performance_by_host_country(team_display_name: str) -> pd.DataFrame | None:
    """Wins, losses, and average tournament finish position by tournament host country."""
    engine = _engine()
    if engine is None:
        return None
    try:
        # Group by tournament host country (from dim_tournaments); one finishing_pos per tournament
        q = text("""
            WITH matches_with_host AS (
                SELECT
                    m.tournament_id,
                    dt.country_code AS host_country,
                    dt.country_name AS host_country_name,
                    (m.team1_display_name = :team AND m.is_winner_team1) OR (m.team2_display_name = :team AND (m.is_winner_team1 = false)) AS won
                FROM core.fct_matches m
                JOIN core.dim_tournaments dt ON dt.tournament_id = m.tournament_id
                WHERE m.team1_display_name = :team OR m.team2_display_name = :team
            ),
            wins_losses AS (
                SELECT
                    host_country,
                    host_country_name,
                    SUM(CASE WHEN won THEN 1 ELSE 0 END)::int AS wins,
                    SUM(CASE WHEN NOT won THEN 1 ELSE 0 END)::int AS losses
                FROM matches_with_host
                GROUP BY host_country, host_country_name
            ),
            finish_by_tournament AS (
                SELECT
                    m.host_country,
                    m.tournament_id,
                    s.finishing_pos
                FROM matches_with_host m
                JOIN core.fct_tournament_standings s
                    ON s.tournament_id = m.tournament_id AND s.team_display_name = :team
                GROUP BY m.host_country, m.tournament_id, s.finishing_pos
            ),
            avg_depth AS (
                SELECT host_country, AVG(finishing_pos) AS avg_finish_pos
                FROM finish_by_tournament
                GROUP BY host_country
            )
            SELECT
                wl.host_country,
                wl.host_country_name,
                wl.wins,
                wl.losses,
                wl.wins + wl.losses AS total_matches,
                ROUND(ad.avg_finish_pos::numeric, 2) AS avg_finish_pos
            FROM wins_losses wl
            LEFT JOIN avg_depth ad ON ad.host_country = wl.host_country
            ORDER BY wl.wins DESC, total_matches DESC, wl.host_country
        """)
        with engine.connect() as conn:
            return pd.read_sql(q, conn, params={"team": team_display_name})
    except Exception:
        return None


def _render_table_browser(engine) -> None:
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


def _render_performance_tab(engine) -> None:
    st.subheader("Performance by tournament host country")
    st.caption("Wins/losses and average tournament finish position (1 = champion) in tournaments hosted in each country.")
    st.info("**Country** = tournament host country (where the event was held).")

    teams = _team_list()
    if not teams:
        st.info("No teams in `core.dim_team_tournaments`. Run ETL and `dbt run` to populate core models.")
        return

    team = st.selectbox("Team", options=teams, index=0, help="Team display name (Player A / Player B)")

    df = _performance_by_host_country(team)
    if df is None:
        st.error("Could not load performance data. Ensure core.fct_matches and core.fct_tournament_standings exist.")
        return
    if df.empty:
        st.warning(f"No matches found for **{team}**.")
        return

    # Use country name for display when available
    x_label = df["host_country_name"].fillna(df["host_country"]).tolist()

    # Wins vs losses by tournament host country (grouped bar)
    fig_wl = go.Figure()
    fig_wl.add_trace(
        go.Bar(name="Wins", x=x_label, y=df["wins"], marker_color="#2ecc71")
    )
    fig_wl.add_trace(
        go.Bar(name="Losses", x=x_label, y=df["losses"], marker_color="#e74c3c")
    )
    fig_wl.update_layout(
        barmode="group",
        title=f"Wins vs losses by tournament host country — {team}",
        xaxis_title="Tournament host country",
        yaxis_title="Matches",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(t=60),
    )
    st.plotly_chart(fig_wl, use_container_width=True)

    # Average tournament finish position (1 = champion; lower is better)
    fig_depth = px.bar(
        df.assign(host_label=x_label),
        x="host_label",
        y="avg_finish_pos",
        title="Average tournament finish position in events hosted in this country (1 = champion)",
        labels={"host_label": "Tournament host country", "avg_finish_pos": "Avg finish position"},
        color="avg_finish_pos",
        color_continuous_scale="RdYlGn_r",
        color_continuous_midpoint=8,
    )
    fig_depth.update_layout(height=400, margin=dict(t=60), showlegend=False)
    fig_depth.update_xaxis(tickangle=-45)
    st.plotly_chart(fig_depth, use_container_width=True)

    with st.expander("Data table"):
        st.dataframe(df, use_container_width=True)
        st.caption("host_country = code, host_country_name = full name of tournament host.")
        st.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"performance_{team.replace(' / ', '_').replace(' ', '')[:50]}.csv",
            mime="text/csv",
            key="perf_csv",
        )


def main() -> None:
    st.set_page_config(page_title="FIVB Leaderboard – Postgres", layout="wide")
    st.title("FIVB Leaderboard")

    engine = _engine()
    if engine is None:
        return

    tab_browser, tab_perf = st.tabs(["Table browser", "Performance"])

    with tab_browser:
        _render_table_browser(engine)

    with tab_perf:
        _render_performance_tab(engine)


if __name__ == "__main__":
    main()
