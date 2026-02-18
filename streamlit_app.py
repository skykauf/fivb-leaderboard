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
    """Distinct team display names from core (for Team Performance tab)."""
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
def _player_list() -> list[tuple[int, str]]:
    """(player_id, full_name) from staging for Player Performance tab."""
    engine = _engine()
    if engine is None:
        return []
    try:
        q = text("""
            SELECT player_id, full_name
            FROM staging.stg_fivb_players
            WHERE full_name IS NOT NULL
            ORDER BY full_name
        """)
        with engine.connect() as conn:
            rows = conn.execute(q).fetchall()
        return [(r[0], r[1]) for r in rows]
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


@st.cache_data(ttl=60)
def _tournament_mart_df() -> pd.DataFrame | None:
    """Load mart.tournament_mart for performance-over-time charts."""
    engine = _engine()
    if engine is None:
        return None
    try:
        q = text("""
            SELECT
                tournament_id,
                team_id,
                team_display_name,
                tournament_name,
                tournament_country_code,
                tournament_country_name,
                season,
                season_year,
                tournament_start_date,
                tournament_tier,
                tournament_gender,
                tournament_is_major,
                finishing_pos,
                tournament_points,
                sum_opponent_points_beaten,
                match_wins,
                match_losses,
                wins_vs_higher_seed,
                losses_vs_lower_seed,
                pool_wins,
                elimination_wins,
                quality_win_loss_score,
                quality_win_loss_score_points
            FROM mart.tournament_mart
            ORDER BY tournament_start_date, team_display_name
        """)
        with engine.connect() as conn:
            return pd.read_sql(q, conn)
    except Exception:
        return None


@st.cache_data(ttl=60)
def _performance_by_host_country_player(player_id: int) -> pd.DataFrame | None:
    """Wins, losses, and average tournament finish position by tournament host country (player level)."""
    engine = _engine()
    if engine is None:
        return None
    try:
        q = text("""
            WITH player_teams AS (
                SELECT team_id, tournament_id
                FROM core.dim_team_tournaments
                WHERE player_a_id = :player_id OR player_b_id = :player_id
            ),
            matches_with_host AS (
                SELECT
                    m.tournament_id,
                    dt.country_code AS host_country,
                    dt.country_name AS host_country_name,
                    (pt.team_id = m.team1_id AND m.is_winner_team1) OR (pt.team_id = m.team2_id AND (m.is_winner_team1 = false)) AS won
                FROM core.fct_matches m
                JOIN player_teams pt ON (m.team1_id = pt.team_id AND m.tournament_id = pt.tournament_id)
                                 OR (m.team2_id = pt.team_id AND m.tournament_id = pt.tournament_id)
                JOIN core.dim_tournaments dt ON dt.tournament_id = m.tournament_id
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
                    dt.country_code AS host_country,
                    pt.tournament_id,
                    s.finishing_pos
                FROM player_teams pt
                JOIN core.fct_tournament_standings s ON s.team_id = pt.team_id AND s.tournament_id = pt.tournament_id
                JOIN core.dim_tournaments dt ON dt.tournament_id = pt.tournament_id
                GROUP BY dt.country_code, dt.country_name, pt.tournament_id, s.finishing_pos
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
            return pd.read_sql(q, conn, params={"player_id": player_id})
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


def _render_performance_charts(df: pd.DataFrame, entity_label: str, download_key: str, download_filename: str) -> None:
    """Shared charts and table for team or player performance by host country."""
    x_label = df["host_country_name"].fillna(df["host_country"]).tolist()

    fig_wl = go.Figure()
    fig_wl.add_trace(go.Bar(name="Wins", x=x_label, y=df["wins"], marker_color="#2ecc71"))
    fig_wl.add_trace(go.Bar(name="Losses", x=x_label, y=df["losses"], marker_color="#e74c3c"))
    fig_wl.update_layout(
        barmode="group",
        title=f"Wins vs losses by tournament host country — {entity_label}",
        xaxis_title="Tournament host country",
        yaxis_title="Matches",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(t=60),
    )
    st.plotly_chart(fig_wl, use_container_width=True)

    fig_depth = px.bar(
        df.assign(host_label=x_label),
        x="host_label",
        y="avg_finish_pos",
        title="Average tournament finish position in events hosted in this country",
        labels={"host_label": "Tournament host country", "avg_finish_pos": "Avg finish position"},
        color="avg_finish_pos",
        color_continuous_scale="RdYlGn_r",
        color_continuous_midpoint=8,
    )
    fig_depth.update_layout(height=400, margin=dict(t=60), showlegend=False)
    fig_depth.update_xaxes(tickangle=-45)
    st.plotly_chart(fig_depth, use_container_width=True)

    with st.expander("Data table"):
        st.dataframe(df, use_container_width=True)
        st.caption("host_country = code, host_country_name = full name of tournament host.")
        st.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=download_filename,
            mime="text/csv",
            key=download_key,
        )


def _render_team_performance_tab(engine) -> None:
    st.subheader("Performance by tournament host country")
    st.caption("Wins/losses and average tournament finish position in tournaments hosted in each country.")
    st.info("**Country** = tournament host country (where the event was held).")

    teams = _team_list()
    if not teams:
        st.info("No teams in `core.dim_team_tournaments`. Run ETL and `dbt run` to populate core models.")
        return

    team = st.selectbox("Team", options=teams, index=0, help="Team display name (Player A / Player B)", key="team_sel")

    df = _performance_by_host_country(team)
    if df is None:
        st.error("Could not load performance data. Ensure core.fct_matches and core.fct_tournament_standings exist.")
        return
    if df.empty:
        st.warning(f"No matches found for **{team}**.")
        return

    _render_performance_charts(df, team, "perf_team_csv", f"performance_team_{team.replace(' / ', '_').replace(' ', '')[:40]}.csv")


def _render_player_performance_tab(engine) -> None:
    st.subheader("Performance by tournament host country")
    st.caption("Wins/losses and average tournament finish position in tournaments hosted in each country (player level).")
    st.info("**Country** = tournament host country (where the event was held).")

    players = _player_list()
    if not players:
        st.info("No players in `staging.stg_fivb_players`. Run ETL and `dbt run` to populate.")
        return

    # Show full_name in dropdown; store player_id for query
    player_options = [p[1] for p in players]
    player_id_by_name = {p[1]: p[0] for p in players}
    player_name = st.selectbox("Player", options=player_options, index=0, key="player_sel")
    player_id = player_id_by_name[player_name]

    df = _performance_by_host_country_player(player_id)
    if df is None:
        st.error("Could not load performance data. Ensure core models exist.")
        return
    if df.empty:
        st.warning(f"No matches found for **{player_name}**.")
        return

    _render_performance_charts(df, player_name, "perf_player_csv", f"performance_player_{player_name.replace(' ', '_')[:40]}.csv")


# Metrics from tournament_mart for the Performance over time tab (column key -> label)
PERFORMANCE_METRICS = {
    "finishing_pos": "Finishing position",
    "tournament_points": "Tournament points earned",
    "sum_opponent_points_beaten": "Sum opponent points beaten",
    "match_wins": "Match wins",
    "match_losses": "Match losses",
    "wins_vs_higher_seed": "Wins vs higher seed (upsets)",
    "losses_vs_lower_seed": "Losses vs lower seed",
    "pool_wins": "Pool wins",
    "elimination_wins": "Elimination wins",
    "quality_win_loss_score": "Quality win/loss score (by finish pos)",
    "quality_win_loss_score_points": "Quality win/loss score (by entry points)",
}


def _render_performance_over_time_tab(engine) -> None:
    st.subheader("Performance metrics across tournament time")
    st.caption("Track team performance over seasons using metrics from **mart.tournament_mart**. Each point is one tournament appearance. Select one or more teams and metrics to compare.")

    df = _tournament_mart_df()
    if df is None:
        st.error("Could not load **mart.tournament_mart**. Run `dbt run --select tournament_mart` to build it.")
        return
    if df.empty:
        st.warning("**mart.tournament_mart** is empty. Run ETL and dbt to populate.")
        return

    teams = df["team_display_name"].dropna().unique().tolist()
    teams_sorted = sorted([t for t in teams if t])
    if not teams_sorted:
        st.warning("No team names in tournament_mart.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        selected_teams = st.multiselect(
            "Teams",
            options=teams_sorted,
            default=teams_sorted[:1] if teams_sorted else [],
            help="Select one or more teams to compare.",
        )
    with c2:
        selected_metrics = st.multiselect(
            "Metrics",
            options=list(PERFORMANCE_METRICS.keys()),
            default=["finishing_pos", "quality_win_loss_score", "sum_opponent_points_beaten"],
            format_func=lambda k: PERFORMANCE_METRICS[k],
            help="Metrics to plot over time.",
        )
    with c3:
        time_axis = st.radio(
            "Time axis",
            options=["tournament_start_date", "season_year"],
            format_func=lambda x: "Tournament start date" if x == "tournament_start_date" else "Season year",
            horizontal=True,
        )
        filter_major = st.checkbox("Majors only (World Champs / Olympics)", value=False, key="perf_major")
        filter_gender = st.selectbox(
            "Gender",
            options=["All", "M", "W"],
            index=0,
            key="perf_gender",
        )

    if not selected_teams:
        st.info("Select at least one team in the sidebar.")
        return
    if not selected_metrics:
        st.info("Select at least one metric in the sidebar.")
        return

    sub = df[df["team_display_name"].isin(selected_teams)].copy()
    if filter_major:
        sub = sub[sub["tournament_is_major"] is True]
    if filter_gender != "All":
        sub = sub[sub["tournament_gender"] == filter_gender]

    if sub.empty:
        st.warning("No rows after filters. Try relaxing filters or choosing other teams.")
        return

    # Ensure time column for x-axis
    if time_axis == "season_year":
        sub = sub[sub["season_year"].notna()]
        sub = sub.sort_values(["season_year", "tournament_start_date", "team_display_name"])
        x_col = "season_year"
    else:
        sub = sub[sub["tournament_start_date"].notna()]
        sub = sub.sort_values(["tournament_start_date", "team_display_name"])
        x_col = "tournament_start_date"

    if sub.empty:
        st.warning("No rows with valid time axis. Check season_year / tournament_start_date.")
        return

    for metric in selected_metrics:
        if metric not in sub.columns:
            continue
        title = PERFORMANCE_METRICS.get(metric, metric)
        hover_cols = [c for c in ["tournament_name", "tournament_country_name", "tournament_country_code"] if c in sub.columns]
        fig = px.line(
            sub,
            x=x_col,
            y=metric,
            color="team_display_name",
            title=title,
            labels={x_col: "Season year" if x_col == "season_year" else "Tournament start", metric: title},
            markers=True,
            hover_data=hover_cols,
        )
        fig.update_layout(height=400, legend=dict(orientation="h", yanchor="bottom", y=1.02), margin=dict(t=50))
        if x_col == "season_year":
            fig.update_xaxes(dtick=1)
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("Data table (filtered)"):
        show_cols = [x_col, "team_display_name", "tournament_name", "season"] + [c for c in selected_metrics if c in sub.columns]
        show_cols = [c for c in show_cols if c in sub.columns]
        st.dataframe(sub[show_cols], use_container_width=True, height=300)
        st.download_button(
            label="Download filtered CSV",
            data=sub[show_cols].to_csv(index=False).encode("utf-8"),
            file_name="tournament_mart_performance_over_time.csv",
            mime="text/csv",
            key="perf_over_time_csv",
        )


def main() -> None:
    st.set_page_config(page_title="FIVB Leaderboard – Postgres", layout="wide")
    st.title("FIVB Leaderboard")

    engine = _engine()
    if engine is None:
        return

    tab_browser, tab_team, tab_player, tab_over_time = st.tabs(
        ["Table browser", "Team Performance", "Player Performance", "Performance over time"]
    )

    with tab_browser:
        _render_table_browser(engine)

    with tab_team:
        _render_team_performance_tab(engine)

    with tab_player:
        _render_player_performance_tab(engine)

    with tab_over_time:
        _render_performance_over_time_tab(engine)


if __name__ == "__main__":
    main()
