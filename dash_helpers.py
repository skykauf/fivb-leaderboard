"""
Dashboard helpers: data and rendering for team/player performance and over-time views.

Use from a Streamlit app (or other dashboard) by passing the streamlit module and engine.
Example:
  from dash_helpers import get_team_list, render_team_performance_tab
  teams = get_team_list(engine)
  render_team_performance_tab(st, engine)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    import streamlit

# Metrics from tournament_mart for performance-over-time (column key -> label)
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


# ---- Data (no Streamlit dependency) ----

def get_team_list(engine: Engine) -> list[str]:
    """Distinct team display names from core.dim_team_tournaments."""
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


def get_player_list(engine: Engine) -> list[tuple[int, str]]:
    """(player_id, full_name) from staging.stg_fivb_players."""
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


def get_performance_by_host_country(engine: Engine, team_display_name: str) -> pd.DataFrame | None:
    """Wins, losses, and average tournament finish position by tournament host country (team)."""
    try:
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


def get_performance_metrics_by_host_country_team(engine: Engine, team_display_name: str) -> pd.DataFrame | None:
    """Wins, losses, avg finish pos, and averaged performance metrics by host country (from mart.tournament_mart)."""
    try:
        q = text("""
            SELECT
                tournament_country_code AS host_country,
                tournament_country_name AS host_country_name,
                SUM(match_wins)::int AS wins,
                SUM(match_losses)::int AS losses,
                SUM(match_wins) + SUM(match_losses) AS total_matches,
                ROUND(AVG(finishing_pos)::numeric, 2) AS avg_finish_pos,
                ROUND(AVG(quality_win_loss_score)::numeric, 4) AS avg_quality_win_loss_score,
                ROUND(AVG(quality_win_loss_score_points)::numeric, 4) AS avg_quality_win_loss_score_points,
                ROUND(AVG(sum_opponent_points_beaten)::numeric, 2) AS avg_sum_opponent_points_beaten
            FROM mart.tournament_mart
            WHERE team_display_name = :team
            GROUP BY tournament_country_code, tournament_country_name
            ORDER BY wins DESC, total_matches DESC, host_country
        """)
        with engine.connect() as conn:
            return pd.read_sql(q, conn, params={"team": team_display_name})
    except Exception:
        return None


def get_performance_metrics_by_host_country_player(engine: Engine, player_id: int) -> pd.DataFrame | None:
    """Wins, losses, avg finish pos, and averaged performance metrics by host country for a player (from mart.tournament_mart)."""
    try:
        q = text("""
            SELECT
                tm.tournament_country_code AS host_country,
                tm.tournament_country_name AS host_country_name,
                SUM(tm.match_wins)::int AS wins,
                SUM(tm.match_losses)::int AS losses,
                SUM(tm.match_wins) + SUM(tm.match_losses) AS total_matches,
                ROUND(AVG(tm.finishing_pos)::numeric, 2) AS avg_finish_pos,
                ROUND(AVG(tm.quality_win_loss_score)::numeric, 4) AS avg_quality_win_loss_score,
                ROUND(AVG(tm.quality_win_loss_score_points)::numeric, 4) AS avg_quality_win_loss_score_points,
                ROUND(AVG(tm.sum_opponent_points_beaten)::numeric, 2) AS avg_sum_opponent_points_beaten
            FROM mart.tournament_mart tm
            JOIN core.dim_team_tournaments dtt ON dtt.team_id = tm.team_id AND dtt.tournament_id = tm.tournament_id
            WHERE dtt.player_a_id = :player_id OR dtt.player_b_id = :player_id
            GROUP BY tm.tournament_country_code, tm.tournament_country_name
            ORDER BY wins DESC, total_matches DESC, host_country
        """)
        with engine.connect() as conn:
            return pd.read_sql(q, conn, params={"player_id": player_id})
    except Exception:
        return None


def get_performance_by_host_country_player(engine: Engine, player_id: int) -> pd.DataFrame | None:
    """Wins, losses, and average tournament finish position by host country (player level)."""
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


def get_tournament_mart_df(engine: Engine) -> pd.DataFrame | None:
    """Load mart.tournament_mart for performance-over-time charts."""
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


# ---- Rendering (requires streamlit) ----

def render_performance_charts(
    st: "streamlit.delta_generator.DeltaGenerator",
    df: pd.DataFrame,
    entity_label: str,
    download_key: str,
    download_filename: str,
) -> None:
    """Render wins/losses, avg finish pos, and quality score charts + data table (team or player)."""
    import plotly.express as px
    import plotly.graph_objects as go

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
    st.plotly_chart(fig_wl, width="stretch")

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
    st.plotly_chart(fig_depth, width="stretch")

    if "avg_quality_win_loss_score" in df.columns and df["avg_quality_win_loss_score"].notna().any():
        fig_qwl = px.bar(
            df.assign(host_label=x_label),
            x="host_label",
            y="avg_quality_win_loss_score",
            title="Average quality win/loss score by host country",
            labels={"host_label": "Tournament host country", "avg_quality_win_loss_score": "Avg quality win/loss score"},
        )
        fig_qwl.update_layout(height=400, margin=dict(t=60), showlegend=False)
        fig_qwl.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_qwl, width="stretch")

    if "avg_quality_win_loss_score_points" in df.columns and df["avg_quality_win_loss_score_points"].notna().any():
        fig_qwl_p = px.bar(
            df.assign(host_label=x_label),
            x="host_label",
            y="avg_quality_win_loss_score_points",
            title="Average quality win/loss score (points-based) by host country",
            labels={"host_label": "Tournament host country", "avg_quality_win_loss_score_points": "Avg quality score (points)"},
        )
        fig_qwl_p.update_layout(height=400, margin=dict(t=60), showlegend=False)
        fig_qwl_p.update_xaxes(tickangle=-45)
        st.plotly_chart(fig_qwl_p, width="stretch")

    with st.expander("Data table"):
        st.dataframe(df, width="stretch")
        st.caption("host_country = code, host_country_name = full name of tournament host.")
        st.download_button(
            label="Download as CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=download_filename,
            mime="text/csv",
            key=download_key,
        )


def render_team_performance_tab(st: "streamlit.delta_generator.DeltaGenerator", engine: Engine) -> None:
    """Render Team Performance by host country tab."""
    st.subheader("Performance by tournament host country")
    st.caption("Wins/losses, average finish position, and performance metrics (averaged by country) in tournaments hosted in each country.")
    st.info("**Country** = tournament host country (where the event was held).")

    teams = get_team_list(engine)
    if not teams:
        st.info("No teams in `core.dim_team_tournaments`. Run ETL and `dbt run` to populate core models.")
        return

    team = st.selectbox("Team", options=teams, index=0, help="Team display name (Player A / Player B)", key="team_sel")

    df = get_performance_metrics_by_host_country_team(engine, team)
    if df is None:
        df = get_performance_by_host_country(engine, team)
    if df is None:
        st.error("Could not load performance data. Ensure core models and optionally mart.tournament_mart exist.")
        return
    if df.empty:
        st.warning(f"No matches found for **{team}**.")
        return

    render_performance_charts(st, df, team, "perf_team_csv", f"performance_team_{team.replace(' / ', '_').replace(' ', '')[:40]}.csv")


def render_player_performance_tab(st: "streamlit.delta_generator.DeltaGenerator", engine: Engine) -> None:
    """Render Player Performance by host country tab."""
    st.subheader("Performance by tournament host country")
    st.caption("Wins/losses, average finish position, and performance metrics (averaged by country) in tournaments hosted in each country (player level).")
    st.info("**Country** = tournament host country (where the event was held).")

    players = get_player_list(engine)
    if not players:
        st.info("No players in `staging.stg_fivb_players`. Run ETL and `dbt run` to populate.")
        return

    player_options = [p[1] for p in players]
    player_id_by_name = {p[1]: p[0] for p in players}
    player_name = st.selectbox("Player", options=player_options, index=0, key="player_sel")
    player_id = player_id_by_name[player_name]

    df = get_performance_metrics_by_host_country_player(engine, player_id)
    if df is None:
        df = get_performance_by_host_country_player(engine, player_id)
    if df is None:
        st.error("Could not load performance data. Ensure core models and optionally mart.tournament_mart exist.")
        return
    if df.empty:
        st.warning(f"No matches found for **{player_name}**.")
        return

    render_performance_charts(st, df, player_name, "perf_player_csv", f"performance_player_{player_name.replace(' ', '_')[:40]}.csv")


def render_performance_over_time_tab(st: "streamlit.delta_generator.DeltaGenerator", engine: Engine) -> None:
    """Render Performance over time tab (tournament_mart metrics)."""
    import plotly.express as px

    st.subheader("Performance metrics across tournament time")
    st.caption("Track team performance over seasons using metrics from **mart.tournament_mart**. Each point is one tournament appearance. Select one or more teams and metrics to compare.")

    df = get_tournament_mart_df(engine)
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
        st.plotly_chart(fig, width="stretch")

    with st.expander("Data table (filtered)"):
        show_cols = [x_col, "team_display_name", "tournament_name", "season"] + [c for c in selected_metrics if c in sub.columns]
        show_cols = [c for c in show_cols if c in sub.columns]
        st.dataframe(sub[show_cols], width="stretch", height=300)
        st.download_button(
            label="Download filtered CSV",
            data=sub[show_cols].to_csv(index=False).encode("utf-8"),
            file_name="tournament_mart_performance_over_time.csv",
            mime="text/csv",
            key="perf_over_time_csv",
        )
