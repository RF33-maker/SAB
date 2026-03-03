-- ============================================================
-- Swish Assistant — Supabase SQL Views
-- Run this in the Supabase SQL Editor to create or update views.
-- All views are read-only and update automatically as data changes.
-- ============================================================


-- ============================================================
-- 1. v_player_game_log
--    Per-game traditional stats for every player, with readable
--    column aliases and game context joined from game_schedule.
-- ============================================================
CREATE OR REPLACE VIEW public.v_player_game_log AS
SELECT
    ps.id,
    ps.game_key,
    ps.league_id,
    ps.team_id,
    ps.player_id,
    ps.full_name                        AS player_name,
    ps.team_name,
    ps.side,
    gs.matchtime                        AS game_date,
    gs.hometeam,
    gs.awayteam,
    ps.shirtnumber                      AS jersey,
    ps.playingposition                  AS position,
    ps.starter,
    ps.sminutes                         AS min,
    ps.spoints                          AS pts,
    ps.sassists                         AS ast,
    ps.sreboundstotal                   AS reb,
    ps.sreboundsoffensive               AS oreb,
    ps.sreboundsdefensive               AS dreb,
    ps.ssteals                          AS stl,
    ps.sblocks                          AS blk,
    ps.sturnovers                       AS tov,
    ps.sfoulspersonal                   AS pf,
    ps.sfieldgoalsmade                  AS fgm,
    ps.sfieldgoalsattempted             AS fga,
    ps.sfieldgoalspercentage            AS fg_pct,
    ps.sthreepointersmade               AS tpm,
    ps.sthreepointersattempted          AS tpa,
    ps.sthreepointerspercentage         AS tp_pct,
    ps.stwopointersmade                 AS twom,
    ps.stwopointersattempted            AS twoa,
    ps.stwopointerspercentage           AS two_pct,
    ps.sfreethrowsmade                  AS ftm,
    ps.sfreethrowsattempted             AS fta,
    ps.sfreethrowspercentage            AS ft_pct,
    ps.splusminuspoints                 AS plus_minus,
    ps.spointsinthepaint                AS pitp,
    ps.spointsfastbreak                 AS fastbreak_pts,
    ps.spointssecondchance              AS second_chance_pts,
    ps.sblocksreceived                  AS blk_received,
    ps.sfoulson                         AS fouls_drawn
FROM public.player_stats ps
LEFT JOIN public.game_schedule gs ON gs.game_key = ps.game_key
ORDER BY gs.matchtime DESC NULLS LAST;


-- ============================================================
-- 2. v_player_season_averages
--    Per-player per-league season averages and totals.
--    Replaces the player_season_averages table.
-- ============================================================
CREATE OR REPLACE VIEW public.v_player_season_averages AS
SELECT
    ps.full_name                            AS player_name,
    ps.team_id,
    ps.team_name,
    ps.league_id,
    COUNT(*)                                AS games_played,
    ROUND(AVG(ps.spoints)::numeric, 1)      AS avg_pts,
    ROUND(AVG(ps.sassists)::numeric, 1)     AS avg_ast,
    ROUND(AVG(ps.sreboundstotal)::numeric, 1)   AS avg_reb,
    ROUND(AVG(ps.sreboundsoffensive)::numeric, 1) AS avg_oreb,
    ROUND(AVG(ps.sreboundsdefensive)::numeric, 1) AS avg_dreb,
    ROUND(AVG(ps.ssteals)::numeric, 1)      AS avg_stl,
    ROUND(AVG(ps.sblocks)::numeric, 1)      AS avg_blk,
    ROUND(AVG(ps.sturnovers)::numeric, 1)   AS avg_tov,
    ROUND(AVG(ps.sfoulspersonal)::numeric, 1) AS avg_pf,
    ROUND(AVG(ps.sfieldgoalsattempted)::numeric, 1) AS avg_fga,
    ROUND(AVG(ps.sfieldgoalsmade)::numeric, 1) AS avg_fgm,
    ROUND(AVG(ps.sthreepointersattempted)::numeric, 1) AS avg_tpa,
    ROUND(AVG(ps.sthreepointersmade)::numeric, 1) AS avg_tpm,
    ROUND(AVG(ps.sfreethrowsattempted)::numeric, 1) AS avg_fta,
    ROUND(AVG(ps.sfreethrowsmade)::numeric, 1) AS avg_ftm,
    ROUND(AVG(ps.splusminuspoints)::numeric, 1) AS avg_plus_minus,
    SUM(ps.spoints)                         AS total_pts,
    SUM(ps.sassists)                        AS total_ast,
    SUM(ps.sreboundstotal)                  AS total_reb,
    SUM(ps.ssteals)                         AS total_stl,
    SUM(ps.sblocks)                         AS total_blk,
    SUM(ps.sturnovers)                      AS total_tov,
    SUM(ps.sfieldgoalsmade)                 AS total_fgm,
    SUM(ps.sfieldgoalsattempted)            AS total_fga,
    CASE WHEN SUM(ps.sfieldgoalsattempted) > 0
        THEN ROUND((SUM(ps.sfieldgoalsmade)::numeric / SUM(ps.sfieldgoalsattempted)) * 100, 1)
        ELSE NULL END                        AS season_fg_pct,
    SUM(ps.sthreepointersmade)              AS total_tpm,
    SUM(ps.sthreepointersattempted)         AS total_tpa,
    CASE WHEN SUM(ps.sthreepointersattempted) > 0
        THEN ROUND((SUM(ps.sthreepointersmade)::numeric / SUM(ps.sthreepointersattempted)) * 100, 1)
        ELSE NULL END                        AS season_tp_pct,
    SUM(ps.sfreethrowsmade)                 AS total_ftm,
    SUM(ps.sfreethrowsattempted)            AS total_fta,
    CASE WHEN SUM(ps.sfreethrowsattempted) > 0
        THEN ROUND((SUM(ps.sfreethrowsmade)::numeric / SUM(ps.sfreethrowsattempted)) * 100, 1)
        ELSE NULL END                        AS season_ft_pct
FROM public.player_stats ps
GROUP BY ps.full_name, ps.team_id, ps.team_name, ps.league_id;


-- ============================================================
-- 3. v_player_advanced_game
--    Per-game advanced metrics for every player.
-- ============================================================
CREATE OR REPLACE VIEW public.v_player_advanced_game AS
SELECT
    ps.id,
    ps.game_key,
    ps.league_id,
    ps.team_id,
    ps.player_id,
    ps.full_name                    AS player_name,
    ps.team_name,
    gs.matchtime                    AS game_date,
    ps.sminutes                     AS min,
    ps.spoints                      AS pts,
    ps.efg_percent,
    ps.ts_percent,
    ps.usage_percent,
    ps.ast_percent,
    ps.oreb_percent,
    ps.dreb_percent,
    ps.reb_percent,
    ps.tov_percent,
    ps.pie,
    ps.off_rating,
    ps.def_rating,
    ps.net_rating,
    ps.three_point_rate,
    ps.player_possessions,
    ps.pts_percent_2pt,
    ps.pts_percent_3pt,
    ps.pts_percent_ft,
    ps.pts_percent_midrange,
    ps.pts_percent_pitp,
    ps.pts_percent_fastbreak,
    ps.pts_percent_second_chance,
    ps.pts_percent_off_turnovers
FROM public.player_stats ps
LEFT JOIN public.game_schedule gs ON gs.game_key = ps.game_key
ORDER BY gs.matchtime DESC NULLS LAST;


-- ============================================================
-- 4. v_team_game_log
--    Per-game traditional team stats with readable aliases
--    and game context from game_schedule.
-- ============================================================
CREATE OR REPLACE VIEW public.v_team_game_log AS
SELECT
    ts.id,
    ts.game_key,
    ts.league_id,
    ts.team_id,
    ts.name                             AS team_name,
    ts.side,
    gs.matchtime                        AS game_date,
    gs.hometeam,
    gs.awayteam,
    ts.score,
    ts.tot_spoints                      AS pts,
    ts.tot_sassists                     AS ast,
    ts.tot_sreboundstotal               AS reb,
    ts.tot_sreboundsoffensive           AS oreb,
    ts.tot_sreboundsdefensive           AS dreb,
    ts.tot_ssteals                      AS stl,
    ts.tot_sblocks                      AS blk,
    ts.tot_sturnovers                   AS tov,
    ts.tot_sfoulspersonal               AS pf,
    ts.tot_sfieldgoalsmade              AS fgm,
    ts.tot_sfieldgoalsattempted         AS fga,
    ts.tot_sfieldgoalspercentage        AS fg_pct,
    ts.tot_sthreepointersmade           AS tpm,
    ts.tot_sthreepointersattempted      AS tpa,
    ts.tot_sthreepointerspercentage     AS tp_pct,
    ts.tot_stwopointersmade             AS twom,
    ts.tot_stwopointersattempted        AS twoa,
    ts.tot_stwopointerspercentage       AS two_pct,
    ts.tot_sfreethrowsmade              AS ftm,
    ts.tot_sfreethrowsattempted         AS fta,
    ts.tot_sfreethrowspercentage        AS ft_pct,
    ts.tot_spointsinthepaint            AS pitp,
    ts.tot_spointsfastbreak             AS fastbreak_pts,
    ts.tot_spointssecondchance          AS second_chance_pts,
    ts.tot_spointsfromturnovers         AS pts_off_turnovers,
    ts.tot_sbenchpoints                 AS bench_pts,
    ts.p1_score,
    ts.p2_score,
    ts.p3_score,
    ts.p4_score
FROM public.team_stats ts
LEFT JOIN public.game_schedule gs ON gs.game_key = ts.game_key
ORDER BY gs.matchtime DESC NULLS LAST;


-- ============================================================
-- 5. v_team_season_averages
--    Per-team per-league season averages and totals.
-- ============================================================
CREATE OR REPLACE VIEW public.v_team_season_averages AS
SELECT
    ts.name                                 AS team_name,
    ts.team_id,
    ts.league_id,
    COUNT(*)                                AS games_played,
    ROUND(AVG(ts.tot_spoints)::numeric, 1)  AS avg_pts,
    ROUND(AVG(ts.tot_sassists)::numeric, 1) AS avg_ast,
    ROUND(AVG(ts.tot_sreboundstotal)::numeric, 1)  AS avg_reb,
    ROUND(AVG(ts.tot_ssteals)::numeric, 1)  AS avg_stl,
    ROUND(AVG(ts.tot_sblocks)::numeric, 1)  AS avg_blk,
    ROUND(AVG(ts.tot_sturnovers)::numeric, 1) AS avg_tov,
    ROUND(AVG(ts.tot_sfieldgoalsmade)::numeric, 1) AS avg_fgm,
    ROUND(AVG(ts.tot_sfieldgoalsattempted)::numeric, 1) AS avg_fga,
    CASE WHEN SUM(ts.tot_sfieldgoalsattempted) > 0
        THEN ROUND((SUM(ts.tot_sfieldgoalsmade)::numeric / SUM(ts.tot_sfieldgoalsattempted)) * 100, 1)
        ELSE NULL END                        AS season_fg_pct,
    ROUND(AVG(ts.tot_sthreepointersmade)::numeric, 1) AS avg_tpm,
    ROUND(AVG(ts.tot_sthreepointersattempted)::numeric, 1) AS avg_tpa,
    CASE WHEN SUM(ts.tot_sthreepointersattempted) > 0
        THEN ROUND((SUM(ts.tot_sthreepointersmade)::numeric / SUM(ts.tot_sthreepointersattempted)) * 100, 1)
        ELSE NULL END                        AS season_tp_pct,
    ROUND(AVG(ts.tot_sfreethrowsmade)::numeric, 1) AS avg_ftm,
    ROUND(AVG(ts.tot_sfreethrowsattempted)::numeric, 1) AS avg_fta,
    ROUND(AVG(ts.tot_spointsinthepaint)::numeric, 1) AS avg_pitp,
    ROUND(AVG(ts.tot_spointsfastbreak)::numeric, 1) AS avg_fastbreak_pts,
    ROUND(AVG(ts.tot_sbenchpoints)::numeric, 1) AS avg_bench_pts
FROM public.team_stats ts
GROUP BY ts.name, ts.team_id, ts.league_id;


-- ============================================================
-- 6. v_team_advanced_game
--    Per-game advanced team metrics.
-- ============================================================
CREATE OR REPLACE VIEW public.v_team_advanced_game AS
SELECT
    ts.id,
    ts.game_key,
    ts.league_id,
    ts.team_id,
    ts.name                         AS team_name,
    ts.side,
    gs.matchtime                    AS game_date,
    ts.possessions,
    ts.opp_possessions,
    ts.off_rating,
    ts.def_rating,
    ts.net_rating,
    ts.pace,
    ts.efg_percent,
    ts.ts_percent,
    ts.three_point_rate,
    ts.ft_rate,
    ts.tov_percent,
    ts.opp_tov_percent,
    ts.oreb_percent,
    ts.dreb_percent,
    ts.reb_percent,
    ts.opp_oreb_percent,
    ts.ast_percent,
    ts.ast_to_ratio,
    ts.pie,
    ts.opp_efg_percent,
    ts.opp_ft_rate,
    ts.fga_percent_2pt,
    ts.fga_percent_3pt,
    ts.fga_percent_midrange,
    ts.pts_percent_2pt,
    ts.pts_percent_3pt,
    ts.pts_percent_midrange,
    ts.pts_percent_pitp,
    ts.pts_percent_fastbreak,
    ts.pts_percent_second_chance,
    ts.pts_percent_off_turnovers,
    ts.opp_fgm,
    ts.opp_fga,
    ts.opp_3pm,
    ts.opp_points,
    ts.opp_turnovers
FROM public.team_stats ts
LEFT JOIN public.game_schedule gs ON gs.game_key = ts.game_key
ORDER BY gs.matchtime DESC NULLS LAST;


-- ============================================================
-- 7. v_league_leaders
--    Top players by category per league, with rank columns.
-- ============================================================
CREATE OR REPLACE VIEW public.v_league_leaders AS
SELECT
    player_name,
    team_id,
    team_name,
    league_id,
    games_played,
    avg_pts,
    avg_ast,
    avg_reb,
    avg_stl,
    avg_blk,
    avg_tov,
    season_fg_pct,
    season_tp_pct,
    season_ft_pct,
    total_pts,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_pts DESC)  AS pts_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_ast DESC)  AS ast_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_reb DESC)  AS reb_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_stl DESC)  AS stl_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_blk DESC)  AS blk_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY total_pts DESC) AS total_pts_rank
FROM public.v_player_season_averages
WHERE games_played >= 1;


-- ============================================================
-- 8. v_upcoming_games
--    Future scheduled games, ordered by soonest first.
-- ============================================================
CREATE OR REPLACE VIEW public.v_upcoming_games AS
SELECT
    game_key,
    league_id,
    home_team_id,
    away_team_id,
    hometeam,
    awayteam,
    matchtime                       AS game_date,
    competitionname                 AS competition,
    "LiveStats URL"                 AS livestats_url,
    pool,
    status
FROM public.game_schedule
WHERE matchtime > NOW()
ORDER BY matchtime ASC;


-- ============================================================
-- 9. v_recent_games
--    Past games, most recent first.
-- ============================================================
CREATE OR REPLACE VIEW public.v_recent_games AS
SELECT
    game_key,
    league_id,
    home_team_id,
    away_team_id,
    hometeam,
    awayteam,
    matchtime                       AS game_date,
    competitionname                 AS competition,
    "LiveStats URL"                 AS livestats_url,
    pool,
    status
FROM public.game_schedule
WHERE matchtime <= NOW()
ORDER BY matchtime DESC;
