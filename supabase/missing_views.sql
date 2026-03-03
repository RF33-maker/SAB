-- ============================================================
-- Swish Assistant — Missing Views Fix
-- Run this in the Supabase SQL Editor.
-- Creates only the 3 views that were not created in the
-- initial views.sql run (v_league_leaders, v_upcoming_games,
-- v_recent_games).
-- Requires v_player_season_averages to already exist.
-- ============================================================


-- ============================================================
-- 7. v_league_leaders
--    Top players by category per league, with rank columns.
--    Built on top of v_player_season_averages.
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
    RANK() OVER (PARTITION BY league_id ORDER BY avg_pts DESC)   AS pts_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_ast DESC)   AS ast_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_reb DESC)   AS reb_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_stl DESC)   AS stl_rank,
    RANK() OVER (PARTITION BY league_id ORDER BY avg_blk DESC)   AS blk_rank,
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
    matchtime                   AS game_date,
    competitionname             AS competition,
    "LiveStats URL"             AS livestats_url,
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
    matchtime                   AS game_date,
    competitionname             AS competition,
    "LiveStats URL"             AS livestats_url,
    pool,
    status
FROM public.game_schedule
WHERE matchtime <= NOW()
ORDER BY matchtime DESC;
