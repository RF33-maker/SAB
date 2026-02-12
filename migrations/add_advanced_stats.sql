-- Migration: Add Advanced Stats Columns
-- Created: 2025-11-19
-- Description: Adds advanced basketball analytics columns to team_stats and player_stats tables

-- ========================================
-- TEAM STATS - Advanced Analytics Columns
-- ========================================

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS possessions numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_possessions numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS off_rating numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS def_rating numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS net_rating numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pace numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS efg_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS ts_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS three_point_rate numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS ft_rate numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS tov_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_tov_percent numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS oreb_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS dreb_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS reb_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_oreb_percent numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS ast_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS ast_to_ratio numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pie numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_efg_percent numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_ft_rate numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS fga_percent_2pt numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS fga_percent_3pt numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS fga_percent_midrange numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_2pt numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_3pt numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_midrange numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_pitp numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_fastbreak numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_second_chance numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_off_turnovers numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS pts_percent_ft numeric;

ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_fgm numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_fga numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_3pm numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_points numeric;
ALTER TABLE team_stats ADD COLUMN IF NOT EXISTS opp_turnovers numeric;

-- ========================================
-- PLAYER STATS - Advanced Analytics Columns
-- ========================================

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS efg_percent numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS ts_percent numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS three_point_rate numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS usage_percent numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS player_possessions numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS ast_percent numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS ast_to_ratio numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS oreb_percent numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS dreb_percent numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS reb_percent numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS tov_percent numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS off_rating numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS def_rating numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS net_rating numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pie numeric;

ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_2pt numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_3pt numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_midrange numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_pitp numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_fastbreak numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_second_chance numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_off_turnovers numeric;
ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS pts_percent_ft numeric;

-- Migration complete
