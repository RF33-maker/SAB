-- Migration: PDF Ingestion Pipeline Tables
-- Created: 2026-03-26
-- Description: Adds new tables and columns for Genius Sports post-game PDF exports
--              and fixes incorrect JSON key mappings in team_stats.

-- ========================================
-- TEAM STATS - Fix & Add Columns
-- ========================================

-- Fix: columns already exist under old incorrect names, add correctly-named ones
-- (The TEAM_FIELD_MAP will now use the corrected JSON keys, same DB columns)
-- The existing tot_timeleading, tot_biggestscoringrun, tot_leadchanges,
-- tot_timesscoreslevel columns stay — only the Python-side JSON key lookup is fixed.

-- New: tot_sBiggestLead (was entirely missing from TEAM_FIELD_MAP)
-- Applied to both public and test schemas
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sbiggestlead text;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sbiggestlead text;

-- New: unmapped team fields
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sfoulson numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sfoulstotal numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sfoulsteam numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sreboundsteam numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sreboundsteamdefensive numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sreboundsteamoffensive numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_sturnovers_team numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_1 numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_2 numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_3 numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_4 numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_5 numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_6 numeric;
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS tot_eff_7 numeric;

ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sfoulson numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sfoulstotal numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sfoulsteam numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sreboundsteam numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sreboundsteamdefensive numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sreboundsteamoffensive numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_sturnovers_team numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_1 numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_2 numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_3 numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_4 numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_5 numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_6 numeric;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS tot_eff_7 numeric;

-- Game leaders JSON
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS game_leaders_json jsonb;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS game_leaders_json jsonb;

-- Source tag (team_stats)
ALTER TABLE public.team_stats ADD COLUMN IF NOT EXISTS source_type text;
ALTER TABLE test.team_stats ADD COLUMN IF NOT EXISTS source_type text;

-- ========================================
-- PLAYER STATS - Add PDF-specific columns
-- ========================================

-- source_type: tracks whether row came from 'json' or 'pdf'
ALTER TABLE public.player_stats ADD COLUMN IF NOT EXISTS source_type text;
ALTER TABLE test.player_stats ADD COLUMN IF NOT EXISTS source_type text;

-- starter: whether the player started the game (PDF box score only)
ALTER TABLE public.player_stats ADD COLUMN IF NOT EXISTS starter boolean;
ALTER TABLE test.player_stats ADD COLUMN IF NOT EXISTS starter boolean;

-- eff_1: efficiency stat from PDF box score (rightmost column before PTS)
ALTER TABLE public.player_stats ADD COLUMN IF NOT EXISTS eff_1 integer;
ALTER TABLE test.player_stats ADD COLUMN IF NOT EXISTS eff_1 integer;

-- sfoulson: fouls drawn (FD column in PDF box score)
ALTER TABLE public.player_stats ADD COLUMN IF NOT EXISTS sfoulson integer;
ALTER TABLE test.player_stats ADD COLUMN IF NOT EXISTS sfoulson integer;

-- ========================================
-- LIVE EVENTS - Add score_diff for PDF PBP
-- Applied to both schemas
-- ========================================

ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS score_diff integer;
ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS score_diff integer;

-- ========================================
-- GAME SCHEDULE - Attendance & Officials
-- Applied to both schemas
-- ========================================

ALTER TABLE public.game_schedule ADD COLUMN IF NOT EXISTS attendance integer;
ALTER TABLE public.game_schedule ADD COLUMN IF NOT EXISTS officials jsonb;
ALTER TABLE test.game_schedule ADD COLUMN IF NOT EXISTS attendance integer;
ALTER TABLE test.game_schedule ADD COLUMN IF NOT EXISTS officials jsonb;

-- Final score columns — used by collision detection to distinguish games that
-- share the same Game No. but produced different results (organiser error).
ALTER TABLE public.game_schedule ADD COLUMN IF NOT EXISTS home_score integer;
ALTER TABLE public.game_schedule ADD COLUMN IF NOT EXISTS away_score integer;
ALTER TABLE test.game_schedule ADD COLUMN IF NOT EXISTS home_score integer;
ALTER TABLE test.game_schedule ADD COLUMN IF NOT EXISTS away_score integer;

-- ========================================
-- LINEUP STATS (PDF: Line Up Analysis)
-- Created in BOTH public and test schemas so the PDF parser (test schema)
-- and any production queries (public schema) can use the same table shape.
-- ========================================

CREATE TABLE IF NOT EXISTS public.lineup_stats (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid REFERENCES leagues(league_id),
    team_id             uuid REFERENCES teams(team_id),
    team_name           text,
    lineup              text NOT NULL,
    time_on_court       text,
    score               text,
    score_diff          integer,
    pts_per_min         numeric,
    rebounds            integer,
    steals              integer,
    turnovers           integer,
    assists             integer,
    source_type         text DEFAULT 'pdf',
    identifier_duplicate text UNIQUE,
    created_at          timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS test.lineup_stats (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid,
    team_id             uuid,
    team_name           text,
    lineup              text NOT NULL,
    time_on_court       text,
    score               text,
    score_diff          integer,
    pts_per_min         numeric,
    rebounds            integer,
    steals              integer,
    turnovers           integer,
    assists             integer,
    source_type         text DEFAULT 'pdf',
    identifier_duplicate text UNIQUE,
    created_at          timestamptz DEFAULT now()
);

-- ========================================
-- PLAYER PLUS/MINUS (PDF: Player Plus/Minus Summary)
-- Created in BOTH public and test schemas.
-- ========================================

CREATE TABLE IF NOT EXISTS public.player_plus_minus (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid REFERENCES leagues(league_id),
    team_id             uuid REFERENCES teams(team_id),
    player_id           uuid REFERENCES players(id),
    full_name           text,
    player_name         text GENERATED ALWAYS AS (full_name) STORED,
    shirt_number        text,
    team_name           text,
    mins_on             text,
    mins_off            text,
    score_on            text,
    score_off           text,
    pts_diff_on         integer,
    pts_diff_off        integer,
    pts_per_min_on      numeric,
    pts_per_min_off     numeric,
    assists_on          integer,
    assists_off         integer,
    rebounds_on         integer,
    rebounds_off        integer,
    steals_on           integer,
    steals_off          integer,
    turnovers_on        integer,
    turnovers_off       integer,
    source_type         text DEFAULT 'pdf',
    identifier_duplicate text UNIQUE,
    created_at          timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS test.player_plus_minus (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid,
    team_id             uuid,
    player_id           uuid,
    full_name           text,
    player_name         text GENERATED ALWAYS AS (full_name) STORED,
    shirt_number        text,
    team_name           text,
    mins_on             text,
    mins_off            text,
    score_on            text,
    score_off           text,
    pts_diff_on         integer,
    pts_diff_off        integer,
    pts_per_min_on      numeric,
    pts_per_min_off     numeric,
    assists_on          integer,
    assists_off         integer,
    rebounds_on         integer,
    rebounds_off        integer,
    steals_on           integer,
    steals_off          integer,
    turnovers_on        integer,
    turnovers_off       integer,
    source_type         text DEFAULT 'pdf',
    identifier_duplicate text UNIQUE,
    created_at          timestamptz DEFAULT now()
);

-- ========================================
-- ROTATIONS SUMMARY (PDF: Rotations Summary)
-- Created in BOTH public and test schemas.
-- ========================================

CREATE TABLE IF NOT EXISTS public.rotations_summary (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid REFERENCES leagues(league_id),
    team_id             uuid REFERENCES teams(team_id),
    team_name           text,
    lineup              text NOT NULL,
    quarter_on          integer,
    time_on             text,
    quarter_off         integer,
    time_off            text,
    time_on_court       text,
    score               text,
    score_diff          integer,
    rebounds            integer,
    steals              integer,
    turnovers           integer,
    assists             integer,
    source_type         text DEFAULT 'pdf',
    identifier_duplicate text UNIQUE,
    created_at          timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS test.rotations_summary (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid,
    team_id             uuid,
    team_name           text,
    lineup              text NOT NULL,
    quarter_on          integer,
    time_on             text,
    quarter_off         integer,
    time_off            text,
    time_on_court       text,
    score               text,
    score_diff          integer,
    rebounds            integer,
    steals              integer,
    turnovers           integer,
    assists             integer,
    source_type         text DEFAULT 'pdf',
    identifier_duplicate text UNIQUE,
    created_at          timestamptz DEFAULT now()
);

-- Migration complete
