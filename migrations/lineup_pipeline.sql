-- Migration: Lineup Analytics Pipeline
-- Created: 2026-04-22
-- Description: Adds new columns to live_events for lineup reconstruction,
--              creates game_rosters, lineup_stints, and player_on_court_stints tables.
--              Apply to both public and test schemas.

-- ========================================
-- LIVE EVENTS - Add lineup reconstruction fields
-- ========================================

ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS shirt_number  text;
ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS pno           integer;
ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS period_type   text;
ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS previous_action integer;
ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS team_score    integer;
ALTER TABLE public.live_events ADD COLUMN IF NOT EXISTS opp_score     integer;

ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS shirt_number    text;
ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS pno             integer;
ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS period_type     text;
ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS previous_action integer;
ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS team_score      integer;
ALTER TABLE test.live_events ADD COLUMN IF NOT EXISTS opp_score       integer;

-- ========================================
-- GAME ROSTERS
-- Populated during game parsing from tm[side].pl roster data.
-- One row per player per game. Enables starting-five identification
-- and shirt_number / pno lookups during lineup reconstruction.
-- ========================================

CREATE TABLE IF NOT EXISTS public.game_rosters (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid REFERENCES public.leagues(league_id),
    team_id             uuid REFERENCES public.teams(team_id),
    team_no             text,
    player_name         text,
    shirt_number        text,
    pno                 integer,
    starter             boolean DEFAULT false,
    active              boolean DEFAULT true,
    player_id           uuid REFERENCES public.players(id),
    created_at          timestamptz DEFAULT now(),
    UNIQUE (game_key, team_id, shirt_number)
);

CREATE TABLE IF NOT EXISTS test.game_rosters (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid,
    team_id             uuid,
    team_no             text,
    player_name         text,
    shirt_number        text,
    pno                 integer,
    starter             boolean DEFAULT false,
    active              boolean DEFAULT true,
    player_id           uuid,
    created_at          timestamptz DEFAULT now(),
    UNIQUE (game_key, team_id, shirt_number)
);

-- ========================================
-- LINEUP STINTS
-- One row per continuous 5-man lineup period for one team in one game.
-- lineup_key: deterministic identifier for the 5-man group.
-- Possession columns default to 0, nullable, ready for future computation.
-- ========================================

CREATE TABLE IF NOT EXISTS public.lineup_stints (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid REFERENCES public.leagues(league_id),
    team_id             uuid REFERENCES public.teams(team_id),
    lineup_key          text NOT NULL,
    lineup_player_ids   text[],
    lineup_names        text[],
    period              integer,
    start_action        integer,
    end_action          integer,
    start_clock         text,
    end_clock           text,
    start_game_secs     integer,
    end_game_secs       integer,
    seconds_played      integer DEFAULT 0,
    points_for          integer DEFAULT 0,
    points_against      integer DEFAULT 0,
    fg2_made            integer DEFAULT 0,
    fg2_attempted       integer DEFAULT 0,
    fg3_made            integer DEFAULT 0,
    fg3_attempted       integer DEFAULT 0,
    ft_made             integer DEFAULT 0,
    ft_attempted        integer DEFAULT 0,
    oreb                integer DEFAULT 0,
    dreb                integer DEFAULT 0,
    assists             integer DEFAULT 0,
    turnovers           integer DEFAULT 0,
    fouls               integer DEFAULT 0,
    steals              integer DEFAULT 0,
    blocks              integer DEFAULT 0,
    possessions_for     integer DEFAULT 0,
    possessions_against integer DEFAULT 0,
    -- True when exactly 5 players are in the lineup; false for degraded stints
    -- (e.g. data gaps, unresolved substitutions). Consumers should filter on this.
    is_valid_lineup     boolean DEFAULT true,
    created_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS lineup_stints_game_key_idx ON public.lineup_stints (game_key);
CREATE INDEX IF NOT EXISTS lineup_stints_lineup_key_idx ON public.lineup_stints (lineup_key);
CREATE INDEX IF NOT EXISTS lineup_stints_team_id_idx ON public.lineup_stints (team_id);

CREATE TABLE IF NOT EXISTS test.lineup_stints (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    game_key            text NOT NULL,
    league_id           uuid,
    team_id             uuid,
    lineup_key          text NOT NULL,
    lineup_player_ids   text[],
    lineup_names        text[],
    period              integer,
    start_action        integer,
    end_action          integer,
    start_clock         text,
    end_clock           text,
    start_game_secs     integer,
    end_game_secs       integer,
    seconds_played      integer DEFAULT 0,
    points_for          integer DEFAULT 0,
    points_against      integer DEFAULT 0,
    fg2_made            integer DEFAULT 0,
    fg2_attempted       integer DEFAULT 0,
    fg3_made            integer DEFAULT 0,
    fg3_attempted       integer DEFAULT 0,
    ft_made             integer DEFAULT 0,
    ft_attempted        integer DEFAULT 0,
    oreb                integer DEFAULT 0,
    dreb                integer DEFAULT 0,
    assists             integer DEFAULT 0,
    turnovers           integer DEFAULT 0,
    fouls               integer DEFAULT 0,
    steals              integer DEFAULT 0,
    blocks              integer DEFAULT 0,
    possessions_for     integer DEFAULT 0,
    possessions_against integer DEFAULT 0,
    is_valid_lineup     boolean DEFAULT true,
    created_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS test_lineup_stints_game_key_idx ON test.lineup_stints (game_key);
CREATE INDEX IF NOT EXISTS test_lineup_stints_lineup_key_idx ON test.lineup_stints (lineup_key);

-- ========================================
-- PLAYER ON COURT STINTS
-- One row per player per lineup stint.
-- Carries over seconds_played, points_for, points_against from the parent stint.
-- Used for player on/off analysis.
-- ========================================

CREATE TABLE IF NOT EXISTS public.player_on_court_stints (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    stint_id            uuid REFERENCES public.lineup_stints(id) ON DELETE CASCADE,
    game_key            text NOT NULL,
    league_id           uuid REFERENCES public.leagues(league_id),
    team_id             uuid REFERENCES public.teams(team_id),
    player_id           uuid REFERENCES public.players(id),
    player_name         text,
    shirt_number        text,
    lineup_key          text NOT NULL,
    period              integer,
    start_game_secs     integer,
    end_game_secs       integer,
    seconds_played      integer DEFAULT 0,
    points_for          integer DEFAULT 0,
    points_against      integer DEFAULT 0,
    possessions_for     integer DEFAULT 0,
    possessions_against integer DEFAULT 0,
    created_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS player_on_court_stints_game_key_idx ON public.player_on_court_stints (game_key);
CREATE INDEX IF NOT EXISTS player_on_court_stints_player_id_idx ON public.player_on_court_stints (player_id);
CREATE INDEX IF NOT EXISTS player_on_court_stints_lineup_key_idx ON public.player_on_court_stints (lineup_key);

CREATE TABLE IF NOT EXISTS test.player_on_court_stints (
    id                  uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    stint_id            uuid,
    game_key            text NOT NULL,
    league_id           uuid,
    team_id             uuid,
    player_id           uuid,
    player_name         text,
    shirt_number        text,
    lineup_key          text NOT NULL,
    period              integer,
    start_game_secs     integer,
    end_game_secs       integer,
    seconds_played      integer DEFAULT 0,
    points_for          integer DEFAULT 0,
    points_against      integer DEFAULT 0,
    possessions_for     integer DEFAULT 0,
    possessions_against integer DEFAULT 0,
    created_at          timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS test_player_on_court_stints_game_key_idx ON test.player_on_court_stints (game_key);
CREATE INDEX IF NOT EXISTS test_player_on_court_stints_player_id_idx ON test.player_on_court_stints (player_id);

-- Migration complete
