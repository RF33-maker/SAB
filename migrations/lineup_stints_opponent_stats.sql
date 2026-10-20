-- Migration: exact opponent stats on lineup stints
-- Created: 2026-10-20
-- Description: The lineup builder now counts what the opposing team did during
--              each stint as the events happen, instead of estimating it later
--              from overlapping opponent stints. That gives on/off an exact
--              "opposing team" view (their eFG%, rebounds, turnovers...).
--              lineup_stints_context_v1 gets the same columns, plus stint_id so a
--              context row can be traced to its stint. All additive and nullable.

alter table public.lineup_stints
  add column if not exists opp_fg2_made integer,
  add column if not exists opp_fg2_attempted integer,
  add column if not exists opp_fg3_made integer,
  add column if not exists opp_fg3_attempted integer,
  add column if not exists opp_ft_made integer,
  add column if not exists opp_ft_attempted integer,
  add column if not exists opp_oreb integer,
  add column if not exists opp_dreb integer,
  add column if not exists opp_assists integer,
  add column if not exists opp_turnovers integer,
  add column if not exists opp_steals integer,
  add column if not exists opp_blocks integer,
  add column if not exists opp_fouls integer;

-- opp_oreb, opp_dreb and opp_fg2_attempted already exist on the context table
-- (numeric, from the old overlap estimate).
alter table public.lineup_stints_context_v1
  add column if not exists stint_id uuid,
  add column if not exists opp_fg2_made numeric,
  add column if not exists opp_fg3_made numeric,
  add column if not exists opp_fg3_attempted numeric,
  add column if not exists opp_ft_made numeric,
  add column if not exists opp_ft_attempted numeric,
  add column if not exists opp_assists numeric,
  add column if not exists opp_turnovers numeric,
  add column if not exists opp_steals numeric,
  add column if not exists opp_blocks numeric,
  add column if not exists opp_fouls numeric;
