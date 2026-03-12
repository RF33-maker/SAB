-- ============================================================
-- Swish Assistant — shot_chart Schema Migration
-- Run this in the Supabase SQL Editor.
-- Adds player_id and action_number columns to shot_chart,
-- and creates a unique index to prevent duplicate inserts
-- when the parser re-processes the same game.
-- ============================================================

ALTER TABLE public.shot_chart
    ADD COLUMN IF NOT EXISTS player_id uuid NULL,
    ADD COLUMN IF NOT EXISTS action_number integer NULL;

CREATE UNIQUE INDEX IF NOT EXISTS shot_chart_game_action_uniq
    ON public.shot_chart (game_key, action_number);
