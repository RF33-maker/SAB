-- Fingerprint of the LiveStats feed as last parsed for a game. After full time the
-- worker re-checks the feed (scorers correct stats after the whistle) and only
-- re-parses when the fingerprint changes. Nullable: a game with no fingerprint is
-- re-parsed once on its first post-game re-check.
ALTER TABLE public.game_schedule
  ADD COLUMN IF NOT EXISTS last_fingerprint text;
