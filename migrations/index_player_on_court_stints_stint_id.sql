-- Migration: index player_on_court_stints.stint_id
-- Created: 2026-10-20
-- Description: Deleting a lineup_stints row cascades to player_on_court_stints
--              (stint_id ON DELETE CASCADE). Without an index every delete scanned
--              the whole table, so lineup_builder's per-game delete hit the
--              statement timeout and old stints were left beside the new ones.
--              Applied to the live project on 2026-10-20.

create index if not exists player_on_court_stints_stint_id_idx
  on public.player_on_court_stints (stint_id);
