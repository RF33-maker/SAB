#!/usr/bin/env python3
"""
backfill_lineups.py
-------------------
Backfill lineup stints for all (or a subset of) existing games.

Usage examples:
  # Process all games
  python -m app.backfill_lineups

  # Dry-run (compute stints but don't write to DB)
  python -m app.backfill_lineups --dry-run

  # Single game
  python -m app.backfill_lineups --game-key 2025-01-10_TeamA_vs_TeamB

  # Filter by league UUID
  python -m app.backfill_lineups --league-id <uuid>

  # Reprocess even already-processed games
  python -m app.backfill_lineups --force
"""

import argparse
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("backfill_lineups")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")


def get_db():
    from supabase import create_client
    from supabase.lib.client_options import ClientOptions
    return create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
        options=ClientOptions(schema=DB_SCHEMA),
    )


def fetch_games(db, league_id: str = None, game_key: str = None) -> list:
    """
    Return games from game_schedule that have a LiveStats URL.
    Optionally filter by league_id or a single game_key.
    """
    q = db.table("game_schedule").select(
        'game_key, league_id, "LiveStats URL"'
    )
    if game_key:
        q = q.eq("game_key", game_key)
    elif league_id:
        q = q.eq("league_id", league_id)

    res = q.execute()
    return [g for g in res.data if g.get("LiveStats URL")]


def game_already_processed(db, game_key: str) -> bool:
    """
    Return True if lineup_stints already contains rows for this game.
    Used to skip already-processed games unless --force is specified.
    """
    try:
        res = (
            db.table("lineup_stints")
            .select("id")
            .eq("game_key", game_key)
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception as exc:
        log.warning("Could not check lineup_stints for %s: %s", game_key, exc)
        return False


def run_backfill(
    game_key: str = None,
    league_id: str = None,
    dry_run: bool = False,
    force: bool = False,
):
    """
    Main entry point for the backfill.

    Parameters
    ----------
    game_key   : str   Process only this game.
    league_id  : str   Filter to this league UUID.
    dry_run    : bool  Compute stints but skip all DB writes.
    force      : bool  Reprocess games even if stints already exist.
    """
    from app.utils.lineup_builder import build_lineups_for_game

    db = get_db()

    games = fetch_games(db, league_id=league_id, game_key=game_key)
    log.info("Found %d games to process", len(games))

    processed = 0
    skipped = 0
    errors = 0

    for game in games:
        gk = game["game_key"]
        lid = game["league_id"]

        if not lid:
            log.warning("game=%s has no league_id, skipping", gk)
            skipped += 1
            continue

        if not force and not dry_run and game_already_processed(db, gk):
            log.info("game=%s already processed, skipping (use --force to reprocess)", gk)
            skipped += 1
            continue

        log.info("Processing game=%s", gk)
        try:
            ok = build_lineups_for_game(
                game_key=gk,
                league_id=lid,
                dry_run=dry_run,
            )
            if ok:
                processed += 1
            else:
                skipped += 1
        except Exception as exc:
            log.error("Failed to build lineups for game=%s: %s", gk, exc, exc_info=True)
            errors += 1

    print("\n" + "=" * 60)
    print("Lineup backfill complete")
    print(f"  Processed : {processed}")
    print(f"  Skipped   : {skipped}")
    print(f"  Errors    : {errors}")
    print(f"  Dry-run   : {dry_run}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Backfill lineup stints for existing games"
    )
    parser.add_argument(
        "--game-key",
        dest="game_key",
        default=None,
        help="Process only this single game key",
    )
    parser.add_argument(
        "--league-id",
        dest="league_id",
        default=None,
        help="Filter to games in this league UUID",
    )
    parser.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=False,
        help="Compute stints but do NOT write to the database",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Reprocess games even if lineup_stints rows already exist",
    )
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment")
        sys.exit(1)

    run_backfill(
        game_key=args.game_key,
        league_id=args.league_id,
        dry_run=args.dry_run,
        force=args.force,
    )


if __name__ == "__main__":
    main()
