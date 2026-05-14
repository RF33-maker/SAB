#!/usr/bin/env python3
"""
backfill_lineup_context_existing.py
------------------------------------
Backfill lineup_stints_context_v1 for an existing league by processing one
game at a time to avoid timeouts on large datasets (10k+ stints).

Uses the Supabase REST client (SUPABASE_URL + SUPABASE_KEY) — no direct
Postgres connection required.

Uses a tracking table (lineup_context_backfill_status) to record progress
and allow safe resumption after failures.

lineup_stints_context_v1 must already exist in Supabase before running.

Usage examples:

  # Check progress for a league
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --status

  # Process the next 10 pending games (ordered by ascending stint count)
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --limit 10

  # Process a single specific game
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --game-key <KEY>

  # Reset failed games to pending, then process up to 25
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --limit 25 --reset-failed
"""

import argparse
import logging
import os
import sys
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("backfill_lineup_context")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

def get_db():
    from supabase import create_client
    from supabase.lib.client_options import ClientOptions
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL and SUPABASE_KEY must be set.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY,
                         options=ClientOptions(schema="public"))


# ---------------------------------------------------------------------------
# Tracking table helpers
# ---------------------------------------------------------------------------

def ensure_tracking_rows(db, league_id: str):
    """
    Discover all game_keys in lineup_stints for the league and insert
    'pending' tracking rows for any that don't have one yet.
    """
    page, page_size = 0, 1000
    all_game_keys = set()
    while True:
        res = (db.table("lineup_stints")
               .select("game_key")
               .eq("league_id", league_id)
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        batch = res.data or []
        for r in batch:
            all_game_keys.add(r["game_key"])
        if len(batch) < page_size:
            break
        page += 1

    if not all_game_keys:
        log.info("No lineup_stints found for league %s", league_id)
        return 0

    # Fetch existing tracking rows
    existing = set()
    page = 0
    while True:
        res = (db.table("lineup_context_backfill_status")
               .select("game_key")
               .eq("league_id", league_id)
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        batch = res.data or []
        for r in batch:
            existing.add(r["game_key"])
        if len(batch) < page_size:
            break
        page += 1

    new_keys = all_game_keys - existing
    if new_keys:
        rows = [{"game_key": gk, "league_id": league_id, "status": "pending"}
                for gk in new_keys]
        # Insert in chunks of 200
        for i in range(0, len(rows), 200):
            db.table("lineup_context_backfill_status").insert(rows[i:i+200]).execute()
        log.info("Inserted %d new tracking rows", len(new_keys))

    return len(all_game_keys)


def get_pending_games(db, league_id: str, limit: int = None) -> list:
    """Return pending game_keys ordered by game_key (proxy for size)."""
    q = (db.table("lineup_context_backfill_status")
         .select("game_key")
         .eq("league_id", league_id)
         .eq("status", "pending")
         .order("game_key"))
    if limit:
        q = q.limit(limit)
    res = q.execute()
    return [r["game_key"] for r in (res.data or [])]


def mark_running(db, league_id: str, game_key: str):
    (db.table("lineup_context_backfill_status")
     .update({"status": "running"})
     .eq("league_id", league_id)
     .eq("game_key", game_key)
     .execute())


def mark_complete(db, league_id: str, game_key: str, inserted: int):
    (db.table("lineup_context_backfill_status")
     .update({"status": "complete", "inserted_rows": inserted})
     .eq("league_id", league_id)
     .eq("game_key", game_key)
     .execute())


def mark_failed(db, league_id: str, game_key: str, error: str):
    (db.table("lineup_context_backfill_status")
     .update({"status": "failed", "error_msg": error[:500]})
     .eq("league_id", league_id)
     .eq("game_key", game_key)
     .execute())


def reset_failed(db, league_id: str):
    (db.table("lineup_context_backfill_status")
     .update({"status": "pending", "error_msg": None})
     .eq("league_id", league_id)
     .eq("status", "failed")
     .execute())
    log.info("Reset failed rows to pending for league %s", league_id)


def print_status(db, league_id: str):
    res = (db.table("lineup_context_backfill_status")
           .select("status")
           .eq("league_id", league_id)
           .execute())
    rows = res.data or []
    counts = defaultdict(int)
    for r in rows:
        counts[r["status"]] += 1
    total = len(rows)
    print(f"\n{'─'*45}")
    print(f"League: {league_id}")
    print(f"{'─'*45}")
    print(f"  Total games tracked : {total}")
    for status in ("complete", "pending", "running", "failed"):
        n = counts.get(status, 0)
        if n or status in ("complete", "pending"):
            pct = f" ({n/total*100:.0f}%)" if total else ""
            print(f"  {status:<18}: {n}{pct}")
    print(f"{'─'*45}\n")


# ---------------------------------------------------------------------------
# Core per-game processing (Python implementation of the SQL CTE)
# ---------------------------------------------------------------------------

def fetch_stints(db, game_key: str, league_id: str) -> list:
    page, page_size, results = 0, 1000, []
    while True:
        res = (db.table("lineup_stints")
               .select("*")
               .eq("game_key", game_key)
               .eq("league_id", league_id)
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        batch = res.data or []
        results.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return results


def compute_context_rows(stints: list) -> list:
    """
    Replicate the CTE logic in Python:
      1. Sort stints per team by (start_game_secs, start_action).
      2. Compute score_margin = cumulative net points BEFORE this stint.
      3. Derive is_garbage_time, is_short_clock_end_period, is_competitive_stint.
    """
    # Group by team_id for window function
    by_team = defaultdict(list)
    for s in stints:
        by_team[s["team_id"]].append(s)

    output = []
    for team_id, team_stints in by_team.items():
        team_stints.sort(key=lambda s: (
            s.get("start_game_secs") or 0,
            s.get("start_action") or 0,
        ))
        cumulative = 0
        for s in team_stints:
            score_margin = cumulative
            pf = s.get("points_for") or 0
            pa = s.get("points_against") or 0
            cumulative += pf - pa

            period = s.get("period") or 0
            sgs = s.get("start_game_secs") or 0
            margin_abs = abs(score_margin)
            is_valid = s.get("is_valid_lineup", True)

            # Garbage time: Q4 only, tiered by start_game_secs
            if period == 4:
                if sgs >= 2100 and margin_abs >= 10:
                    is_garbage = True
                elif sgs >= 1950 and margin_abs >= 20:
                    is_garbage = True
                elif sgs >= 1800 and margin_abs >= 25:
                    is_garbage = True
                else:
                    is_garbage = False
            else:
                is_garbage = False

            # Short-clock end-of-period: Q1/Q2/Q3 only
            if period == 1 and sgs >= 598:
                is_short = True
            elif period == 2 and sgs >= 1198:
                is_short = True
            elif period == 3 and sgs >= 1798:
                is_short = True
            else:
                is_short = False

            is_competitive = bool(is_valid and not is_garbage and not is_short)

            output.append({
                "stint_id": s["id"],
                "game_key": s["game_key"],
                "league_id": s["league_id"],
                "team_id": s["team_id"],
                "lineup_key": s.get("lineup_key"),
                "lineup_player_ids": s.get("lineup_player_ids"),
                "lineup_names": s.get("lineup_names"),
                "period": period,
                "start_action": s.get("start_action"),
                "end_action": s.get("end_action"),
                "start_clock": s.get("start_clock"),
                "end_clock": s.get("end_clock"),
                "start_game_secs": sgs,
                "end_game_secs": s.get("end_game_secs"),
                "seconds_played": s.get("seconds_played") or 0,
                "points_for": pf,
                "points_against": pa,
                "fg2_made": s.get("fg2_made") or 0,
                "fg2_attempted": s.get("fg2_attempted") or 0,
                "fg3_made": s.get("fg3_made") or 0,
                "fg3_attempted": s.get("fg3_attempted") or 0,
                "ft_made": s.get("ft_made") or 0,
                "ft_attempted": s.get("ft_attempted") or 0,
                "oreb": s.get("oreb") or 0,
                "dreb": s.get("dreb") or 0,
                "assists": s.get("assists") or 0,
                "turnovers": s.get("turnovers") or 0,
                "fouls": s.get("fouls") or 0,
                "steals": s.get("steals") or 0,
                "blocks": s.get("blocks") or 0,
                "possessions_for": s.get("possessions_for") or 0,
                "possessions_against": s.get("possessions_against") or 0,
                "is_valid_lineup": is_valid,
                "score_margin": score_margin,
                "is_garbage_time": is_garbage,
                "is_short_clock_end_period": is_short,
                "is_competitive_stint": is_competitive,
            })
    return output


def delete_context_for_game(db, game_key: str, league_id: str):
    (db.table("lineup_stints_context_v1")
     .delete()
     .eq("game_key", game_key)
     .eq("league_id", league_id)
     .execute())


def insert_context_rows(db, rows: list) -> int:
    chunk_size = 200
    inserted = 0
    for i in range(0, len(rows), chunk_size):
        db.table("lineup_stints_context_v1").insert(rows[i:i+chunk_size]).execute()
        inserted += len(rows[i:i+chunk_size])
    return inserted


def process_game(db, league_id: str, game_key: str) -> int:
    """
    Process a single game:
      1. Mark running.
      2. Fetch stints.
      3. Compute context rows in Python.
      4. Delete old context rows, insert new ones.
      5. Mark complete.
    Returns number of rows inserted.
    """
    mark_running(db, league_id, game_key)
    try:
        stints = fetch_stints(db, game_key, league_id)
        if not stints:
            log.warning("No stints found for game %s — marking complete with 0 rows", game_key)
            mark_complete(db, league_id, game_key, 0)
            return 0

        context_rows = compute_context_rows(stints)
        delete_context_for_game(db, game_key, league_id)
        inserted = insert_context_rows(db, context_rows)
        mark_complete(db, league_id, game_key, inserted)
        log.info("  ✅ %s — %d stints → %d context rows", game_key, len(stints), inserted)
        return inserted
    except Exception as e:
        err = str(e)
        log.error("  ❌ %s — %s", game_key, err)
        mark_failed(db, league_id, game_key, err)
        return 0


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Backfill lineup_stints_context_v1 for a league, one game at a time."
    )
    parser.add_argument("--league-id", required=True, help="League UUID")
    parser.add_argument("--status", action="store_true",
                        help="Show backfill progress and exit")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max number of pending games to process")
    parser.add_argument("--game-key", default=None,
                        help="Process a single specific game_key")
    parser.add_argument("--reset-failed", action="store_true",
                        help="Reset failed games to pending before processing")
    args = parser.parse_args()

    league_id = args.league_id
    db = get_db()

    # Ensure tracking rows exist for all games in this league
    total = ensure_tracking_rows(db, league_id)
    log.info("Tracking table covers %d games for league %s", total, league_id)

    if args.reset_failed:
        reset_failed(db, league_id)

    if args.status:
        print_status(db, league_id)
        return

    if args.game_key:
        # Single game mode
        process_game(db, league_id, args.game_key)
        print_status(db, league_id)
        return

    # Batch mode
    pending = get_pending_games(db, league_id, limit=args.limit)
    if not pending:
        log.info("No pending games — nothing to do.")
        print_status(db, league_id)
        return

    log.info("Processing %d pending game(s)...", len(pending))
    total_inserted = 0
    for i, gk in enumerate(pending, 1):
        log.info("[%d/%d] %s", i, len(pending), gk)
        total_inserted += process_game(db, league_id, gk)

    log.info("Done. Total context rows inserted: %d", total_inserted)
    print_status(db, league_id)


if __name__ == "__main__":
    main()
