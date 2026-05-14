#!/usr/bin/env python3
"""
backfill_lineup_context_existing.py
------------------------------------
Backfill lineup_stints_context_v1 for an existing league, one game at a time.

Uses the Supabase REST client (SUPABASE_URL + SUPABASE_KEY).
The tracking table (lineup_context_backfill_status) is optional — if it
doesn't exist the script still runs using in-memory progress tracking.

Usage examples:

  # Check how many games need processing
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --status

  # Process the next 10 games
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --limit 10

  # Process all games
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID>

  # Process a single specific game
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --game-key <KEY>
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


def _table_exists(db, table_name: str) -> bool:
    """Quick probe — returns False on 404/42P01 (table not found)."""
    try:
        db.table(table_name).select("*").limit(1).execute()
        return True
    except Exception as e:
        return "42P01" not in str(e) and "404" not in str(e) and "does not exist" not in str(e).lower()


# ---------------------------------------------------------------------------
# Fetch all game keys for a league
# ---------------------------------------------------------------------------

def fetch_all_game_keys(db, league_id: str) -> list:
    page, page_size, results = 0, 1000, set()
    while True:
        res = (db.table("lineup_stints")
               .select("game_key")
               .eq("league_id", league_id)
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        batch = res.data or []
        for r in batch:
            results.add(r["game_key"])
        if len(batch) < page_size:
            break
        page += 1
    return sorted(results)


def fetch_already_done_keys(db, league_id: str) -> set:
    """Return game_keys that already have rows in lineup_stints_context_v1."""
    try:
        page, page_size, done = 0, 1000, set()
        while True:
            res = (db.table("lineup_stints_context_v1")
                   .select("game_key")
                   .eq("league_id", league_id)
                   .range(page * page_size, (page + 1) * page_size - 1)
                   .execute())
            batch = res.data or []
            for r in batch:
                done.add(r["game_key"])
            if len(batch) < page_size:
                break
            page += 1
        return done
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Optional tracking table helpers (silently skipped if table absent)
# ---------------------------------------------------------------------------

TRACKING_TABLE = "lineup_context_backfill_status"
_tracking_available = None   # cached after first check


def _tracking_ok(db) -> bool:
    global _tracking_available
    if _tracking_available is None:
        _tracking_available = _table_exists(db, TRACKING_TABLE)
        if not _tracking_available:
            log.warning(
                "Tracking table '%s' not found — progress won't be persisted. "
                "Run the SQL in the README to create it for resumable backfills.",
                TRACKING_TABLE,
            )
    return _tracking_available


def _track(db, league_id, game_key, status, inserted=None, error=None):
    if not _tracking_ok(db):
        return
    try:
        update = {"status": status}
        if inserted is not None:
            update["inserted_rows"] = inserted
        if error is not None:
            update["error_msg"] = str(error)[:500]
        (db.table(TRACKING_TABLE)
         .upsert({"game_key": game_key, "league_id": league_id, **update},
                 on_conflict="game_key,league_id")
         .execute())
    except Exception as e:
        log.debug("Tracking update failed (non-fatal): %s", e)


# ---------------------------------------------------------------------------
# Per-game context computation (Python equivalent of the SQL CTE)
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
    Compute score_margin (window fn) and derive flags for every stint.

    Garbage time  — Q4 only, tiered by start_game_secs:
        >= 2100 s and |margin| >= 10
        >= 1950 s and |margin| >= 20
        >= 1800 s and |margin| >= 25
    Short-clock   — Q1 end >= 598 s, Q2 end >= 1198 s, Q3 end >= 1798 s
    """
    by_team = defaultdict(list)
    for s in stints:
        by_team[s["team_id"]].append(s)

    output = []
    for _, team_stints in by_team.items():
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
            sgs    = s.get("start_game_secs") or 0
            is_valid = bool(s.get("is_valid_lineup", True))
            m = abs(score_margin)

            # Garbage time
            if period == 4:
                is_garbage = (sgs >= 2100 and m >= 10) or \
                             (sgs >= 1950 and m >= 20) or \
                             (sgs >= 1800 and m >= 25)
            else:
                is_garbage = False

            # Short-clock end-of-period
            is_short = (period == 1 and sgs >= 598) or \
                       (period == 2 and sgs >= 1198) or \
                       (period == 3 and sgs >= 1798)

            output.append({
                "stint_id":                  s["id"],
                "game_key":                  s["game_key"],
                "league_id":                 s["league_id"],
                "team_id":                   s["team_id"],
                "lineup_key":                s.get("lineup_key"),
                "lineup_player_ids":         s.get("lineup_player_ids"),
                "lineup_names":              s.get("lineup_names"),
                "period":                    period,
                "start_action":              s.get("start_action"),
                "end_action":                s.get("end_action"),
                "start_clock":               s.get("start_clock"),
                "end_clock":                 s.get("end_clock"),
                "start_game_secs":           sgs,
                "end_game_secs":             s.get("end_game_secs"),
                "seconds_played":            s.get("seconds_played") or 0,
                "points_for":                pf,
                "points_against":            pa,
                "fg2_made":                  s.get("fg2_made") or 0,
                "fg2_attempted":             s.get("fg2_attempted") or 0,
                "fg3_made":                  s.get("fg3_made") or 0,
                "fg3_attempted":             s.get("fg3_attempted") or 0,
                "ft_made":                   s.get("ft_made") or 0,
                "ft_attempted":              s.get("ft_attempted") or 0,
                "oreb":                      s.get("oreb") or 0,
                "dreb":                      s.get("dreb") or 0,
                "assists":                   s.get("assists") or 0,
                "turnovers":                 s.get("turnovers") or 0,
                "fouls":                     s.get("fouls") or 0,
                "steals":                    s.get("steals") or 0,
                "blocks":                    s.get("blocks") or 0,
                "possessions_for":           s.get("possessions_for") or 0,
                "possessions_against":       s.get("possessions_against") or 0,
                "is_valid_lineup":           is_valid,
                "score_margin":              score_margin,
                "is_garbage_time":           is_garbage,
                "is_short_clock_end_period": is_short,
                "is_competitive_stint":      is_valid and not is_garbage and not is_short,
            })
    return output


def delete_context_for_game(db, game_key: str, league_id: str):
    try:
        (db.table("lineup_stints_context_v1")
         .delete()
         .eq("game_key", game_key)
         .eq("league_id", league_id)
         .execute())
    except Exception:
        pass  # table may be empty; non-fatal


_STRIPPED_COLS: set = set()   # columns confirmed missing from the table


def insert_context_rows(db, rows: list) -> int:
    """
    Insert rows in chunks of 200.  Auto-strips any column the table doesn't
    have (PGRST204) and retries — same pattern used throughout the codebase.
    """
    # Remove globally-discovered missing columns from every row upfront
    if _STRIPPED_COLS:
        rows = [{k: v for k, v in r.items() if k not in _STRIPPED_COLS}
                for r in rows]

    inserted = 0
    chunk_size = 200
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        for attempt in range(20):
            try:
                db.table("lineup_stints_context_v1").insert(chunk).execute()
                inserted += len(chunk)
                break
            except Exception as e:
                err = str(e)
                if "PGRST204" in err:
                    # Extract the unknown column name from the error message
                    import re
                    m = re.search(r"'([^']+)' column of", err)
                    if m:
                        col = m.group(1)
                        if col not in _STRIPPED_COLS:
                            log.warning("Column '%s' missing from lineup_stints_context_v1 — stripping and retrying", col)
                            _STRIPPED_COLS.add(col)
                        chunk = [{k: v for k, v in r.items() if k not in _STRIPPED_COLS}
                                 for r in chunk]
                        continue
                raise
    return inserted


def process_game(db, league_id: str, game_key: str) -> int:
    _track(db, league_id, game_key, "running")
    try:
        stints = fetch_stints(db, game_key, league_id)
        if not stints:
            log.warning("  ⚠️  %s — no stints found, skipping", game_key)
            _track(db, league_id, game_key, "complete", inserted=0)
            return 0
        ctx_rows = compute_context_rows(stints)
        delete_context_for_game(db, game_key, league_id)
        inserted = insert_context_rows(db, ctx_rows)
        _track(db, league_id, game_key, "complete", inserted=inserted)
        log.info("  ✅ %s — %d stints → %d context rows", game_key, len(stints), inserted)
        return inserted
    except Exception as e:
        log.error("  ❌ %s — %s", game_key, e)
        _track(db, league_id, game_key, "failed", error=str(e))
        return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Backfill lineup_stints_context_v1 one game at a time."
    )
    parser.add_argument("--league-id", required=True)
    parser.add_argument("--status",      action="store_true",
                        help="Show counts and exit")
    parser.add_argument("--limit",       type=int, default=None,
                        help="Max games to process this run")
    parser.add_argument("--game-key",    default=None,
                        help="Process one specific game_key")
    args = parser.parse_args()

    league_id = args.league_id
    db = get_db()

    all_keys  = fetch_all_game_keys(db, league_id)
    done_keys = fetch_already_done_keys(db, league_id)
    pending   = [k for k in all_keys if k not in done_keys]

    log.info("League %s: %d total games, %d already done, %d pending",
             league_id, len(all_keys), len(done_keys), len(pending))

    if args.status:
        print(f"\n{'─'*50}")
        print(f"  Total games  : {len(all_keys)}")
        print(f"  Done         : {len(done_keys)}")
        print(f"  Pending      : {len(pending)}")
        print(f"{'─'*50}\n")
        return

    if args.game_key:
        process_game(db, league_id, args.game_key)
        return

    to_process = pending[:args.limit] if args.limit else pending
    if not to_process:
        log.info("Nothing to do — all games already processed.")
        return

    log.info("Processing %d game(s)...", len(to_process))
    total_inserted, failed = 0, 0
    for i, gk in enumerate(to_process, 1):
        log.info("[%d/%d] %s", i, len(to_process), gk)
        n = process_game(db, league_id, gk)
        if n == 0 and gk in [r for r in to_process]:
            failed += 1
        total_inserted += n

    log.info("Done. %d context rows inserted, %d games failed.", total_inserted, failed)
    print(f"\n{'─'*50}")
    print(f"  Processed    : {len(to_process)}")
    print(f"  Rows inserted: {total_inserted}")
    remaining = len(pending) - len(to_process)
    print(f"  Still pending: {remaining}")
    print(f"{'─'*50}\n")


if __name__ == "__main__":
    main()
