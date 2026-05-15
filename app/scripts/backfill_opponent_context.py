#!/usr/bin/env python3
"""
backfill_opponent_context.py
-----------------------------
Fills opp_oreb, opp_dreb, opp_fg2_attempted in lineup_stints_context_v1
using per-game overlap logic with seconds_played as the denominator.

Mirrors the SQL CTE in the task spec exactly:
  overlap / nullif(opp.seconds_played, 0) as the weighting fraction.

Status tracking columns used (must exist in table):
  opponent_context_status      text
  opponent_context_updated_at  timestamptz

Row lifecycle:
  seconds_played = 0  →  opponent_context_status = 'ignored_zero_seconds'
  seconds_played > 0  →  opponent_context_status = 'complete'  (after update)

CLI:
  python -m app.scripts.backfill_opponent_context --all
  python -m app.scripts.backfill_opponent_context --league-id <UUID>
  python -m app.scripts.backfill_opponent_context --game-key <GAME_KEY>
  python -m app.scripts.backfill_opponent_context --status
  python -m app.scripts.backfill_opponent_context --all --force    (reprocess complete rows too)
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("backfill_opp_context")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE = "lineup_stints_context_v1"


def get_db():
    from supabase import create_client
    from supabase.lib.client_options import ClientOptions
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL and SUPABASE_KEY must be set.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY,
                         options=ClientOptions(schema="public"))


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_game_keys(db, league_id=None, force=False) -> list:
    """
    Distinct game_keys that have pending positive-second rows.
    With --force, returns all game_keys that have any positive-second rows.
    """
    page, page_size, keys = 0, 1000, set()
    while True:
        q = db.table(TABLE).select("game_key").gt("seconds_played", 0)
        if league_id:
            q = q.eq("league_id", league_id)
        if not force:
            q = q.neq("opponent_context_status", "complete")
        res = q.range(page * page_size, (page + 1) * page_size - 1).execute()
        batch = res.data or []
        for r in batch:
            keys.add(r["game_key"])
        if len(batch) < page_size:
            break
        page += 1
    return sorted(keys)


def fetch_all_rows_for_game(db, game_key: str) -> list:
    """All rows for a game — used as the opponent pool for overlap calculation."""
    page, page_size, results = 0, 1000, []
    while True:
        res = (db.table(TABLE)
               .select("id,team_id,period,start_game_secs,end_game_secs,"
                       "seconds_played,oreb,dreb,fg2_attempted,"
                       "opponent_context_status")
               .eq("game_key", game_key)
               .range(page * page_size, (page + 1) * page_size - 1)
               .execute())
        batch = res.data or []
        results.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return results


def fetch_status(db, league_id=None) -> dict:
    page, page_size = 0, 1000
    totals = {"total": 0, "complete": 0, "ignored_zero_seconds": 0, "pending": 0}
    while True:
        q = db.table(TABLE).select("seconds_played,opponent_context_status")
        if league_id:
            q = q.eq("league_id", league_id)
        res = q.range(page * page_size, (page + 1) * page_size - 1).execute()
        batch = res.data or []
        for r in batch:
            totals["total"] += 1
            status = r.get("opponent_context_status") or ""
            sp = r.get("seconds_played") or 0
            if status == "complete":
                totals["complete"] += 1
            elif status == "ignored_zero_seconds":
                totals["ignored_zero_seconds"] += 1
            elif sp > 0:
                totals["pending"] += 1
        if len(batch) < page_size:
            break
        page += 1
    return totals


# ---------------------------------------------------------------------------
# Overlap computation (Python mirror of the SQL CTE)
# ---------------------------------------------------------------------------

def compute_opponent_context(all_rows: list, target_ids: set) -> dict:
    """
    For every row in target_ids (seconds_played > 0), compute the weighted sum
    of the opponent's oreb/dreb/fg2_attempted during the overlapping time window.

    Weighting: overlap_seconds / opp.seconds_played
    Filters:   opp.period == cur.period  AND  opp.team_id != cur.team_id
               AND opp.seconds_played > 0

    Returns: {row_id: {opp_oreb, opp_dreb, opp_fg2_attempted}}
    """
    by_team = defaultdict(list)
    for r in all_rows:
        by_team[r["team_id"]].append(r)

    team_ids = list(by_team.keys())
    results = {}

    for team_id in team_ids:
        opp_pool = [r for tid, rlist in by_team.items()
                    if tid != team_id
                    for r in rlist
                    if (r.get("seconds_played") or 0) > 0]

        for cur in by_team[team_id]:
            if cur["id"] not in target_ids:
                continue
            if (cur.get("seconds_played") or 0) <= 0:
                continue

            s1     = cur.get("start_game_secs") or 0
            e1     = cur.get("end_game_secs") or 0
            period = cur.get("period")

            opp_oreb = opp_dreb = opp_fg2 = 0.0

            for opp in opp_pool:
                if opp.get("period") != period:
                    continue
                s2 = opp.get("start_game_secs") or 0
                e2 = opp.get("end_game_secs") or 0
                overlap = max(0, min(e1, e2) - max(s1, s2))
                if overlap <= 0:
                    continue
                frac = overlap / (opp.get("seconds_played") or 1)
                opp_oreb += (opp.get("oreb") or 0) * frac
                opp_dreb += (opp.get("dreb") or 0) * frac
                opp_fg2  += (opp.get("fg2_attempted") or 0) * frac

            results[cur["id"]] = {
                "opp_oreb":          round(opp_oreb, 4),
                "opp_dreb":          round(opp_dreb, 4),
                "opp_fg2_attempted": round(opp_fg2, 4),
            }

    return results


# ---------------------------------------------------------------------------
# Update helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mark_zero_second_rows(db, game_key: str):
    try:
        db.table(TABLE).update({
            "opponent_context_status":   "ignored_zero_seconds",
            "opponent_context_updated_at": now_iso(),
        }).eq("game_key", game_key).eq("seconds_played", 0).execute()
    except Exception as e:
        log.warning("  [%s] Could not mark zero-second rows: %s", game_key, e)


def update_rows(db, context: dict) -> int:
    """
    Update each row individually via PATCH .eq("id", ...).
    Groups rows with identical values to minimise HTTP round-trips.
    """
    if not context:
        return 0

    ts = now_iso()

    # Group by rounded value tuple to batch rows that share the same values
    by_values: dict[tuple, list] = defaultdict(list)
    for row_id, vals in context.items():
        key = (vals["opp_oreb"], vals["opp_dreb"], vals["opp_fg2_attempted"])
        by_values[key].append(row_id)

    updated = 0
    for (opp_oreb, opp_dreb, opp_fg2), row_ids in by_values.items():
        payload = {
            "opp_oreb":                  opp_oreb,
            "opp_dreb":                  opp_dreb,
            "opp_fg2_attempted":         opp_fg2,
            "opponent_context_status":   "complete",
            "opponent_context_updated_at": ts,
        }
        # Send in chunks of 50 IDs per .in_() call
        for i in range(0, len(row_ids), 50):
            chunk = row_ids[i:i+50]
            try:
                db.table(TABLE).update(payload).in_("id", chunk).execute()
                updated += len(chunk)
            except Exception as e:
                log.error("  update failed for ids %s…: %s", chunk[:3], e)

    return updated


# ---------------------------------------------------------------------------
# Per-game processor
# ---------------------------------------------------------------------------

def process_game(db, game_key: str, force: bool = False) -> tuple:
    """Returns (rows_updated, error_msg_or_None)."""
    try:
        all_rows = fetch_all_rows_for_game(db, game_key)
        if not all_rows:
            return 0, None

        mark_zero_second_rows(db, game_key)

        # Determine which rows need updating
        if force:
            target_ids = {r["id"] for r in all_rows
                          if (r.get("seconds_played") or 0) > 0}
        else:
            target_ids = {r["id"] for r in all_rows
                          if (r.get("seconds_played") or 0) > 0
                          and r.get("opponent_context_status") != "complete"}

        if not target_ids:
            return 0, None

        context = compute_opponent_context(all_rows, target_ids)
        updated = update_rows(db, context)
        return updated, None

    except Exception as e:
        return 0, str(e)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def print_status(db, league_id=None):
    log.info("Fetching status counts...")
    counts = fetch_status(db, league_id)
    print(f"\n{'─'*55}")
    print(f"  Total rows              : {counts['total']}")
    print(f"  Complete                : {counts['complete']}")
    print(f"  Ignored (0 seconds)     : {counts['ignored_zero_seconds']}")
    print(f"  Pending (positive secs) : {counts['pending']}")
    print(f"{'─'*55}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Backfill opponent context in lineup_stints_context_v1."
    )
    parser.add_argument("--all",       action="store_true",
                        help="Process all leagues")
    parser.add_argument("--league-id", default=None)
    parser.add_argument("--game-key",  default=None)
    parser.add_argument("--status",    action="store_true")
    parser.add_argument("--force",     action="store_true",
                        help="Reprocess rows already marked complete")
    args = parser.parse_args()

    db = get_db()

    if args.status:
        print_status(db, args.league_id)
        return

    if args.game_key:
        log.info("Processing single game: %s", args.game_key)
        updated, err = process_game(db, args.game_key, force=args.force)
        if err:
            log.error("  ❌ %s — %s", args.game_key, err)
        else:
            log.info("  ✅ %s — %d rows updated", args.game_key, updated)
        return

    if not args.all and not args.league_id:
        parser.error("Specify --all, --league-id, --game-key, or --status")

    game_keys = fetch_game_keys(db, league_id=args.league_id, force=args.force)
    total = len(game_keys)
    if total == 0:
        log.info("Nothing to do — no pending games found. Use --force to reprocess.")
        return
    log.info("Found %d game(s) with pending rows.", total)

    succeeded = failed = total_updated = 0
    for i, gk in enumerate(game_keys, 1):
        updated, err = process_game(db, gk, force=args.force)
        if err:
            log.error("  ❌ [%d/%d] %s — %s", i, total, gk, err)
            failed += 1
        else:
            log.info("  ✅ [%d/%d] %s — %d rows updated", i, total, gk, updated)
            succeeded += 1
            total_updated += updated

    print(f"\n{'─'*55}")
    print(f"  Games processed  : {total}")
    print(f"  Succeeded        : {succeeded}")
    print(f"  Failed           : {failed}")
    print(f"  Rows updated     : {total_updated}")
    print(f"{'─'*55}\n")


if __name__ == "__main__":
    main()
