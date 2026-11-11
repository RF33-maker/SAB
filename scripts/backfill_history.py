"""
Parse a batch of past games straight into the database, one at a time,
without going through the live worker's queue.

For a history import (thousands of finished games from earlier seasons) the
worker is the wrong tool: it parses due games one after another inside its
polling loop, so a big queue delays live games, and it recomputes a whole
league's advanced stats after every single game. This script uses the same
parse_and_store_game the worker uses, then marks each row final with its
post-game window already closed (final_detected_at = tip-off), so the worker
never looks at it again. Advanced stats are computed once per competition at
the end.

Usage (env: SUPABASE_URL, SUPABASE_KEY = service role key):
    python scripts/backfill_history.py --keys keys.txt [--limit 20] [--dry-run]

keys.txt holds one game_schedule.game_key per line. Rows that already have
parsed_at are skipped, so a stopped run can simply be started again.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.json_parser import game_db, parse_and_store_game  # noqa: E402
from app.utils.compute_advanced_stats import compute_advanced_stats  # noqa: E402

COLS = 'game_key, competitionname, matchtime, hometeam, awayteam, "LiveStats URL", league_id, organisation, parsed_at'


def numeric_id(url):
    import re
    m = re.search(r"/(\d+)/?(?:data\.json)?$", (url or "").strip())
    return m.group(1) if m else None


def load_rows(keys):
    rows = []
    for i in range(0, len(keys), 200):
        chunk = keys[i:i + 200]
        rows += game_db.table("game_schedule").select(COLS).in_("game_key", chunk).execute().data or []
    return sorted(
        (r for r in rows if not r.get("parsed_at")),
        key=lambda r: r.get("matchtime") or "",
        reverse=True,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", required=True)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-advanced", action="store_true")
    args = ap.parse_args()

    keys = [k.strip() for k in open(args.keys) if k.strip()]
    rows = load_rows(keys)
    if args.limit:
        rows = rows[:args.limit]
    print(f"{len(keys)} keys, {len(rows)} still to parse", flush=True)
    if args.dry_run:
        for r in rows[:20]:
            print("  would parse", r["game_key"], r["competitionname"], r["matchtime"], flush=True)
        return

    leagues, failed = set(), []
    t_run = time.time()
    for n, r in enumerate(rows, 1):
        key = r["game_key"]
        gid = numeric_id(r.get("LiveStats URL"))
        t0 = time.time()
        try:
            if not gid:
                raise ValueError("no LiveStats id in URL")
            league_id = parse_and_store_game(
                numeric_id=gid,
                league_name=r["competitionname"],
                game_date=r.get("matchtime"),
                home_team_name=r.get("hometeam"),
                away_team_name=r.get("awayteam"),
                game_key=key,
                livestats_url=r.get("LiveStats URL"),
                league_id=r.get("league_id"),
                organisation=r.get("organisation"),
            )
            if not league_id:
                raise RuntimeError("feed returned no stats")
            # parse_and_store_game logs a play-by-play error and carries on, so a
            # dropped connection there would leave the game with stats but no plays.
            plays = game_db.table("live_events").select("game_key", count="exact", head=True) \
                .eq("game_key", key).execute().count
            if not plays:
                raise RuntimeError("no play-by-play stored")
            game_db.table("game_schedule").update({
                "status": "final",
                "parsed_at": datetime.now(timezone.utc).isoformat(),
                "final_detected_at": r.get("matchtime"),
                "next_poll_at": None,
                "poll_fail_count": 0,
            }).eq("game_key", key).execute()
            leagues.add(league_id)
            print(f"[{n}/{len(rows)}] ok   {key} {r['competitionname']} {time.time() - t0:.1f}s", flush=True)
        except Exception as e:
            failed.append(key)
            print(f"[{n}/{len(rows)}] FAIL {key} {r['competitionname']}: {e}", flush=True)

    if not args.skip_advanced:
        for lid in sorted(leagues):
            t0 = time.time()
            try:
                compute_advanced_stats(lid)
                print(f"advanced stats ok {lid} {time.time() - t0:.1f}s", flush=True)
            except Exception as e:
                print(f"advanced stats FAIL {lid}: {e}", flush=True)

    print(f"done: {len(rows) - len(failed)} parsed, {len(failed)} failed, {time.time() - t_run:.0f}s", flush=True)
    for k in failed:
        print("  failed", k, flush=True)


if __name__ == "__main__":
    main()
