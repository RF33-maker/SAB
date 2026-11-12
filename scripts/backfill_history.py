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
    python scripts/backfill_history.py --keys keys.txt [--limit 20] [--dry-run] [--replace-plays]

keys.txt holds one game_schedule.game_key per line. Rows that already have
parsed_at are skipped, so a stopped run can simply be started again.

--replace-plays re-parses games that were stored before a parser fix: set
their parsed_at to null first, and for each game the script checks the feed
still loads, then clears its play-by-play, shots and lineup stints (which are
only ever appended to) before parsing. Box scores are upserted in place.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.json_parser import build_data_url, game_db, parse_and_store_game  # noqa: E402
from app.utils.compute_advanced_stats import compute_advanced_stats  # noqa: E402
import app.utils.json_parser as json_parser  # noqa: E402
import app.utils.lineup_builder as lineup_builder  # noqa: E402

# A re-parse keeps the teams the game's box score already points at. The team
# lookup depends on which seasons were parsed first, so asking it again can
# give a different (same-named) team, which the team box score can't move to
# and which would put the players on new ids. Filled per game by clear_plays.
_pinned_teams = {}
_get_or_create_team = json_parser.get_or_create_team


def _pinned_get_or_create_team(league_id, name, user_id=None, sibling_league_ids=None):
    pinned = _pinned_teams.get(json_parser.normalize_team_name(name))
    return pinned or _get_or_create_team(league_id, name, user_id, sibling_league_ids)


json_parser.get_or_create_team = _pinned_get_or_create_team

# The parser refreshes the whole league's on/off table after every game. On a
# backfill that grows slower with each game (and times out once the league is
# big), so it is done once per competition at the end instead.
_refresh_player_on_off = lineup_builder._refresh_player_on_off
lineup_builder._refresh_player_on_off = lambda db, league_id: None

COLS = 'game_key, competitionname, matchtime, hometeam, awayteam, "LiveStats URL", league_id, organisation, parsed_at'

# Rows a re-parse rebuilds from the feed (keyed on action numbers / stints).
REPLAY_TABLES = ("live_events", "shot_chart", "lineup_stints", "lineup_stints_context_v1", "player_on_court_stints")


def box_player_ids(key):
    rows = game_db.table("player_stats").select("player_id").eq("game_key", key).execute().data or []
    return {r["player_id"] for r in rows}


def clear_plays(key, gid):
    """Clear a game's play-by-play and lineups, but only once its feed loads.
    Pins the game's teams to the ones its box score already uses."""
    resp = requests.get(build_data_url(gid), headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    if resp.status_code != 200 or not (resp.json().get("pbp") or []):
        raise RuntimeError(f"feed not loading (HTTP {resp.status_code}); kept existing plays")
    # By side, not by name: a team row can since have been renamed away from
    # the name in the feed.
    feed_teams = json_parser.feed_teams(resp.json())
    by_side = {str(r["side"]): r["team_id"] for r in
               game_db.table("team_stats").select("side,team_id").eq("game_key", key).execute().data or []
               if r.get("team_id")}
    _pinned_teams.clear()
    for side, team in feed_teams.items():
        if by_side.get(side) and team.get("name"):
            _pinned_teams[json_parser.normalize_team_name(team["name"])] = by_side[side]
    for table in REPLAY_TABLES:
        game_db.table(table).delete().eq("game_key", key).execute()


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
    ap.add_argument("--replace-plays", action="store_true")
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
            if args.replace_plays:
                players_before = box_player_ids(key)
                clear_plays(key, gid)
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
            if args.replace_plays and box_player_ids(key) != players_before:
                print(f"  note {key}: box score players changed on re-parse", flush=True)
            # Box score rows carry no game date of their own: the site dates
            # and orders them by created_at, which would otherwise be the day
            # of the import (and would rank a 2016 game as a player's latest).
            if r.get("matchtime"):
                for table in ("player_stats", "team_stats"):
                    game_db.table(table).update({"created_at": r["matchtime"]}).eq("game_key", key).execute()
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

    # Run these in SQL (select refresh_player_on_off('<id>')): through the API
    # the call hits the 8s statement timeout on a full competition.
    for lid in sorted(leagues):
        print(f"on/off refresh needed {lid}", flush=True)

    print(f"done: {len(rows) - len(failed)} parsed, {len(failed)} failed, {time.time() - t_run:.0f}s", flush=True)
    for k in failed:
        print("  failed", k, flush=True)


if __name__ == "__main__":
    main()
