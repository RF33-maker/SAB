#!/usr/bin/env python3
"""
Shot chart backfill script.

Fetches LiveStats JSON for all final games that have no shot data yet
and populates public.shot_chart.

Run from project root:
    python -m app.backfill_shots

Optional env vars:
    LEAGUE_ID   - restrict backfill to a single league
    LIMIT       - max number of games to process (default: all)
    DRY_RUN     - set to "true" to print counts without inserting
"""

import os
import sys
import logging
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("backfill_shots")

from app.utils.json_parser import game_db, ref_db

LIVESTATS_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"
FILTER_LEAGUE_ID = os.environ.get("LEAGUE_ID")
LIMIT = int(os.environ.get("LIMIT", "9999"))

REQUEST_HEADERS = {
    "User-Agent": "SwishAssistant/1.0 (ShotBackfill)",
    "Accept": "application/json",
}


def get_games_missing_shots() -> list:
    """
    Return all final games that have a LiveStats URL but either:
    - zero rows in shot_chart, OR
    - shots exist but all have NULL clock (need clock backfill)
    """
    log.info("Querying games missing shot data or clock...")

    query = (
        game_db.table("game_schedule")
        .select('game_key, competitionname, hometeam, awayteam, league_id, "LiveStats URL"')
        .eq("status", "final")
        .not_.is_('"LiveStats URL"', "null")
    )
    if FILTER_LEAGUE_ID:
        query = query.eq("league_id", FILTER_LEAGUE_ID)

    result = query.execute()
    all_final = result.data or []
    log.info("Found %d final games with a LiveStats URL", len(all_final))

    # Games with at least one shot that has a clock value — fully done
    clocked_result = (
        game_db.table("shot_chart")
        .select("game_key")
        .not_.is_("clock", "null")
        .execute()
    )
    games_with_clock = {row["game_key"] for row in (clocked_result.data or [])}

    missing = [g for g in all_final if g["game_key"] not in games_with_clock]
    log.info("%d games need shot/clock backfill", len(missing))
    return missing[:LIMIT]


def build_roster_map_from_db(game_key: str) -> dict:
    """
    Build {(side, pno_int): player_id} from player_stats rows already in DB.
    We join on full_name by fetching all player_stats for the game and then
    cross-referencing with the JSON pl dict in the caller.
    Returns a flat name -> player_id map; caller does the pno linking.
    """
    result = (
        game_db.table("player_stats")
        .select("player_id, full_name, side, shirtnumber")
        .eq("game_key", game_key)
        .execute()
    )
    # name_map: (side, normalised_full_name) -> player_id
    name_map = {}
    for row in result.data or []:
        if row.get("player_id") and row.get("full_name"):
            key = (row["side"], row["full_name"].strip().lower())
            name_map[key] = row["player_id"]
    return name_map


def extract_numeric_id(url: str) -> str | None:
    if not url:
        return None
    return url.rstrip("/").split("/")[-1]


def fetch_json(numeric_id: str) -> dict | None:
    url = f"{LIVESTATS_BASE}/{numeric_id}/data.json"
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json()
        log.warning("HTTP %d for %s", r.status_code, url)
        return None
    except Exception as e:
        log.error("Request failed for %s: %s", url, e)
        return None


def parse_shots_for_game(game: dict, data: dict) -> list:
    """
    Build shot records from JSON, linking player_id via name_map from DB.
    Clock is sourced from the PBP events (shots have no clock field in the JSON).
    """
    game_key = game["game_key"]
    league_id = game["league_id"]
    teams = data.get("tm", {})

    # Build PBP clock lookup: actionNumber -> clock string
    pbp_clock_map = {}
    for event in data.get("pbp", []):
        an = event.get("actionNumber")
        cl = event.get("clock")
        if an is not None and cl:
            pbp_clock_map[an] = cl

    # Load existing player_stats for name->player_id mapping
    name_map = build_roster_map_from_db(game_key)

    # Get team_ids from ref_db
    team_id_cache = {}
    for side, team in teams.items():
        team_name = team.get("name", "")
        if not team_name:
            continue
        res = ref_db.table("teams").select("team_id").eq("league_id", league_id).eq("name", team_name).execute()
        if res.data:
            team_id_cache[side] = res.data[0]["team_id"]

    # Build pno -> player_id roster_map using JSON pl dict + name_map
    roster_map = {}  # (side, pno_int) -> player_id
    for side, team in teams.items():
        for pid, player in team.get("pl", {}).items():
            full_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
            key = (side, full_name.lower())
            player_id = name_map.get(key)
            if player_id:
                try:
                    roster_map[(side, int(pid))] = player_id
                except (ValueError, TypeError):
                    pass

    shot_records = []
    for side, team in teams.items():
        team_id = team_id_cache.get(side)
        team_shots = team.get("shot") or []
        log.info("  Side %s: %d shots", side, len(team_shots))

        for s in team_shots:
            action_number = s.get("actionNumber")
            if action_number is None:
                continue

            pno_raw = s.get("pno")
            try:
                pno = int(pno_raw) if pno_raw is not None else None
            except (ValueError, TypeError):
                pno = None

            linked_player_id = roster_map.get((side, pno)) if pno is not None else None

            record = {
                "league_id": league_id,
                "game_key": game_key,
                "team_id": team_id,
                "player_id": linked_player_id,
                "player_name": s.get("player"),
                "team_no": s.get("tno"),
                "period": s.get("per"),
                "shot_type": s.get("actionType"),
                "sub_type": s.get("subType"),
                "success": s.get("r") == 1,
                "x": s.get("x"),
                "y": s.get("y"),
                "action_number": action_number,
                "clock": pbp_clock_map.get(action_number),
            }
            shot_records.append(record)

    return shot_records


def run_backfill():
    log.info("=== Shot Chart Backfill %s===", "(DRY RUN) " if DRY_RUN else "")

    games = get_games_missing_shots()
    if not games:
        log.info("Nothing to backfill. All done.")
        return

    total_shots = 0
    processed = 0
    failed = 0

    for i, game in enumerate(games, 1):
        game_key = game["game_key"]
        url = game.get("LiveStats URL", "")
        numeric_id = extract_numeric_id(url)

        log.info("[%d/%d] %s vs %s  game_key=%s",
                 i, len(games), game.get("hometeam"), game.get("awayteam"), game_key)

        if not numeric_id:
            log.warning("  No numeric ID — skipping")
            failed += 1
            continue

        data = fetch_json(numeric_id)
        if not data:
            log.warning("  Could not fetch JSON — skipping")
            failed += 1
            continue

        shot_records = parse_shots_for_game(game, data)
        log.info("  %d shot records prepared", len(shot_records))

        if not shot_records:
            log.info("  No shots in JSON for this game")
            processed += 1
            continue

        if DRY_RUN:
            log.info("  DRY RUN — skipping insert")
        else:
            try:
                game_db.table("shot_chart").upsert(
                    shot_records,
                    on_conflict="game_key,action_number"
                ).execute()
                log.info("  Upserted %d shots", len(shot_records))
                total_shots += len(shot_records)
            except Exception as e:
                log.error("  Upsert failed: %s", e)
                failed += 1
                continue

        processed += 1

    log.info("=== Backfill complete: %d games processed, %d total shots inserted, %d failed ===",
             processed, total_shots, failed)


if __name__ == "__main__":
    run_backfill()
