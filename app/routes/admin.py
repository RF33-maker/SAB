import os
import logging

from flask import Blueprint, jsonify, request
from app.utils.chat_data import supabase

admin_bp = Blueprint("admin", __name__)
log = logging.getLogger("admin")

ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "")


def _check_auth() -> bool:
    provided = request.headers.get("X-Admin-Key", "")
    return bool(ADMIN_SECRET) and provided == ADMIN_SECRET


def _game_already_processed(game_key: str) -> bool:
    try:
        res = (
            supabase.table("lineup_stints")
            .select("id")
            .eq("game_key", game_key)
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception as exc:
        log.warning("Could not check lineup_stints for %s: %s", game_key, exc)
        return False


def _count_stints(game_key: str) -> int:
    """Return the current row count in lineup_stints for this game."""
    try:
        res = (
            supabase.table("lineup_stints")
            .select("id", count="exact")
            .eq("game_key", game_key)
            .execute()
        )
        return res.count or 0
    except Exception as exc:
        log.warning("Could not count stints for game=%s: %s", game_key, exc)
        return 0


@admin_bp.route("/api/admin/backfill-lineups", methods=["POST"])
def backfill_lineups():
    """
    Trigger lineup backfill for all games in the database that have
    play-by-play events.

    Auth:
      X-Admin-Key header must match the ADMIN_SECRET environment variable.

    Query params:
      league_id  (str)  — scope to a specific league UUID
      force      (bool) — if 'true', reprocess games that already have lineup stints

    Response JSON:
      games_processed  int   — games for which build_lineups_for_game succeeded
      games_skipped    int   — games skipped (already processed, or no data returned)
      stints_written   int   — total lineup_stints rows written across processed games
      errors           list  — per-game error dicts for both hard exceptions and soft failures
    """
    if not _check_auth():
        return jsonify({"message": "Forbidden"}), 403

    league_id_filter = request.args.get("league_id", "").strip() or None
    force = request.args.get("force", "").lower() == "true"

    from app.utils.lineup_builder import build_lineups_for_game

    try:
        events_query = supabase.table("live_events").select("game_key, league_id")
        if league_id_filter:
            events_query = events_query.eq("league_id", league_id_filter)
        events_res = events_query.execute()
    except Exception as exc:
        log.error("Failed to query live_events: %s", exc, exc_info=True)
        return jsonify({"message": f"Failed to query live_events: {exc}"}), 500

    seen: set = set()
    games: list = []
    for row in (events_res.data or []):
        gk = row.get("game_key")
        if not gk or gk in seen:
            continue
        seen.add(gk)
        lid = row.get("league_id")
        if not lid:
            try:
                sched = (
                    supabase.table("game_schedule")
                    .select("league_id")
                    .eq("game_key", gk)
                    .limit(1)
                    .execute()
                )
                lid = (sched.data[0].get("league_id") if sched.data else None)
            except Exception as exc:
                log.warning("Could not fetch league_id for game=%s: %s", gk, exc)
        if lid:
            games.append({"game_key": gk, "league_id": lid})
        else:
            log.warning("game=%s: no league_id found, skipping", gk)

    log.info("Backfill triggered: %d qualifying games (force=%s)", len(games), force)

    games_processed = 0
    games_skipped = 0
    stints_written = 0
    error_list: list = []

    for game in games:
        gk = game["game_key"]
        lid = game["league_id"]

        if not force and _game_already_processed(gk):
            log.info("game=%s already processed, skipping (pass force=true to reprocess)", gk)
            games_skipped += 1
            continue

        log.info("Processing game=%s league=%s", gk, lid)
        try:
            ok = build_lineups_for_game(game_key=gk, league_id=lid)
            if ok:
                games_processed += 1
                stints_written += _count_stints(gk)
            else:
                games_skipped += 1
                error_list.append({
                    "game_key": gk,
                    "error": "build_lineups_for_game returned False (no stints generated or missing data)",
                })
        except Exception as exc:
            log.error("Failed to build lineups for game=%s: %s", gk, exc, exc_info=True)
            error_list.append({"game_key": gk, "error": str(exc)})

    return jsonify({
        "games_processed": games_processed,
        "games_skipped": games_skipped,
        "stints_written": stints_written,
        "errors": error_list,
    }), 200
