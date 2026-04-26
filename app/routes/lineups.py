from flask import Blueprint, jsonify, request
from app.utils.chat_data import supabase
import logging

lineups_bp = Blueprint("lineups", __name__)
log = logging.getLogger("lineups")


def _valid_only_flag() -> bool:
    return request.args.get("valid_only", "").lower() == "true"


# ---------------------------------------------------------------------------
# GET /api/lineups/<game_key>
# ---------------------------------------------------------------------------

@lineups_bp.route("/api/lineups/<game_key>", methods=["GET"])
def get_lineups(game_key: str):
    """
    Return all lineup stints for a game.

    Query params:
      team_id     (str)  — filter to a single team
      valid_only  (bool) — if 'true', exclude is_valid_lineup=False rows
    """
    if not game_key or not game_key.strip():
        return jsonify({"message": "game_key is required"}), 400
    try:
        query = (
            supabase.table("lineup_stints")
            .select("*")
            .eq("game_key", game_key)
        )

        team_id = request.args.get("team_id", "").strip()
        if team_id:
            query = query.eq("team_id", team_id)

        if _valid_only_flag():
            query = query.eq("is_valid_lineup", True)

        result = query.order("start_game_secs").execute()
        return jsonify(result.data), 200

    except Exception as exc:
        log.error("GET /api/lineups/%s error: %s", game_key, exc, exc_info=True)
        return jsonify({"message": str(exc)}), 500


# ---------------------------------------------------------------------------
# GET /api/on-off/<game_key>
# ---------------------------------------------------------------------------

@lineups_bp.route("/api/on-off/<game_key>", methods=["GET"])
def get_on_off_game(game_key: str):
    """
    Per-player on/off aggregation for a single game.

    Aggregates player_on_court_stints by player and returns:
      player_id, player_name, team_id, stint_count,
      seconds_played, points_for, points_against, net_points,
      possessions_for, possessions_against

    Query params:
      team_id     (str)  — filter to a single team
      valid_only  (bool) — if 'true', exclude stints with is_valid_lineup=False
                           (the flag is stored on lineup_stints, not here, but
                            lineup_key rows carry it via the stint join; we filter
                            by fetching only rows whose stint has is_valid_lineup=True
                            via a sub-select through lineup_stints.id = stint_id)
    """
    if not game_key or not game_key.strip():
        return jsonify({"message": "game_key is required"}), 400
    try:
        query = (
            supabase.table("player_on_court_stints")
            .select(
                "player_id,player_name,team_id,shirt_number,"
                "seconds_played,points_for,points_against,"
                "possessions_for,possessions_against,stint_id"
            )
            .eq("game_key", game_key)
        )

        team_id = request.args.get("team_id", "").strip()
        if team_id:
            query = query.eq("team_id", team_id)

        rows = query.execute().data or []

        if _valid_only_flag() and rows:
            valid_stint_ids = _fetch_valid_stint_ids(game_key)
            rows = [r for r in rows if r.get("stint_id") in valid_stint_ids]

        aggregated = _aggregate_player_rows(rows)
        return jsonify(aggregated), 200

    except Exception as exc:
        log.error("GET /api/on-off/%s error: %s", game_key, exc, exc_info=True)
        return jsonify({"message": str(exc)}), 500


# ---------------------------------------------------------------------------
# GET /api/on-off/player/<player_id>
# ---------------------------------------------------------------------------

@lineups_bp.route("/api/on-off/player/<player_id>", methods=["GET"])
def get_on_off_player(player_id: str):
    """
    Cross-game aggregated on/off stats for a single player.

    Returns one summary object with totals across all games the player
    appears in, plus a per-game breakdown.

    Query params:
      valid_only  (bool) — if 'true', exclude invalid-lineup stints
    """
    if not player_id or not player_id.strip():
        return jsonify({"message": "player_id is required"}), 400
    try:
        query = (
            supabase.table("player_on_court_stints")
            .select(
                "game_key,player_id,player_name,team_id,shirt_number,"
                "seconds_played,points_for,points_against,"
                "possessions_for,possessions_against,stint_id"
            )
            .eq("player_id", player_id)
        )

        rows = query.execute().data or []

        if _valid_only_flag() and rows:
            game_keys = list({r["game_key"] for r in rows})
            valid_ids: set = set()
            for gk in game_keys:
                valid_ids |= _fetch_valid_stint_ids(gk)
            rows = [r for r in rows if r.get("stint_id") in valid_ids]

        if not rows:
            return jsonify({"player_id": player_id, "games": [], "totals": {}}), 200

        per_game: dict = {}
        for r in rows:
            gk = r["game_key"]
            if gk not in per_game:
                per_game[gk] = []
            per_game[gk].append(r)

        games_out = []
        for gk, game_rows in per_game.items():
            agg = _aggregate_player_rows(game_rows)
            if agg:
                entry = _merge_agg_buckets(agg)
                entry["game_key"] = gk
                games_out.append(entry)

        # Merge ALL per-(player_id,team_id) buckets into one season total so
        # players who changed teams during the season don't lose rows.
        all_agg = _aggregate_player_rows(rows)
        totals = _merge_agg_buckets(all_agg) if all_agg else {}

        return jsonify({
            "player_id": player_id,
            "player_name": totals.get("player_name"),
            "team_id": totals.get("team_id"),
            "totals": totals,
            "games": sorted(games_out, key=lambda g: g.get("game_key", "")),
        }), 200

    except Exception as exc:
        log.error("GET /api/on-off/player/%s error: %s", player_id, exc, exc_info=True)
        return jsonify({"message": str(exc)}), 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_valid_stint_ids(game_key: str) -> set:
    """Return the set of lineup_stints IDs that have is_valid_lineup=True."""
    try:
        res = (
            supabase.table("lineup_stints")
            .select("id")
            .eq("game_key", game_key)
            .eq("is_valid_lineup", True)
            .execute()
        )
        return {r["id"] for r in (res.data or [])}
    except Exception as exc:
        log.warning("Could not fetch valid stint IDs for game=%s: %s", game_key, exc)
        return set()


def _merge_agg_buckets(buckets: list) -> dict:
    """
    Collapse multiple per-(player_id,team_id) aggregated buckets into a single
    totals dict.  Needed for players who changed teams mid-season so that season
    totals are complete even when _aggregate_player_rows returns >1 bucket.
    If team_id is consistent across all buckets it is preserved; otherwise None.
    """
    if not buckets:
        return {}
    if len(buckets) == 1:
        return dict(buckets[0])
    team_ids = {b.get("team_id") for b in buckets}
    merged: dict = {
        "player_id": buckets[0].get("player_id"),
        "player_name": buckets[0].get("player_name"),
        "shirt_number": buckets[0].get("shirt_number"),
        "team_id": team_ids.pop() if len(team_ids) == 1 else None,
        "stint_count": sum(b["stint_count"] for b in buckets),
        "seconds_played": sum(b["seconds_played"] for b in buckets),
        "points_for": sum(b["points_for"] for b in buckets),
        "points_against": sum(b["points_against"] for b in buckets),
        "possessions_for": sum(b["possessions_for"] for b in buckets),
        "possessions_against": sum(b["possessions_against"] for b in buckets),
    }
    merged["net_points"] = merged["points_for"] - merged["points_against"]
    return merged


def _aggregate_player_rows(rows: list) -> list:
    """
    Group rows by (player_id, team_id) and sum numeric stats.
    Returns a list of aggregated dicts sorted by seconds_played desc.
    """
    buckets: dict = {}
    for r in rows:
        key = (r.get("player_id") or "", r.get("team_id") or "")
        if key not in buckets:
            buckets[key] = {
                "player_id": r.get("player_id"),
                "player_name": r.get("player_name"),
                "shirt_number": r.get("shirt_number"),
                "team_id": r.get("team_id"),
                "stint_count": 0,
                "seconds_played": 0,
                "points_for": 0,
                "points_against": 0,
                "possessions_for": 0,
                "possessions_against": 0,
            }
        b = buckets[key]
        b["stint_count"] += 1
        b["seconds_played"] += r.get("seconds_played") or 0
        b["points_for"] += r.get("points_for") or 0
        b["points_against"] += r.get("points_against") or 0
        b["possessions_for"] += r.get("possessions_for") or 0
        b["possessions_against"] += r.get("possessions_against") or 0

    result = []
    for b in buckets.values():
        b["net_points"] = b["points_for"] - b["points_against"]
        result.append(b)

    result.sort(key=lambda x: x["seconds_played"], reverse=True)
    return result
