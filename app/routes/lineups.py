from flask import Blueprint, jsonify, request
from app.utils.chat_data import supabase
import logging

lineups_bp = Blueprint("lineups", __name__)
log = logging.getLogger("lineups")


def _valid_only_flag() -> bool:
    return request.args.get("valid_only", "").lower() == "true"


# ---------------------------------------------------------------------------
# GET /api/lineups/top  — cross-game best lineup rankings
# ---------------------------------------------------------------------------

@lineups_bp.route("/api/lineups/top", methods=["GET"])
def get_top_lineups():
    """
    Aggregate lineup_stints across games, grouped by (lineup_key, team_id).

    Returns ranked 5-man units with cumulative totals and efficiency ratings.

    Query params:
      league_id   (str)  — scope results to a specific season/league
      min_seconds (int)  — exclude lineups with less total seconds (default 0)
      valid_only  (bool) — if 'true', exclude is_valid_lineup=False stints
    """
    try:
        query = (
            supabase.table("lineup_stints")
            .select(
                "lineup_key,lineup_player_ids,lineup_names,team_id,league_id,"
                "seconds_played,points_for,points_against,"
                "possessions_for,possessions_against,is_valid_lineup"
            )
        )

        league_id = request.args.get("league_id", "").strip()
        if league_id:
            query = query.eq("league_id", league_id)

        if _valid_only_flag():
            query = query.eq("is_valid_lineup", True)

        rows = query.execute().data or []

        try:
            min_seconds = int(request.args.get("min_seconds", 0))
        except (ValueError, TypeError):
            min_seconds = 0

        aggregated = _aggregate_lineup_rows(rows, min_seconds, league_id or None)
        return jsonify(aggregated), 200

    except Exception as exc:
        log.error("GET /api/lineups/top error: %s", exc, exc_info=True)
        return jsonify({"message": str(exc)}), 500


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

    Returns on/off/diff blocks with advanced percentage metrics, plus
    player metadata.

    Response shape:
      {
        player_id, player_name, team_id,
        on:   { ortg, drtg, nrtg, oreb_pct, dreb_pct, reb_pct, ast_pct,
                blk_pct, stl_pct, tov_pct, seconds_played },
        off:  { ... same keys ... },
        diff: { ... ON minus OFF for each metric ... }
      }

    When there are no stints for the player, on/off/diff are all null.

    Important notes for consumers
    ------------------------------
    - `off.seconds_played` is always 0.  The total game duration is not
      stored in `team_stats`, so we cannot derive the exact time the player
      spent on the bench.  `on.seconds_played` is accurate (summed from
      player_on_court_stints).

    - Opponent-derived percentages (oreb_pct, dreb_pct, reb_pct, blk_pct,
      stl_pct) are approximations for the ON block.  Because the DB does not
      record per-stint opponent box-score stats, the opponent's game totals
      are scaled by possession share (opp_poss_on / opp_poss_total) to
      estimate opponent activity during the player's on-court time.  OFF
      opponent values are the complementary remainder.  These are standard
      approximations for on/off analysis but are not exact.

    - Rate metrics (ortg, drtg, blk_pct, stl_pct) return null rather than
      a number when the denominator (possessions or opponent attempts) is
      zero for a given block.

    Query params:
      valid_only  (bool) — if 'true', exclude invalid-lineup stints
    """
    if not player_id or not player_id.strip():
        return jsonify({"message": "player_id is required"}), 400
    try:
        # ------------------------------------------------------------------
        # 1. Fetch player_on_court_stints for this player
        # ------------------------------------------------------------------
        poc_rows = (
            supabase.table("player_on_court_stints")
            .select(
                "game_key,player_id,player_name,team_id,shirt_number,"
                "seconds_played,points_for,points_against,"
                "possessions_for,possessions_against,stint_id"
            )
            .eq("player_id", player_id)
            .execute()
            .data or []
        )

        if _valid_only_flag() and poc_rows:
            game_keys_set = {r["game_key"] for r in poc_rows}
            valid_ids: set = set()
            for gk in game_keys_set:
                valid_ids |= _fetch_valid_stint_ids(gk)
            poc_rows = [r for r in poc_rows if r.get("stint_id") in valid_ids]

        if not poc_rows:
            return jsonify({"player_id": player_id, "on": None, "off": None, "diff": None}), 200

        player_name = poc_rows[0].get("player_name")
        # Determine team_id — None if player changed teams
        team_ids_seen = {r["team_id"] for r in poc_rows}
        team_id = team_ids_seen.pop() if len(team_ids_seen) == 1 else None

        # ------------------------------------------------------------------
        # 2. Fetch lineup_stints for those stint_ids to get team box stats
        #    recorded during the player's ON-court time
        # ------------------------------------------------------------------
        stint_ids = [r["stint_id"] for r in poc_rows if r.get("stint_id")]
        ls_map: dict = {}
        if stint_ids:
            # Supabase .in_() is limited; batch if needed
            BATCH = 500
            for i in range(0, len(stint_ids), BATCH):
                batch = stint_ids[i: i + BATCH]
                ls_rows = (
                    supabase.table("lineup_stints")
                    .select(
                        "id,oreb,dreb,assists,turnovers,steals,blocks,"
                        "fg2_made,fg2_attempted,fg3_made,fg3_attempted,"
                        "ft_made,ft_attempted,"
                        "possessions_for,possessions_against,points_for,points_against"
                    )
                    .in_("id", batch)
                    .execute()
                    .data or []
                )
                for ls in ls_rows:
                    ls_map[ls["id"]] = ls

        # ------------------------------------------------------------------
        # 3. Fetch team_stats for every game the player appeared in
        #    (both player's team and opponent team rows)
        # ------------------------------------------------------------------
        game_keys = list({r["game_key"] for r in poc_rows})
        ts_rows: list = []
        BATCH = 200
        for i in range(0, len(game_keys), BATCH):
            batch = game_keys[i: i + BATCH]
            ts_rows += (
                supabase.table("team_stats")
                .select(
                    "game_key,team_id,"
                    "tot_spoints,opp_points,possessions,opp_possessions,"
                    "tot_sreboundsoffensive,tot_sreboundstotal,"
                    "tot_sassists,tot_sfieldgoalsmade,"
                    "tot_sfieldgoalsattempted,tot_sthreepointersattempted,"
                    "tot_sfreethrowsattempted,tot_sfreethrowsmade,"
                    "tot_sturnovers,tot_ssteals,tot_sblocks"
                )
                .in_("game_key", batch)
                .execute()
                .data or []
            )

        # Build lookup: (game_key, team_id) -> team_stats row
        ts_lookup: dict = {}
        for ts in ts_rows:
            ts_lookup[(ts["game_key"], ts["team_id"])] = ts

        # Build per-game: game_key -> player's team_id in that game
        game_team_map: dict = {}
        for r in poc_rows:
            game_team_map.setdefault(r["game_key"], r["team_id"])

        # ------------------------------------------------------------------
        # 4. Aggregate ON-court team stats from lineup_stints per game,
        #    and accumulate team totals + opponent totals across all games
        # ------------------------------------------------------------------
        on_raw = _empty_raw_counts()
        team_raw = _empty_raw_counts()
        opp_raw = _empty_raw_counts()

        # Also track ON possessions against for opponent scaling per-game
        # (we accumulate from poc_rows which have per-stint possessions_against)
        # Group poc_rows by game so we can compute per-game totals
        poc_by_game: dict = {}
        for r in poc_rows:
            poc_by_game.setdefault(r["game_key"], []).append(r)

        for gk, game_poc in poc_by_game.items():
            player_tid = game_team_map[gk]

            # --- ON-court: sum lineup_stints data for the player's stints ---
            on_game = _empty_raw_counts()
            for poc in game_poc:
                sid = poc.get("stint_id")
                ls = ls_map.get(sid) if sid else None
                if ls:
                    on_game["oreb"]         += ls.get("oreb") or 0
                    on_game["dreb"]         += ls.get("dreb") or 0
                    on_game["ast"]          += ls.get("assists") or 0
                    on_game["tov"]          += ls.get("turnovers") or 0
                    on_game["stl"]          += ls.get("steals") or 0
                    on_game["blk"]          += ls.get("blocks") or 0
                    on_game["fgm"]          += (ls.get("fg2_made") or 0) + (ls.get("fg3_made") or 0)
                    on_game["fga"]          += (ls.get("fg2_attempted") or 0) + (ls.get("fg3_attempted") or 0)
                    on_game["fga2"]         += ls.get("fg2_attempted") or 0
                    on_game["fta"]          += ls.get("ft_attempted") or 0
                    on_game["ftm"]          += ls.get("ft_made") or 0
                # Always accumulate points/possessions from poc (more reliable)
                on_game["pts"]              += poc.get("points_for") or 0
                on_game["pts_against"]      += poc.get("points_against") or 0
                on_game["poss_for"]         += poc.get("possessions_for") or 0
                on_game["poss_against"]     += poc.get("possessions_against") or 0
                on_game["seconds"]          += poc.get("seconds_played") or 0

            _add_raw(on_raw, on_game)

            # --- Team game totals from team_stats ---
            ts = ts_lookup.get((gk, player_tid)) or {}
            team_game = _team_stats_to_raw(ts)
            _add_raw(team_raw, team_game)

            # --- Opponent game totals (other team in same game) ---
            opp_ts = None
            for (gk2, tid2), row in ts_lookup.items():
                if gk2 == gk and tid2 != player_tid:
                    opp_ts = row
                    break
            opp_game = _team_stats_to_raw(opp_ts or {})

            # Scale opponent full-game totals to ON-court time using
            # possession fraction: opp_poss_on / opp_poss_total
            opp_poss_total = opp_game["poss_for"] or opp_game["poss_against"] or 0
            opp_poss_on = on_game["poss_against"]  # opp possessions while player is on
            if opp_poss_total > 0 and opp_poss_on > 0:
                scale = opp_poss_on / opp_poss_total
                on_game["opp_oreb"]     = (opp_game["oreb"] or 0) * scale
                on_game["opp_dreb"]     = (opp_game["dreb"] or 0) * scale
                on_game["opp_fga2"]     = (opp_game["fga2"] or 0) * scale
                on_game["opp_poss"]     = opp_poss_on
            else:
                on_game["opp_oreb"]     = 0.0
                on_game["opp_dreb"]     = 0.0
                on_game["opp_fga2"]     = 0.0
                on_game["opp_poss"]     = 0.0

            # Re-add on_game opp fields to on_raw (already added above without opp)
            on_raw["opp_oreb"]  += on_game["opp_oreb"]
            on_raw["opp_dreb"]  += on_game["opp_dreb"]
            on_raw["opp_fga2"]  += on_game["opp_fga2"]
            on_raw["opp_poss"]  += on_game["opp_poss"]

            _add_raw(opp_raw, opp_game)

        # ------------------------------------------------------------------
        # 5. Compute OFF-court raw counts: team_total - ON
        # ------------------------------------------------------------------
        off_raw = _subtract_raw(team_raw, on_raw)

        # OFF opponent: opp_total - ON_opp (scaled portion already in on_raw)
        off_raw["opp_oreb"]     = max(0.0, opp_raw["oreb"] - on_raw["opp_oreb"])
        off_raw["opp_dreb"]     = max(0.0, opp_raw["dreb"] - on_raw["opp_dreb"])
        off_raw["opp_fga2"]     = max(0.0, opp_raw["fga2"] - on_raw["opp_fga2"])
        off_raw["opp_poss"]     = max(0.0, opp_raw["poss_for"] - on_raw["opp_poss"])

        # ------------------------------------------------------------------
        # 6. Compute advanced metrics for ON and OFF blocks
        # ------------------------------------------------------------------
        on_block  = _compute_advanced_block(on_raw)
        off_block = _compute_advanced_block(off_raw)
        diff_block = _compute_diff_block(on_block, off_block)

        return jsonify({
            "player_id":   player_id,
            "player_name": player_name,
            "team_id":     team_id,
            "on":          on_block,
            "off":         off_block,
            "diff":        diff_block,
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


def _aggregate_lineup_rows(
    rows: list,
    min_seconds: int = 0,
    league_id: str | None = None,
) -> list:
    """
    Group lineup_stints rows by (lineup_key, team_id) and sum numeric stats.

    Computes efficiency ratings (per-100-possessions) where possession data
    is available.  Returns results sorted by net_rating desc, then
    seconds_played desc.

    Args:
        rows:        Raw lineup_stints rows from Supabase.
        min_seconds: Exclude buckets whose total seconds_played < this value.
        league_id:   The league_id filter that was applied to the query.
                     When provided it is echoed back in every result row so
                     the field is authoritative.  When None (no filter) the
                     field is set to None to avoid implying a single league
                     when the data may span multiple leagues.
    """
    buckets: dict = {}
    for r in rows:
        key = (r.get("lineup_key") or "", r.get("team_id") or "")
        if key not in buckets:
            buckets[key] = {
                "lineup_key": r.get("lineup_key"),
                "lineup_player_ids": r.get("lineup_player_ids"),
                "lineup_names": r.get("lineup_names"),
                "team_id": r.get("team_id"),
                "league_id": league_id,
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
        if b["seconds_played"] < min_seconds:
            continue

        b["net_points"] = b["points_for"] - b["points_against"]

        pf = b["possessions_for"]
        pa = b["possessions_against"]
        b["off_rating"] = round(b["points_for"] / pf * 100, 1) if pf > 0 else None
        b["def_rating"] = round(b["points_against"] / pa * 100, 1) if pa > 0 else None
        if b["off_rating"] is not None and b["def_rating"] is not None:
            b["net_rating"] = round(b["off_rating"] - b["def_rating"], 1)
        else:
            b["net_rating"] = None

        result.append(b)

    result.sort(
        key=lambda x: (
            x["net_rating"] if x["net_rating"] is not None else float("-inf"),
            x["seconds_played"],
        ),
        reverse=True,
    )
    return result


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


# ---------------------------------------------------------------------------
# On/off advanced stats helpers
# ---------------------------------------------------------------------------

def _empty_raw_counts() -> dict:
    """Return a zeroed dict of all raw counting stats used for on/off analysis."""
    return {
        "seconds":      0,
        "pts":          0,
        "pts_against":  0,
        "poss_for":     0,
        "poss_against": 0,
        "oreb":         0.0,
        "dreb":         0.0,
        "ast":          0.0,
        "tov":          0.0,
        "stl":          0.0,
        "blk":          0.0,
        "fgm":          0.0,
        "fga":          0.0,
        "fga2":         0.0,
        "fta":          0.0,
        "ftm":          0.0,
        "opp_oreb":     0.0,
        "opp_dreb":     0.0,
        "opp_fga2":     0.0,
        "opp_poss":     0.0,
    }


def _add_raw(dest: dict, src: dict) -> None:
    """Add all fields from src into dest in place (numeric accumulation)."""
    for k in dest:
        dest[k] = dest.get(k, 0) + src.get(k, 0)


def _subtract_raw(team: dict, on: dict) -> dict:
    """
    Compute OFF = team_total - ON for the counting stats that come from
    the team's own box score (not the opp_* fields, which are handled
    separately by the caller).
    """
    own_keys = [
        "seconds", "pts", "pts_against", "poss_for", "poss_against",
        "oreb", "dreb", "ast", "tov", "stl", "blk",
        "fgm", "fga", "fga2", "fta", "ftm",
    ]
    off = _empty_raw_counts()
    for k in own_keys:
        off[k] = max(0.0, (team.get(k) or 0) - (on.get(k) or 0))
    return off


def _team_stats_to_raw(ts: dict) -> dict:
    """
    Convert a team_stats row into a raw counts dict compatible with
    _empty_raw_counts().

    oreb: tot_sreboundsoffensive
    dreb: tot_sreboundstotal - tot_sreboundsoffensive
    fga2: tot_sfieldgoalsattempted - tot_sthreepointersattempted
    pts_against: opp_points (opponent score recorded on team_stats row)
    """
    oreb = ts.get("tot_sreboundsoffensive") or 0
    reb  = ts.get("tot_sreboundstotal") or 0
    dreb = max(0, reb - oreb)
    fga  = ts.get("tot_sfieldgoalsattempted") or 0
    tpa  = ts.get("tot_sthreepointersattempted") or 0
    fga2 = max(0, fga - tpa)
    return {
        "seconds":      0,
        "pts":          ts.get("tot_spoints") or 0,
        "pts_against":  ts.get("opp_points") or 0,
        "poss_for":     ts.get("possessions") or 0,
        "poss_against": ts.get("opp_possessions") or 0,
        "oreb":         float(oreb),
        "dreb":         float(dreb),
        "ast":          float(ts.get("tot_sassists") or 0),
        "tov":          float(ts.get("tot_sturnovers") or 0),
        "stl":          float(ts.get("tot_ssteals") or 0),
        "blk":          float(ts.get("tot_sblocks") or 0),
        "fgm":          float(ts.get("tot_sfieldgoalsmade") or 0),
        "fga":          float(fga),
        "fga2":         float(fga2),
        "fta":          float(ts.get("tot_sfreethrowsattempted") or 0),
        "ftm":          float(ts.get("tot_sfreethrowsmade") or 0),
        "opp_oreb":     0.0,
        "opp_dreb":     0.0,
        "opp_fga2":     0.0,
        "opp_poss":     0.0,
    }


def _safe_pct(n, d) -> float | None:
    """Return n/d*100 rounded to 1 decimal, or None when denominator is 0."""
    if not d:
        return None
    return round(n / d * 100, 1)


def _safe_rating(pts, poss) -> float | None:
    """Return pts/poss*100 rounded to 1 decimal, or None when poss is 0."""
    if not poss:
        return None
    return round(pts / poss * 100, 1)


def _compute_advanced_block(raw: dict) -> dict:
    """
    Compute all advanced metrics from a raw counts dict.

    Formulas (task spec):
      ORTG      = pts / poss_for * 100
      DRTG      = pts_against / poss_against * 100
      NRTG      = ORTG - DRTG
      OREB%     = oreb / (oreb + opp_dreb) * 100
      DREB%     = dreb / (dreb + opp_oreb) * 100
      REB%      = (oreb+dreb) / (oreb+dreb+opp_oreb+opp_dreb) * 100
      AST%      = ast / fgm * 100
      BLK%      = blk / opp_fga2 * 100
      STL%      = stl / opp_poss * 100
      TOV%      = tov / (fga + 0.44*fta + tov) * 100
    """
    ortg = _safe_rating(raw["pts"], raw["poss_for"])
    drtg = _safe_rating(raw["pts_against"], raw["poss_against"])

    if ortg is not None and drtg is not None:
        nrtg = round(ortg - drtg, 1)
    else:
        nrtg = None

    oreb = raw["oreb"]
    dreb = raw["dreb"]
    opp_oreb = raw["opp_oreb"]
    opp_dreb = raw["opp_dreb"]

    oreb_pct = _safe_pct(oreb, oreb + opp_dreb)
    dreb_pct = _safe_pct(dreb, dreb + opp_oreb)
    reb_pct  = _safe_pct(oreb + dreb, oreb + dreb + opp_oreb + opp_dreb)
    ast_pct  = _safe_pct(raw["ast"], raw["fgm"])
    blk_pct  = _safe_pct(raw["blk"], raw["opp_fga2"])
    stl_pct  = _safe_pct(raw["stl"], raw["opp_poss"])
    tov_denom = raw["fga"] + 0.44 * raw["fta"] + raw["tov"]
    tov_pct  = _safe_pct(raw["tov"], tov_denom)

    return {
        "seconds_played": int(raw["seconds"]),
        "ortg":           ortg,
        "drtg":           drtg,
        "nrtg":           nrtg,
        "oreb_pct":       oreb_pct,
        "dreb_pct":       dreb_pct,
        "reb_pct":        reb_pct,
        "ast_pct":        ast_pct,
        "blk_pct":        blk_pct,
        "stl_pct":        stl_pct,
        "tov_pct":        tov_pct,
    }


def _diff_metric(on_val, off_val):
    """ON minus OFF; None when either value is None."""
    if on_val is None or off_val is None:
        return None
    return round(on_val - off_val, 1)


def _compute_diff_block(on: dict, off: dict) -> dict:
    """Return a diff block: ON minus OFF for every metric key."""
    keys = ["ortg", "drtg", "nrtg", "oreb_pct", "dreb_pct", "reb_pct",
            "ast_pct", "blk_pct", "stl_pct", "tov_pct"]
    diff = {"seconds_played": int(on["seconds_played"]) - int(off["seconds_played"])}
    for k in keys:
        diff[k] = _diff_metric(on.get(k), off.get(k))
    return diff
