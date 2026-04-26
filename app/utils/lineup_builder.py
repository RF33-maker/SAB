"""
lineup_builder.py
-----------------
Standalone utility module for reconstructing 5-man lineups from raw PBP events
and substitution data for a single game.

Algorithm overview:
  1. Load game_rosters for the game → identify starting fives per team.
  2. Load live_events ordered by action_number.
  3. Walk events in order, tracking the active 5-man lineup per team.
  4. Open a new stint when the game starts or when a substitution completes.
  5. Close the current stint on substitution, period boundary, or end of game.
  6. Attribute scoring, shooting, rebound, turnover, and foul events to the
     correct team's active lineup (and opponent's lineup for points_against).
  7. Write completed lineup_stints rows to Supabase.
  8. Expand each stint into per-player rows in player_on_court_stints.

lineup_key generation:
  - PRIMARY: if all 5 active players have a non-null player_id, sort UUIDs and
    join with "|". Example: "uuid1|uuid2|uuid3|uuid4|uuid5"
  - FALLBACK: sort concatenated tokens of "team_id:player_name:shirt_number"
    for each of the 5 players. This is stable until player_ids are resolved.
    Once player_ids are available the key can be regenerated without schema changes.
"""

import os
import logging
from typing import Optional

log = logging.getLogger("lineup_builder")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

# ---------------------------------------------------------------------------
# Supabase client (lazy init to avoid import-time side effects)
# ---------------------------------------------------------------------------
_game_db = None


def _get_db():
    global _game_db
    if _game_db is None:
        from supabase import create_client
        from supabase.lib.client_options import ClientOptions
        _game_db = create_client(
            SUPABASE_URL,
            SUPABASE_KEY,
            options=ClientOptions(schema=DB_SCHEMA),
        )
    return _game_db


# ---------------------------------------------------------------------------
# Clock / time helpers
# ---------------------------------------------------------------------------

def _clock_to_secs(clock: str) -> int:
    """Convert a clock string 'MM:SS' or 'M:SS' to seconds remaining."""
    try:
        parts = clock.strip().split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except Exception:
        return 0


def _period_start_secs(period: int) -> int:
    """Return cumulative game seconds elapsed at the start of *period*."""
    if period <= 4:
        return (period - 1) * 600  # 10-minute quarters
    else:
        return 4 * 600 + (period - 5) * 300  # 5-minute OT periods


def _period_duration(period: int) -> int:
    return 600 if period <= 4 else 300


def _event_game_secs(period: int, clock: Optional[str]) -> int:
    """Return total elapsed game seconds at the moment of this event."""
    if not period:
        return 0
    duration = _period_duration(period)
    secs_remaining = _clock_to_secs(clock) if clock else 0
    return _period_start_secs(period) + (duration - secs_remaining)


# ---------------------------------------------------------------------------
# lineup_key generation
# ---------------------------------------------------------------------------

def build_lineup_key(players: list, team_id: str) -> str:
    """
    Build a deterministic lineup key for a 5-man group.

    Primary (preferred): sort the 5 player_id UUIDs and join with "|".
    Fallback: sort tokens of "player_id_or_name:shirt_number" per player.

    Parameters
    ----------
    players : list of dicts, each with keys: player_id, player_name, shirt_number
    team_id : str  UUID of the team (used in fallback tokens)

    Returns
    -------
    str  The deterministic lineup key.
    """
    player_ids = [p.get("player_id") for p in players]
    if all(pid for pid in player_ids):
        return "|".join(sorted(str(pid) for pid in player_ids))

    # Fallback: stable token per player using whatever identifiers are available
    tokens = []
    for p in players:
        pid = p.get("player_id") or ""
        name = (p.get("player_name") or "").strip().lower()
        shirt = (p.get("shirt_number") or "").strip()
        tokens.append(f"{team_id}:{pid or name}:{shirt}")
    return "|".join(sorted(tokens))


# ---------------------------------------------------------------------------
# Core data loaders
# ---------------------------------------------------------------------------

def load_game_rosters(game_key: str) -> dict:
    """
    Load roster entries for *game_key* from game_rosters table.

    Returns
    -------
    dict  keyed by team_id → list of player dicts
          {player_name, shirt_number, pno, starter, active, player_id, team_no}
    """
    db = _get_db()
    try:
        res = db.table("game_rosters").select(
            "team_id, team_no, player_name, shirt_number, pno, starter, active, player_id"
        ).eq("game_key", game_key).execute()
    except Exception as exc:
        log.error("Failed to load game_rosters for %s: %s", game_key, exc)
        return {}

    rosters: dict = {}
    for row in res.data:
        tid = row["team_id"]
        if tid not in rosters:
            rosters[tid] = []
        rosters[tid].append(row)

    log.info("Loaded rosters for %d teams in game %s", len(rosters), game_key)
    return rosters


def load_live_events(game_key: str) -> list:
    """
    Load all live_events for *game_key* ordered by action_number ascending.
    """
    db = _get_db()
    try:
        res = (
            db.table("live_events")
            .select(
                "action_number, period, clock, team_id, team_no, player_id, "
                "player_name, shirt_number, pno, action_type, sub_type, "
                "success, scoring, score, team_score, opp_score, period_type"
            )
            .eq("game_key", game_key)
            .order("action_number", desc=False)
            .execute()
        )
        log.info("Loaded %d events for game %s", len(res.data), game_key)
        return res.data
    except Exception as exc:
        log.error("Failed to load live_events for %s: %s", game_key, exc)
        return []


# ---------------------------------------------------------------------------
# Internal lineup state management
# ---------------------------------------------------------------------------

class _TeamLineup:
    """
    Tracks the active 5-man lineup for one team across a game.
    """

    def __init__(self, team_id: str, league_id: str, game_key: str):
        self.team_id = team_id
        self.league_id = league_id
        self.game_key = game_key

        # player_key → player dict (player_id, player_name, shirt_number)
        self.active: dict = {}

        # Current stint metadata
        self.stint_start_secs: int = 0
        self.stint_start_action: Optional[int] = None
        self.stint_start_clock: Optional[str] = None
        self.stint_period: Optional[int] = None

        # Running stats for current stint
        self._reset_stats()

        # Completed stints (list of dicts ready for DB insertion)
        self.completed_stints: list = []

        # pending substitutions: list of (action, 'in'/'out', player_dict)
        self._pending_subs: list = []

    # ------------------------------------------------------------------

    def _reset_stats(self):
        self.pts_for = 0
        self.pts_against = 0
        self.fg2_made = 0
        self.fg2_attempted = 0
        self.fg3_made = 0
        self.fg3_attempted = 0
        self.ft_made = 0
        self.ft_attempted = 0
        self.oreb = 0
        self.dreb = 0
        self.assists = 0
        self.turnovers = 0
        self.fouls = 0
        self.steals = 0
        self.blocks = 0
        self.possessions_for = 0
        self.possessions_against = 0

    def _player_list(self) -> list:
        return list(self.active.values())

    def _lineup_key(self) -> str:
        return build_lineup_key(self._player_list(), self.team_id)

    # ------------------------------------------------------------------
    # Stint open / close

    def open_stint(self, game_secs: int, action: int, clock: str, period: int):
        self.stint_start_secs = game_secs
        self.stint_start_action = action
        self.stint_start_clock = clock
        self.stint_period = period
        self._reset_stats()
        if len(self.active) != 5:
            log.warning(
                "game=%s team=%s: opened stint with %d players (expected 5) at action %d",
                self.game_key, self.team_id, len(self.active), action,
            )

    def close_stint(self, game_secs: int, action: int, clock: str) -> Optional[dict]:
        """Close the current stint and return the completed stint dict, or None."""
        if self.stint_start_action is None:
            return None
        if len(self.active) < 2:
            log.warning(
                "game=%s team=%s: closing degenerate stint with %d players",
                self.game_key, self.team_id, len(self.active),
            )
            self._reset_stats()
            self.stint_start_action = None
            return None

        secs_played = max(0, game_secs - self.stint_start_secs)
        players = self._player_list()
        player_count = len(players)
        key = build_lineup_key(players, self.team_id)

        is_valid = player_count == 5
        if not is_valid:
            log.warning(
                "game=%s team=%s: writing degraded stint with %d players "
                "(start_action=%d, end_action=%d) — is_valid_lineup=False",
                self.game_key, self.team_id, player_count,
                self.stint_start_action, action,
            )

        stint = {
            "game_key": self.game_key,
            "league_id": self.league_id,
            "team_id": self.team_id,
            "lineup_key": key,
            "lineup_player_ids": [p.get("player_id") for p in players],
            "lineup_names": [p.get("player_name") for p in players],
            "period": self.stint_period,
            "start_action": self.stint_start_action,
            "end_action": action,
            "start_clock": self.stint_start_clock,
            "end_clock": clock,
            "start_game_secs": self.stint_start_secs,
            "end_game_secs": game_secs,
            "seconds_played": secs_played,
            "points_for": self.pts_for,
            "points_against": self.pts_against,
            "fg2_made": self.fg2_made,
            "fg2_attempted": self.fg2_attempted,
            "fg3_made": self.fg3_made,
            "fg3_attempted": self.fg3_attempted,
            "ft_made": self.ft_made,
            "ft_attempted": self.ft_attempted,
            "oreb": self.oreb,
            "dreb": self.dreb,
            "assists": self.assists,
            "turnovers": self.turnovers,
            "fouls": self.fouls,
            "steals": self.steals,
            "blocks": self.blocks,
            "possessions_for": self.possessions_for,
            "possessions_against": self.possessions_against,
            "is_valid_lineup": is_valid,
        }
        self.completed_stints.append(stint)

        self._reset_stats()
        self.stint_start_action = None
        return stint

    # ------------------------------------------------------------------
    # Substitution handling

    def buffer_sub(self, direction: str, player: dict, action: int):
        """Buffer a substitution event. direction is 'in' or 'out'."""
        self._pending_subs.append((action, direction, player))

    def flush_pending_subs(self, game_secs: int, action: int, clock: str, period: int):
        """
        Apply buffered substitutions atomically.
        Closes the current stint, applies all pending subs, then opens a new stint.
        """
        if not self._pending_subs:
            return

        self.close_stint(game_secs, action, clock)

        for _, direction, player in self._pending_subs:
            pkey = _player_key(player)
            if direction == "out":
                if pkey in self.active:
                    del self.active[pkey]
                else:
                    # Try to match by name if key doesn't match exactly
                    matched = _find_player_in_active(self.active, player)
                    if matched:
                        del self.active[matched]
                    else:
                        log.warning(
                            "game=%s team=%s: subout of unknown player '%s' (shirt=%s)",
                            self.game_key, self.team_id,
                            player.get("player_name"), player.get("shirt_number"),
                        )
            elif direction == "in":
                # Guard against adding a true duplicate (same player_id or shirt number).
                # Only de-dup on definitive identifiers — fuzzy name matching is too
                # risky here and causes under-counting when different players share surnames.
                already = _find_definitive_match(self.active, player)
                if already:
                    self.active[already] = player
                else:
                    self.active[pkey] = player

        self._pending_subs = []

        if len(self.active) != 5:
            log.warning(
                "game=%s team=%s: lineup has %d players after sub flush at action %d",
                self.game_key, self.team_id, len(self.active), action,
            )

        self.open_stint(game_secs, action, clock, period)

    def period_break(self, game_secs: int, action: int, clock: str, new_period: int, new_clock: str):
        """Close the current stint at period end and open a new one at new period start."""
        self.close_stint(game_secs, action, clock)
        if self.active:
            new_secs = _period_start_secs(new_period)
            self.open_stint(new_secs, action, new_clock, new_period)

    # ------------------------------------------------------------------
    # Stat attribution

    def add_stat(self, action_type: str, sub_type: str, success: bool, scoring: bool, pts: int = 0):
        action_type = (action_type or "").lower()
        sub_type = (sub_type or "").lower()

        if action_type == "2pt":
            self.fg2_attempted += 1
            if success:
                self.fg2_made += 1
                if scoring:
                    self.pts_for += pts or 2
        elif action_type == "3pt":
            self.fg3_attempted += 1
            if success:
                self.fg3_made += 1
                if scoring:
                    self.pts_for += pts or 3
        elif action_type == "freethrow":
            self.ft_attempted += 1
            if success:
                self.ft_made += 1
                if scoring:
                    self.pts_for += pts or 1
        elif action_type == "rebound":
            if sub_type == "offensive":
                self.oreb += 1
            else:
                self.dreb += 1
        elif action_type == "assist":
            self.assists += 1
        elif action_type == "turnover":
            self.turnovers += 1
        elif action_type == "foul":
            self.fouls += 1
        elif action_type == "steal":
            self.steals += 1
        elif action_type == "block":
            self.blocks += 1

    def add_points_against(self, pts: int):
        self.pts_against += pts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _player_key(player: dict) -> str:
    """Unique key for a player within a team's active lineup dict."""
    pid = player.get("player_id")
    if pid:
        return str(pid)
    name = (player.get("player_name") or "").strip().lower()
    shirt = (player.get("shirt_number") or "").strip()
    return f"{name}:{shirt}"


def _name_tokens(name: str):
    """Return (last_name, first_initial) from a player name string."""
    parts = name.strip().lower().split()
    if not parts:
        return "", ""
    last = parts[-1]
    first_initial = parts[0][0] if parts[0] else ""
    return last, first_initial


def _names_match(a: str, b: str) -> bool:
    """
    Return True if two player name strings refer to the same person.
    Handles:
      - exact match
      - abbreviated first name: "V. Iweanya" vs "Vural Iweanya"
      - same last name + same first initial
    """
    a = (a or "").strip().lower()
    b = (b or "").strip().lower()
    if not a or not b:
        return False
    if a == b:
        return True
    a_last, a_init = _name_tokens(a)
    b_last, b_init = _name_tokens(b)
    if a_last != b_last:
        return False
    # last names match — accept if either first part starts with the other's initial
    return (not a_init or not b_init or a_init == b_init)


def _find_player_in_active(active: dict, player: dict) -> Optional[str]:
    """
    Try to find a player in the active dict by shirt number, player_id, or name.
    Uses fuzzy name matching to handle abbreviated vs full names.
    """
    name = (player.get("player_name") or "").strip().lower()
    shirt = (player.get("shirt_number") or "").strip()
    pid = player.get("player_id")

    for key, p in active.items():
        p_name = (p.get("player_name") or "").strip().lower()
        p_shirt = (p.get("shirt_number") or "").strip()
        p_pid = p.get("player_id")
        if pid and p_pid and str(pid) == str(p_pid):
            return key
        if shirt and p_shirt and p_shirt == shirt:
            return key
        if name and _names_match(name, p_name):
            return key
    return None


def _find_definitive_match(active: dict, player: dict) -> Optional[str]:
    """
    Return the active-dict key for player only if there is a definitive match:
    same player_id UUID or same shirt number. Does NOT use name matching to
    avoid false positives when different players share a surname.
    """
    pid = player.get("player_id")
    shirt = (player.get("shirt_number") or "").strip()

    for key, p in active.items():
        p_pid = p.get("player_id")
        p_shirt = (p.get("shirt_number") or "").strip()
        if pid and p_pid and str(pid) == str(p_pid):
            return key
        if shirt and p_shirt and p_shirt == shirt:
            return key
    return None


def _resolve_player_from_event(event: dict, roster: list) -> dict:
    """
    Build a player dict from a live_events row, optionally enriching from roster.
    Falls back to name-based roster lookup when shirt_number is absent (historical data).
    """
    player = {
        "player_id": event.get("player_id"),
        "player_name": event.get("player_name"),
        "shirt_number": event.get("shirt_number") or event.get("pno"),
    }
    if not roster:
        return player

    shirt = str(event.get("shirt_number") or "").strip()
    evt_name = (event.get("player_name") or "").strip()

    # Try shirt-number match first (most reliable)
    if shirt:
        for r in roster:
            if str(r.get("shirt_number") or "").strip() == shirt:
                if not player["player_id"]:
                    player["player_id"] = r.get("player_id")
                if not player["shirt_number"]:
                    player["shirt_number"] = r.get("shirt_number")
                if not player["player_name"]:
                    player["player_name"] = r.get("player_name")
                return player

    # Fall back to name match (handles historical events where shirt_number is NULL)
    if evt_name:
        for r in roster:
            r_name = (r.get("player_name") or "").strip()
            if _names_match(evt_name, r_name):
                if not player["player_id"]:
                    player["player_id"] = r.get("player_id")
                if not player["shirt_number"]:
                    player["shirt_number"] = r.get("shirt_number")
                break

    return player


# ---------------------------------------------------------------------------
# Main lineup reconstruction function
# ---------------------------------------------------------------------------

def build_lineups_for_game(
    game_key: str,
    league_id: str,
    dry_run: bool = False,
) -> bool:
    """
    Reconstruct 5-man lineups for *game_key* and write to lineup_stints
    and player_on_court_stints.

    Parameters
    ----------
    game_key  : str   The game identifier.
    league_id : str   UUID of the league (for FK population).
    dry_run   : bool  If True, compute stints but do NOT write to Supabase.

    Returns
    -------
    bool  True on success, False on unrecoverable error.
    """
    log.info("Starting lineup build for game=%s (dry_run=%s)", game_key, dry_run)

    # --- Load rosters ---
    rosters_by_team = load_game_rosters(game_key)
    if not rosters_by_team:
        log.warning("game=%s: no roster data, skipping lineup build", game_key)
        return False

    # --- Load events ---
    events = load_live_events(game_key)
    if not events:
        log.warning("game=%s: no live_events, skipping lineup build", game_key)
        return False

    # --- Build team state objects ---
    team_state: dict[str, _TeamLineup] = {}
    team_roster_map: dict[str, list] = {}

    for team_id, roster in rosters_by_team.items():
        tl = _TeamLineup(team_id=team_id, league_id=league_id, game_key=game_key)
        starters = [p for p in roster if p.get("starter")]
        if len(starters) != 5:
            log.warning(
                "game=%s team=%s: found %d starters (expected 5); will use all active players as fallback",
                game_key, team_id, len(starters),
            )
            if len(starters) < 5:
                active = [p for p in roster if p.get("active", True)]
                starters = active[:5]

        for s in starters[:5]:
            player = {
                "player_id": s.get("player_id"),
                "player_name": s.get("player_name"),
                "shirt_number": s.get("shirt_number"),
            }
            key = _player_key(player)
            tl.active[key] = player

        team_state[team_id] = tl
        team_roster_map[team_id] = roster

    # Build a map from team_no -> team_id (team_no is the "1" or "2" side key)
    team_no_to_id: dict[str, str] = {}
    for team_id, roster in rosters_by_team.items():
        for row in roster:
            tno = str(row.get("team_no") or "").strip()
            if tno:
                team_no_to_id[tno] = team_id
                break

    # --- Open initial stints at game start ---
    # Anchor to the period start (10:00 for regulation quarters, 5:00 for OT)
    # rather than the first logged event clock, to avoid undercounting dead-ball
    # time before the first action appears in the feed.
    first_event = events[0] if events else None
    if first_event:
        init_period = first_event.get("period") or 1
        period_dur = _period_duration(init_period)
        init_clock = f"{period_dur // 60}:00"
        init_secs = _period_start_secs(init_period)
        init_action = first_event.get("action_number") or 1
        for tl in team_state.values():
            if tl.active:
                tl.open_stint(init_secs, init_action, init_clock, init_period)

    # --- Walk events ---
    current_period = first_event.get("period") if first_event else 1

    # Track which team currently has the ball so we can close open possessions
    # at period boundaries and game end.  None = unknown (e.g. game start before
    # first possession-changing event).
    current_possessor_id: Optional[str] = None

    def _close_open_possession():
        """
        If a possession is in-flight (current_possessor_id is known), attribute
        it to the active stints and return True.  Used at period/game boundaries.
        """
        if current_possessor_id and current_possessor_id in team_state:
            team_state[current_possessor_id].possessions_for += 1
            for _tid, _tl in team_state.items():
                if _tid != current_possessor_id:
                    _tl.possessions_against += 1
            return True
        return False

    for evt in events:
        action_type = (evt.get("action_type") or "").lower()
        period = evt.get("period") or current_period
        clock = evt.get("clock") or ""
        action = evt.get("action_number") or 0
        game_secs = _event_game_secs(period, clock)

        # --- Period boundary ---
        if period != current_period:
            new_period_start_clock = f"{_period_duration(period) // 60}:00"
            # Close any open possession before flushing the period
            _close_open_possession()
            current_possessor_id = None
            for tl in team_state.values():
                # Flush any pending subs before closing period
                tl.flush_pending_subs(
                    _event_game_secs(current_period, "00:00"),
                    action, "00:00", current_period
                )
                tl.period_break(
                    _event_game_secs(current_period, "00:00"),
                    action, "00:00", period, new_period_start_clock
                )
            current_period = period

        # --- Get the teams involved ---
        team_no = str(evt.get("team_no") or "").strip()
        event_team_id = team_no_to_id.get(team_no) or evt.get("team_id")

        # --- Substitution event ---
        if action_type == "substitution":
            direction = (evt.get("sub_type") or "").lower()
            if direction not in ("in", "out"):
                log.debug("game=%s: unknown sub sub_type '%s' at action %d", game_key, direction, action)
                continue

            if event_team_id and event_team_id in team_state:
                roster = team_roster_map.get(event_team_id, [])
                player = _resolve_player_from_event(evt, roster)
                tl = team_state[event_team_id]
                tl.buffer_sub(direction, player, action)

                # Check if we have a complete in+out pair buffered → flush
                pending = tl._pending_subs
                outs = [p for p in pending if p[1] == "out"]
                ins = [p for p in pending if p[1] == "in"]
                if len(outs) == len(ins) and len(outs) > 0:
                    tl.flush_pending_subs(game_secs, action, clock, period)
            else:
                log.warning(
                    "game=%s: substitution event at action %d has unknown team_no=%s",
                    game_key, action, team_no,
                )
            continue

        # --- For any non-sub event, flush pending subs first ---
        for tl in team_state.values():
            if tl._pending_subs:
                tl.flush_pending_subs(game_secs, action, clock, period)

        # --- Stat attribution ---
        if action_type in ("2pt", "3pt", "freethrow"):
            success = bool(evt.get("success"))
            scoring = bool(evt.get("scoring"))

            # Points scored this play
            pts = 0
            if success and scoring:
                if action_type == "2pt":
                    pts = 2
                elif action_type == "3pt":
                    pts = 3
                elif action_type == "freethrow":
                    pts = 1

            if event_team_id and event_team_id in team_state:
                team_state[event_team_id].add_stat(action_type, "", success, scoring, pts)

            # Points against for the opponent
            if pts > 0:
                for tid, tl in team_state.items():
                    if tid != event_team_id:
                        tl.add_points_against(pts)

        elif action_type == "rebound":
            sub_type = (evt.get("sub_type") or "").lower()
            if event_team_id and event_team_id in team_state:
                team_state[event_team_id].add_stat("rebound", sub_type, True, False)

        elif action_type in ("turnover", "foul", "steal", "block"):
            if event_team_id and event_team_id in team_state:
                team_state[event_team_id].add_stat(action_type, "", True, False)

        # --- Possession attribution ---
        # Possessions are tracked via explicit ownership (current_possessor_id).
        # A possession ends on: made FG, turnover, or defensive rebound.
        # Offensive rebounds continue the same possession (possessor unchanged).
        # Period/game boundaries close any open possession via _close_open_possession().
        # Free throws are not counted as independent possession enders; the surrounding
        # made FG or defensive rebound after the last missed FT covers the sequence.
        if action_type in ("2pt", "3pt"):
            success = bool(evt.get("success"))
            scoring = bool(evt.get("scoring"))
            if success and scoring and event_team_id and event_team_id in team_state:
                # Scoring team's possession ended with a make
                team_state[event_team_id].possessions_for += 1
                for tid, tl in team_state.items():
                    if tid != event_team_id:
                        tl.possessions_against += 1
                # Opponent now has possession
                current_possessor_id = next(
                    (tid for tid in team_state if tid != event_team_id), None
                )

        elif action_type == "turnover":
            if event_team_id and event_team_id in team_state:
                team_state[event_team_id].possessions_for += 1
                for tid, tl in team_state.items():
                    if tid != event_team_id:
                        tl.possessions_against += 1
                # Opponent gains possession
                current_possessor_id = next(
                    (tid for tid in team_state if tid != event_team_id), None
                )

        elif action_type == "rebound":
            reb_sub_type = (evt.get("sub_type") or "").lower()
            if reb_sub_type == "defensive" and event_team_id and event_team_id in team_state:
                # Defensive rebound: the team that missed (NOT the rebounder) ends their possession
                team_state[event_team_id].possessions_against += 1
                for tid, tl in team_state.items():
                    if tid != event_team_id:
                        tl.possessions_for += 1
                # Rebounder now has possession
                current_possessor_id = event_team_id
            elif reb_sub_type == "offensive" and event_team_id and event_team_id in team_state:
                # Offensive rebound: same team retains possession
                current_possessor_id = event_team_id

        # --- End-of-game / end-of-period markers ---
        if action_type in ("gameend", "endofgame", "endofperiod", "periodend"):
            # Close any possession still open at the buzzer
            _close_open_possession()
            current_possessor_id = None
            for tl in team_state.values():
                tl.close_stint(game_secs, action, clock)
                tl.stint_start_action = None  # don't reopen

    # --- Close any still-open stints ---
    last_evt = events[-1] if events else None
    if last_evt:
        last_period = last_evt.get("period") or current_period
        last_clock = last_evt.get("clock") or "00:00"
        last_action = last_evt.get("action_number") or 0
        last_secs = _event_game_secs(last_period, last_clock)
        # Close any possession still open at the true end of the game
        _close_open_possession()
        for tl in team_state.values():
            if tl._pending_subs:
                tl.flush_pending_subs(last_secs, last_action, last_clock, last_period)
            tl.close_stint(last_secs, last_action, last_clock)

    # --- Collect all stints ---
    all_stints = []
    for tl in team_state.values():
        all_stints.extend(tl.completed_stints)

    log.info("game=%s: generated %d lineup stints", game_key, len(all_stints))

    if not all_stints:
        log.warning("game=%s: no stints generated, nothing to write", game_key)
        return False

    if dry_run:
        log.info("game=%s: dry_run=True, skipping DB writes", game_key)
        return True

    # --- Write to Supabase ---
    db = _get_db()

    # Delete existing stints for this game first (idempotent).
    # Fetch IDs first then delete in small batches to avoid Supabase statement timeouts
    # that occur when deleting many rows via a single filter query.
    _delete_by_game_key(db, "lineup_stints", game_key)
    _delete_by_game_key(db, "player_on_court_stints", game_key)

    # Insert lineup_stints in chunks
    _bulk_insert(db, "lineup_stints", all_stints, game_key)

    # --- Reload inserted stints to get their IDs ---
    try:
        stint_rows = (
            db.table("lineup_stints")
            .select("id, team_id, lineup_key, period, start_game_secs, end_game_secs, "
                    "seconds_played, points_for, points_against, possessions_for, possessions_against, "
                    "lineup_player_ids, lineup_names")
            .eq("game_key", game_key)
            .execute()
        )
    except Exception as exc:
        log.error("game=%s: failed to reload lineup_stints for player expansion: %s", game_key, exc)
        return True  # stints were written, player rows failed

    # --- Build player_on_court_stints ---
    player_rows = []
    for stint_row in stint_rows.data:
        stint_id = stint_row["id"]
        team_id = stint_row["team_id"]
        lineup_key = stint_row["lineup_key"]
        period = stint_row.get("period")
        start_secs = stint_row.get("start_game_secs")
        end_secs = stint_row.get("end_game_secs")
        secs_played = stint_row.get("seconds_played", 0)
        pts_for = stint_row.get("points_for", 0)
        pts_against = stint_row.get("points_against", 0)
        poss_for = stint_row.get("possessions_for", 0)
        poss_against = stint_row.get("possessions_against", 0)

        player_ids = stint_row.get("lineup_player_ids") or []
        player_names = stint_row.get("lineup_names") or []

        # Look up shirt numbers from rosters
        roster = rosters_by_team.get(team_id, [])
        roster_by_pid = {str(r.get("player_id")): r for r in roster if r.get("player_id")}
        roster_by_name = {(r.get("player_name") or "").strip().lower(): r for r in roster}

        for i, pid in enumerate(player_ids):
            name = player_names[i] if i < len(player_names) else None
            shirt = None
            roster_row = None
            if pid:
                roster_row = roster_by_pid.get(str(pid))
            if not roster_row and name:
                roster_row = roster_by_name.get((name or "").strip().lower())
            if roster_row:
                shirt = roster_row.get("shirt_number")

            player_rows.append({
                "stint_id": stint_id,
                "game_key": game_key,
                "league_id": league_id,
                "team_id": team_id,
                "player_id": pid or None,
                "player_name": name,
                "shirt_number": shirt,
                "lineup_key": lineup_key,
                "period": period,
                "start_game_secs": start_secs,
                "end_game_secs": end_secs,
                "seconds_played": secs_played,
                "points_for": pts_for,
                "points_against": pts_against,
                "possessions_for": poss_for,
                "possessions_against": poss_against,
            })

    _bulk_insert(db, "player_on_court_stints", player_rows, game_key)

    log.info(
        "game=%s: wrote %d lineup stints, %d player on-court rows",
        game_key, len(all_stints), len(player_rows),
    )
    return True


# ---------------------------------------------------------------------------
# Utility: bulk insert / delete with chunking
# ---------------------------------------------------------------------------

def _delete_by_game_key(db, table: str, game_key: str, chunk_size: int = 50):
    """
    Delete all rows for game_key from table using ID-based batch deletes.
    A single DELETE ... WHERE game_key=X can time out in Supabase when the
    table has many rows; fetching IDs first and deleting in small batches is
    much more reliable.
    """
    try:
        id_res = (
            db.table(table)
            .select("id")
            .eq("game_key", game_key)
            .execute()
        )
        ids = [r["id"] for r in (id_res.data or [])]
    except Exception as exc:
        log.warning("game=%s: could not fetch IDs from %s for deletion: %s", game_key, table, exc)
        return

    if not ids:
        return

    for i in range(0, len(ids), chunk_size):
        batch = ids[i:i + chunk_size]
        try:
            db.table(table).delete().in_("id", batch).execute()
        except Exception as exc:
            log.error(
                "game=%s: failed deleting batch from %s (offset=%d): %s",
                game_key, table, i, exc,
            )


def _bulk_insert(db, table: str, rows: list, game_key: str, chunk_size: int = 200):
    if not rows:
        return
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        try:
            db.table(table).insert(chunk).execute()
        except Exception as exc:
            log.error("game=%s: failed inserting chunk into %s (offset=%d): %s", game_key, table, i, exc)
