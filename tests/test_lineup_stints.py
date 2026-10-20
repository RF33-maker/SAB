"""
Lineup stint reconstruction on real games (names and ids anonymised).

Fixtures:
  lineup_1WBR4ZOCHL6EF03AOKCT.json  BCB 2026-27. The scorer logged a Q3 assist
      after "game end", which used to reopen Q3 and Q4 as phantom full-period
      stints (2400s of real time stored as 3600s).
  lineup_1WSTK3MAMV3VWOTN65TY.json  Substitutions carry "X. Surname" stub
      player_ids, which used to split each player's minutes across two ids.
  lineup_1WBQFZBH65PQB0KSPFTK.json  Double overtime. LiveStats numbers OT1 and OT2 as
      periods 1 and 2, which used to fold overtime into the first half. Its
      scorer also re-entered a Q1 substitution at the wrong time after the
      quarter, which leaves 40 seconds with four men on court and ~50s of minutes
      error that no ordering can recover; the tolerances below allow for that.

Expected values come from each game's official box score.
"""
import collections
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.lineup_builder import (  # noqa: E402
    _resolve_player_from_event,
    build_context_rows,
    compute_stints,
    order_events_by_game_clock,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
GAMES = ["1WBR4ZOCHL6EF03AOKCT", "1WSTK3MAMV3VWOTN65TY", "1WBQFZBH65PQB0KSPFTK"]
# Seconds a player's tracked minutes may differ from the box score.
MINUTES_TOLERANCE = {"1WBQFZBH65PQB0KSPFTK": 60}


def load(game_key):
    with open(os.path.join(FIXTURES, f"lineup_{game_key}.json")) as fh:
        data = json.load(fh)
    rosters = collections.defaultdict(list)
    for row in data["rosters"]:
        rosters[row["team_id"]].append(row)
    stints = compute_stints(dict(rosters), data["events"], game_key, "league")
    return data, stints


def secs(mmss):
    mins, s = (str(mmss).split(":") + ["0"])[:2]
    return int(mins) * 60 + int(s)


@pytest.mark.parametrize("game_key", GAMES)
def test_each_team_is_on_court_for_exactly_the_game(game_key):
    data, stints = load(game_key)
    for team in data["teams"]:
        team_stints = [s for s in stints if s["team_id"] == team["team_id"]]
        # Team minutes are five players' minutes summed.
        game_secs = secs(team["tot_sminutes"]) // 5
        assert sum(s["seconds_played"] for s in team_stints) == game_secs
        invalid_secs = sum(s["seconds_played"] for s in team_stints if not s["is_valid_lineup"])
        assert invalid_secs <= 0.02 * game_secs


@pytest.mark.parametrize("game_key", GAMES)
def test_stints_never_overlap(game_key):
    _, stints = load(game_key)
    by_team = collections.defaultdict(list)
    for s in stints:
        by_team[s["team_id"]].append(s)
    for team_stints in by_team.values():
        team_stints.sort(key=lambda s: (s["start_game_secs"], s["end_game_secs"]))
        for prev, cur in zip(team_stints, team_stints[1:]):
            assert cur["start_game_secs"] >= prev["end_game_secs"]


@pytest.mark.parametrize("game_key", GAMES)
def test_stint_points_add_up_to_the_final_score(game_key):
    data, stints = load(game_key)
    for team in data["teams"]:
        team_stints = [s for s in stints if s["team_id"] == team["team_id"]]
        assert sum(s["points_for"] for s in team_stints) == team["tot_spoints"]


@pytest.mark.parametrize("game_key", GAMES)
def test_every_player_keeps_one_id_and_their_official_minutes(game_key):
    data, stints = load(game_key)
    roster_ids = {r["player_id"] for r in data["rosters"]}
    on_court = collections.Counter()
    for s in stints:
        assert set(s["lineup_player_ids"]) <= roster_ids
        for pid in s["lineup_player_ids"]:
            on_court[pid] += s["seconds_played"]
    for row in data["box"]:
        if row["sminutes"] in ("0:00", "00:00"):
            assert on_court.get(row["player_id"], 0) == 0
        else:
            assert abs(on_court[row["player_id"]] - secs(row["sminutes"])) <= MINUTES_TOLERANCE.get(game_key, 30)


OWN_KEYS = ("fg2_made", "fg2_attempted", "fg3_made", "fg3_attempted", "ft_made", "ft_attempted",
            "oreb", "dreb", "assists", "turnovers", "steals", "blocks")


@pytest.mark.parametrize("game_key", GAMES)
def test_opponent_stats_mirror_the_other_team(game_key):
    _, stints = load(game_key)
    teams = sorted({s["team_id"] for s in stints})
    for team, other in ((teams[0], teams[1]), (teams[1], teams[0])):
        mine = [s for s in stints if s["team_id"] == team]
        theirs = [s for s in stints if s["team_id"] == other]
        for k in OWN_KEYS:
            assert sum(s[f"opp_{k}"] for s in mine) == sum(s[k] for s in theirs), k
        opp_points = sum(2 * s["opp_fg2_made"] + 3 * s["opp_fg3_made"] + s["opp_ft_made"] for s in mine)
        assert opp_points == sum(s["points_against"] for s in mine)


@pytest.mark.parametrize("game_key", GAMES)
def test_context_rows_track_the_score_and_starters(game_key):
    data, stints = load(game_key)
    for i, s in enumerate(stints):
        s["id"] = f"stint-{i}"
    rosters = collections.defaultdict(list)
    for row in data["rosters"]:
        rosters[row["team_id"]].append(row)
    rows = build_context_rows(stints, dict(rosters))
    assert len(rows) == len(stints)
    for team in data["teams"]:
        mine = sorted((r for r in rows if r["team_id"] == team["team_id"]), key=lambda r: r["start_game_secs"])
        assert mine[0]["score_margin_at_stint_start"] == 0
        assert mine[0]["team_starters_on_court"] == 5
        assert all(0 <= r["team_starters_on_court"] <= 5 for r in mine)
        last = mine[-1]
        final = last["score_margin_at_stint_start"] + last["points_for"] - last["points_against"]
        opponent = next(t for t in data["teams"] if t["team_id"] != team["team_id"])
        assert final == team["tot_spoints"] - opponent["tot_spoints"]
        assert all(r["is_competitive_stint"] == (r["is_valid_lineup"] and not r["is_garbage_time"]
                                                 and not r["is_short_clock_end_period"]) for r in mine)


def test_late_correction_is_placed_in_its_own_period():
    events = [
        {"action_number": 1, "period": 3, "clock": "09:00"},
        {"action_number": 2, "period": 4, "clock": "05:00"},
        {"action_number": 3, "period": 4, "clock": "00:00"},
        {"action_number": 4, "period": 3, "clock": "08:31:00"},  # entered after the game
        {"action_number": 5, "period": 4, "clock": "00:00"},
    ]
    assert [e["action_number"] for e in order_events_by_game_clock(events)] == [1, 4, 2, 3, 5]


def test_overtime_follows_the_fourth_quarter():
    events = [
        {"action_number": 1, "period": 1, "clock": "09:00", "period_type": "REGULAR"},
        {"action_number": 2, "period": 4, "clock": "00:30", "period_type": "REGULAR"},
        # LiveStats sends OT1 as period 1 and OT2 as period 2.
        {"action_number": 3, "period": 1, "clock": "04:00", "period_type": "OVERTIME"},
        {"action_number": 4, "period": 2, "clock": "02:00", "period_type": "OVERTIME"},
    ]
    ordered = order_events_by_game_clock(events)
    assert [(e["action_number"], e["period"]) for e in ordered] == [(1, 1), (2, 4), (3, 5), (4, 6)]


def test_same_second_events_keep_feed_order():
    events = [
        {"action_number": 7, "period": 2, "clock": "04:10"},
        {"action_number": 5, "period": 2, "clock": "04:10"},
        {"action_number": 6, "period": 2, "clock": "04:10:50"},
    ]
    assert [e["action_number"] for e in order_events_by_game_clock(events)] == [5, 6, 7]


def test_event_player_resolves_to_the_roster_player_not_the_stub():
    roster = [
        {"player_id": "box-1", "player_name": "Kendrick Tchoua", "shirt_number": "2", "pno": 3},
        {"player_id": "box-2", "player_name": "Kevin Tchoua", "shirt_number": "12", "pno": 9},
    ]
    stub = {"player_id": "stub-9", "player_name": "K. Tchoua", "shirt_number": "02", "pno": 3}
    assert _resolve_player_from_event(stub, roster)["player_id"] == "box-1"

    # Same surname and initial: a name alone is ambiguous, so the shirt decides.
    by_shirt = {"player_id": "stub-9", "player_name": "K. Tchoua", "shirt_number": "12"}
    assert _resolve_player_from_event(by_shirt, roster)["player_id"] == "box-2"

    # Nothing to go on but an ambiguous name: keep the event's own identity.
    unknown = {"player_id": "stub-9", "player_name": "K. Tchoua"}
    assert _resolve_player_from_event(unknown, roster)["player_id"] == "stub-9"
