"""
Tests for bringing a 2017-2019 LiveStats feed (no action numbers, team minutes
0) into the shape the parser reads.
"""
import copy
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.legacy_feed import normalize_legacy_feed


def ev(period, action, sub="", tno=0, pno=0, success=1, gt="05:00"):
    return {"period": period, "actionType": action, "subType": sub, "tno": tno,
            "pno": pno, "success": success, "gt": gt, "periodType": "REGULAR"}


def legacy_feed():
    # Newest first, as the feed lists them.
    pbp = [
        ev(4, "game", "end", gt="00:00"),
        ev(4, "period", "end", gt="00:00"),
        ev(4, "3pt", "jumpshot", tno=1, pno=5, success=0, gt="00:03"),
        ev(2, "2pt", "layup", tno=2, pno=7, success=1, gt="04:10"),
        ev(1, "2pt", "jumpshot", tno=1, pno=5, success=1, gt="08:00"),
        ev(1, "game", "start", gt="10:00"),
    ]
    return {
        "pbp": pbp,
        "tm": {
            "1": {"tot_sMinutes": 0,
                  "pl": {"5": {"sMinutes": "32:06"}, "6": {"sMinutes": "27:54"}},
                  "shot": [{"pno": 5, "per": 1, "actionType": "2pt", "r": 1},
                           {"pno": 5, "per": 4, "actionType": "3pt", "r": 0}]},
            "2": {"tot_sMinutes": "200:00", "pl": {},
                  "shot": [{"pno": 7, "per": 2, "actionType": "2pt", "r": 1}]},
        },
    }


def test_events_numbered_in_game_order():
    data = normalize_legacy_feed(legacy_feed())
    by_number = sorted(data["pbp"], key=lambda e: e["actionNumber"])
    assert [e["actionNumber"] for e in by_number] == [1, 2, 3, 4, 5, 6]
    assert by_number[0]["subType"] == "start"
    assert by_number[-1]["subType"] == "end" and by_number[-1]["actionType"] == "game"


def test_shots_take_their_play_number():
    data = normalize_legacy_feed(legacy_feed())
    numbers = {(e["tno"], e["pno"], e["period"]): e["actionNumber"] for e in data["pbp"] if e["tno"]}
    home = data["tm"]["1"]["shot"]
    assert home[0]["actionNumber"] == numbers[(1, 5, 1)]
    assert home[1]["actionNumber"] == numbers[(1, 5, 4)]
    assert data["tm"]["2"]["shot"][0]["actionNumber"] == numbers[(2, 7, 2)]


def test_missing_team_minutes_summed_from_players():
    data = normalize_legacy_feed(legacy_feed())
    assert data["tm"]["1"]["tot_sMinutes"] == "60:00"
    assert data["tm"]["2"]["tot_sMinutes"] == "200:00"


def test_current_feed_left_alone():
    feed = legacy_feed()
    for i, e in enumerate(reversed(feed["pbp"]), start=1):
        e["actionNumber"] = i * 10
    before = copy.deepcopy(feed)
    assert normalize_legacy_feed(feed) == before


def test_empty_feed_left_alone():
    assert normalize_legacy_feed({"pbp": [], "tm": {}}) == {"pbp": [], "tm": {}}
    assert normalize_legacy_feed(None) is None
