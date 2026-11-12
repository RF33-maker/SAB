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
        e["clock"] = f"{e['gt']}:00"
    before = copy.deepcopy(feed)
    assert normalize_legacy_feed(feed) == before


def test_clock_filled_from_gt_on_numbered_feed():
    # 2019-20 feeds number their events but give the clock only as "gt".
    feed = legacy_feed()
    for i, e in enumerate(reversed(feed["pbp"]), start=1):
        e["actionNumber"] = i
    data = normalize_legacy_feed(feed)
    assert [e["clock"] for e in data["pbp"]] == [
        "00:00:00", "00:00:00", "00:03:00", "04:10:00", "08:00:00", "10:00:00"]
    assert data["tm"]["1"]["tot_sMinutes"] == 0  # numbered feeds keep their totals


def test_clock_filled_on_legacy_feed():
    data = normalize_legacy_feed(legacy_feed())
    assert all(e["clock"] == f"{e['gt']}:00" for e in data["pbp"])


def oldest_feed():
    # 2015-17 shape: shots carry "p" and no team number, names lead with the shirt.
    feed = legacy_feed()
    for e in feed["pbp"]:
        if e["pno"]:
            e["player"] = f"1{e['pno']}, A. Smith"
    for team in feed["tm"].values():
        for shot in team["shot"]:
            shot["p"] = shot.pop("pno")
            shot["player"] = f"1{shot['p']}, A. Smith"
    return feed


def test_oldest_shots_get_player_and_team():
    data = normalize_legacy_feed(oldest_feed())
    numbers = {(e["tno"], e["pno"], e["period"]): e["actionNumber"] for e in data["pbp"] if e["tno"]}
    home, away = data["tm"]["1"]["shot"], data["tm"]["2"]["shot"]
    assert [(s["pno"], s["tno"]) for s in home] == [(5, 1), (5, 1)]
    assert (away[0]["pno"], away[0]["tno"]) == (7, 2)
    assert home[0]["actionNumber"] == numbers[(1, 5, 1)]
    assert away[0]["actionNumber"] == numbers[(2, 7, 2)]


def test_oldest_names_lose_the_shirt_prefix():
    data = normalize_legacy_feed(oldest_feed())
    scorer = next(e for e in data["pbp"] if e["pno"] == 5)
    assert scorer["player"] == "A. Smith" and scorer["shirtNumber"] == "15"
    shot = data["tm"]["2"]["shot"][0]
    assert shot["player"] == "A. Smith" and shot["shirtNumber"] == "17"


def test_shirt_prefix_kept_off_numbered_feeds():
    feed = oldest_feed()
    for i, e in enumerate(reversed(feed["pbp"]), start=1):
        e["actionNumber"] = i
    data = normalize_legacy_feed(feed)
    # Only the clock is filled on a numbered feed; names are left as given.
    assert next(e for e in data["pbp"] if e["pno"] == 5)["player"] == "15, A. Smith"


def test_blank_shirt_number_becomes_missing():
    # 2016-17 feed (game 605013) gave one player shirtNumber "", which the
    # integer players.shirtNumber column rejects.
    feed = legacy_feed()
    feed["tm"]["2"]["pl"] = {"7": {"sMinutes": "10:00", "shirtNumber": ""}}
    feed["pbp"][3]["shirtNumber"] = ""
    feed["tm"]["1"]["pl"]["5"]["shirtNumber"] = "12"
    data = normalize_legacy_feed(feed)
    assert data["tm"]["2"]["pl"]["7"]["shirtNumber"] is None
    assert next(e for e in data["pbp"] if e["pno"] == 7)["shirtNumber"] is None
    assert data["tm"]["1"]["pl"]["5"]["shirtNumber"] == "12"


def test_empty_feed_left_alone():
    assert normalize_legacy_feed({"pbp": [], "tm": {}}) == {"pbp": [], "tm": {}}
    assert normalize_legacy_feed(None) is None


def test_overtime_feed_numbered_from_the_tip():
    # OT1 comes through as period 1 / OVERTIME, so the newest and oldest events
    # both say period 1; the earliest event here is a substitution, not "start".
    pbp = [
        {**ev(1, "game", "end", gt="00:00"), "periodType": "OVERTIME", "s1": 104, "s2": 114},
        {**ev(1, "2pt", "layup", tno=2, pno=7, gt="00:30"), "periodType": "OVERTIME", "s1": 104, "s2": 112},
        {**ev(4, "2pt", "layup", tno=1, pno=5, gt="00:10"), "s1": 93, "s2": 93},
        {**ev(1, "2pt", "layup", tno=1, pno=5, gt="09:30"), "s1": 2, "s2": 0},
        {**ev(1, "substitution", "in", tno=1, pno=5, gt="10:00"), "s1": 0, "s2": 0},
    ]
    data = normalize_legacy_feed({"pbp": pbp, "tm": {}})
    by_number = sorted(data["pbp"], key=lambda e: e["actionNumber"])
    assert by_number[0]["actionType"] == "substitution"
    assert by_number[-1]["subType"] == "end" and by_number[-1]["periodType"] == "OVERTIME"
