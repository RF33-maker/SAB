"""
Tests for the post-game reconciliation helpers: when a finished game is looked
at again, and whether the feed changed since it was last parsed.
"""
import copy
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.reconcile import feed_fingerprint, reconcile_next_poll

FULL_TIME = datetime(2026, 10, 2, 20, 25, tzinfo=timezone.utc)


def _next(hours_after_full_time: float | timedelta) -> datetime | None:
    age = hours_after_full_time if isinstance(hours_after_full_time, timedelta) else timedelta(hours=hours_after_full_time)
    now = FULL_TIME + age
    result = reconcile_next_poll(FULL_TIME.isoformat(), now)
    return datetime.fromisoformat(result) if result else None


def test_checks_every_30_minutes_in_first_six_hours():
    now = FULL_TIME + timedelta(hours=1)
    assert _next(1) - now == timedelta(minutes=30)
    now = FULL_TIME + timedelta(hours=5, minutes=59)
    assert _next(timedelta(hours=5, minutes=59)) - now == timedelta(minutes=30)


def test_checks_every_6_hours_after_first_six_hours():
    now = FULL_TIME + timedelta(hours=6)
    assert _next(6) - now == timedelta(hours=6)
    now = FULL_TIME + timedelta(hours=40)
    assert _next(40) - now == timedelta(hours=6)


def test_last_check_lands_on_the_72_hour_mark_not_past_it():
    assert _next(70) == FULL_TIME + timedelta(hours=72)


def test_stops_after_72_hours():
    assert _next(72) is None
    assert _next(100) is None


def test_no_final_time_means_no_recheck():
    assert reconcile_next_poll(None) is None
    assert reconcile_next_poll("not a date") is None


def test_accepts_postgres_and_z_timestamps():
    now = FULL_TIME + timedelta(hours=1)
    assert reconcile_next_poll("2026-10-02 20:25:00+00:00", now) is not None
    assert reconcile_next_poll("2026-10-02T20:25:00Z", now) is not None


def _feed():
    return {
        "clock": "00:00",
        "pbp": [
            {"actionNumber": 2, "period": 1, "gt": "09:50", "actionType": "block", "tno": 1, "pno": 2, "player": "M. Diggins Jr", "success": True, "previousAction": 1},
            {"actionNumber": 1, "period": 1, "gt": "09:50", "actionType": "2pt", "tno": 2, "pno": 7, "player": "B. Mitchell-Day", "success": False, "previousAction": None},
        ],
        "tm": {
            "1": {"tot_sBlocks": 1, "pl": {"1": {"name": "M. Diggins Jr", "shirtNumber": "2", "sBlocks": 1, "sSteals": 1}}},
            "2": {"tot_sBlocks": 0, "pl": {"1": {"name": "B. Mitchell-Day", "shirtNumber": "7", "sBlocks": 0, "sSteals": 0}}},
        },
    }


def test_fingerprint_is_stable():
    assert feed_fingerprint(_feed()) == feed_fingerprint(copy.deepcopy(_feed()))


def test_fingerprint_ignores_pbp_order_and_clock():
    reordered = _feed()
    reordered["pbp"].reverse()
    reordered["clock"] = "00:01"
    assert feed_fingerprint(reordered) == feed_fingerprint(_feed())


def test_fingerprint_changes_when_a_stat_is_corrected():
    corrected = _feed()
    corrected["tm"]["1"]["pl"]["1"]["sBlocks"] = 2
    assert feed_fingerprint(corrected) != feed_fingerprint(_feed())


def test_fingerprint_changes_when_a_late_action_is_added():
    corrected = _feed()
    corrected["pbp"].append({"actionNumber": 763, "period": 2, "gt": "00:00", "actionType": "block", "tno": 1, "pno": 2, "player": "M. Diggins Jr", "success": True, "previousAction": 363})
    assert feed_fingerprint(corrected) != feed_fingerprint(_feed())


def test_fingerprint_changes_when_an_action_is_credited_to_another_player():
    corrected = _feed()
    corrected["pbp"][0]["player"] = "D. Bailey"
    assert feed_fingerprint(corrected) != feed_fingerprint(_feed())
