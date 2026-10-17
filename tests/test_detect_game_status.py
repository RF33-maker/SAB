"""
Tests for detect_game_status: a live game must not be called final early.

Regression: the feed's top-level "period" moves on to the next period during a
break, so the end of Q3 (feed already on period 4) was read as the end of the
game and live games were marked final at the end of the third quarter.
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "test")

try:
    import requests  # noqa: F401
    from supabase import create_client  # noqa: F401
    import app.utils.json_parser  # noqa: F401
    import app.utils.compute_advanced_stats  # noqa: F401
except ImportError:
    # Only the pure status function is under test; stub the heavy dependencies
    # (network clients, pandas-based parsing) when they are not installed.
    for _mod in (
        "requests",
        "supabase",
        "supabase.lib",
        "supabase.lib.client_options",
        "app.utils.json_parser",
        "app.utils.compute_advanced_stats",
    ):
        sys.modules[_mod] = MagicMock()

from app.worker import detect_game_status


def _feed(feed_period, event_period, score=(61, 59), sub_type="end", periods_max=4):
    return {
        "period": feed_period,
        "periodsMax": periods_max,
        "clock": "00:00",
        "tm": {"1": {"tot_sPoints": score[0]}, "2": {"tot_sPoints": score[1]}},
        "pbp": [
            {"actionNumber": 458, "actionType": "3pt", "subType": "jumpshot", "period": event_period},
            {"actionNumber": 459, "actionType": "period", "subType": sub_type, "period": event_period},
        ],
    }


def test_end_of_third_quarter_stays_live_even_if_feed_already_on_period_4():
    assert detect_game_status(_feed(feed_period=4, event_period=3), "live") == "live"


def test_end_of_third_quarter_stays_live():
    assert detect_game_status(_feed(feed_period=3, event_period=3), "live") == "live"


def test_halftime_stays_live():
    assert detect_game_status(_feed(feed_period=3, event_period=2), "live") == "live"


def test_last_period_ending_without_game_end_is_final():
    assert detect_game_status(_feed(feed_period=4, event_period=4), "live") == "final"


def test_last_period_ending_level_is_not_final():
    assert detect_game_status(_feed(feed_period=4, event_period=4, score=(70, 70)), "live") == "live"


def test_overtime_period_ending_unlevel_is_final():
    assert detect_game_status(_feed(feed_period=5, event_period=5), "live") == "final"


def test_game_end_event_is_still_final():
    feed = _feed(feed_period=4, event_period=4)
    feed["pbp"].append({"actionNumber": 460, "actionType": "game", "subType": "end", "period": 4})
    assert detect_game_status(feed, "live") == "final"
