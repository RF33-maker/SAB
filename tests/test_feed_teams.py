"""
Finished games' LiveStats feeds put league-leader blocks under "tm" next to the
two teams; only "1" and "2" are teams.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "test")

from app.utils.json_parser import feed_teams

LEADERS = {"lds": {"sBlocks": [], "sSteals": [], "sAssists": [], "sReboundsTotal": [], "sPoints": []}}


def test_skips_league_leader_blocks():
    # Shape of Greenwich Titans v Solent Kestrels (LiveStats 2908084) at full time.
    data = {"tm": {
        "1": {"name": "Greenwich Titans Senior Men", "score": 64},
        "2": {"name": "Solent Kestrels Senior Men", "score": 73},
        "sPointsAverage": LEADERS,
        "sReboundsTotalAverage": LEADERS,
        "sAssistsAverage": LEADERS,
    }}
    assert list(feed_teams(data)) == ["1", "2"]


def test_live_feed_unchanged():
    data = {"tm": {"1": {"name": "A"}, "2": {"name": "B"}}}
    assert feed_teams(data) == data["tm"]


def test_no_teams_yet():
    assert feed_teams({}) == {}
    assert feed_teams({"tm": None}) == {}
