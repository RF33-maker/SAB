"""
A live game's stints are rebuilt at most every LIVE_REBUILD_SECONDS, and the
league's season on/off summary is only recomputed when a game is final.

Run: python -m pytest tests/test_live_rebuild_throttle.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils import lineup_builder as lb  # noqa: E402


def _stub(monkeypatch):
    """Replace the database-touching steps; record what a build did."""
    calls = {"writes": [], "refresh": 0}
    monkeypatch.setattr(lb, "load_game_rosters", lambda game_key: {"A": [{}], "B": [{}]})
    monkeypatch.setattr(lb, "load_live_events", lambda game_key: [{"team_id": "A"}])
    monkeypatch.setattr(lb, "compute_stints", lambda *a, **k: [{"stint": 1}])

    def write(rosters, stints, game_key, league_id, refresh_season=True):
        calls["writes"].append((game_key, refresh_season))
        if refresh_season:
            calls["refresh"] += 1
        return True

    monkeypatch.setattr(lb, "_write_stints", write)
    lb._last_live_build.clear()
    return calls


def test_live_rebuilds_are_spaced_out(monkeypatch):
    calls = _stub(monkeypatch)
    clock = {"t": 1000.0}
    monkeypatch.setattr(lb.time, "monotonic", lambda: clock["t"])

    assert lb.build_lineups_for_game("G1", "L", live=True)      # first poll builds
    clock["t"] += 15
    assert lb.build_lineups_for_game("G1", "L", live=True)      # 15s later: skipped
    clock["t"] += lb.LIVE_REBUILD_SECONDS
    assert lb.build_lineups_for_game("G1", "L", live=True)      # due again
    assert [w[0] for w in calls["writes"]] == ["G1", "G1"]


def test_live_builds_never_refresh_the_season(monkeypatch):
    calls = _stub(monkeypatch)
    lb.build_lineups_for_game("G1", "L", live=True)
    assert calls["writes"] == [("G1", False)]
    assert calls["refresh"] == 0


def test_final_build_always_runs_and_refreshes(monkeypatch):
    calls = _stub(monkeypatch)
    lb.build_lineups_for_game("G1", "L", live=True)
    lb.build_lineups_for_game("G1", "L")                         # final, seconds later
    assert calls["writes"] == [("G1", False), ("G1", True)]
    assert calls["refresh"] == 1


def test_each_game_has_its_own_clock(monkeypatch):
    calls = _stub(monkeypatch)
    lb.build_lineups_for_game("G1", "L", live=True)
    lb.build_lineups_for_game("G2", "L", live=True)
    assert [w[0] for w in calls["writes"]] == ["G1", "G2"]


def test_failed_live_build_is_not_retried_every_poll(monkeypatch):
    calls = _stub(monkeypatch)
    monkeypatch.setattr(lb, "load_live_events", lambda game_key: [])   # nothing to build from
    assert lb.build_lineups_for_game("G1", "L", live=True) is False
    assert lb.live_rebuild_due("G1") is False
    assert calls["writes"] == []
