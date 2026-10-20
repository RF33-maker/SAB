"""Team rating and pace calculations in advanced_team_stats."""
import sys
import types

# advanced_team_stats imports a live Supabase client at module load; stub it for unit tests.
if "app.utils.supabase_queries" not in sys.modules:
    stub = types.ModuleType("app.utils.supabase_queries")
    stub.supabase = None
    sys.modules["app.utils.supabase_queries"] = stub

from app.utils import advanced_team_stats as ats  # noqa: E402


def box(fga, fta, orb, tov, pts, minutes="199:60"):
    return {
        "tot_sfieldgoalsattempted": fga,
        "tot_sfreethrowsattempted": fta,
        "tot_sreboundsoffensive": orb,
        "tot_sturnovers": tov,
        "tot_spoints": pts,
        "tot_sminutes": minutes,
    }


def compute(team, opp, monkeypatch):
    written = {}
    monkeypatch.setattr(ats, "write_team_advanced_to_supabase", lambda _id, fields: written.update(fields))
    ats.process_team_vs_opponent({**team, "id": "t"}, opp)
    return written


def test_parse_game_minutes_handles_feed_format():
    assert ats.parse_game_minutes("199:60") == 40
    assert ats.parse_game_minutes("200:00") == 40
    assert ats.parse_game_minutes("224:60") == 45
    assert ats.parse_game_minutes("249:60") == 50


def test_parse_game_minutes_falls_back_to_40():
    for bad in (None, "", "abc", "12:00", "999:00"):
        assert ats.parse_game_minutes(bad) == 40


def test_ratings_share_one_possession_count(monkeypatch):
    # Hemel Storm v Milton Keynes Breakers, BCB 3 Oct 2026 (double overtime).
    hemel = box(93, 26, 20, 20, 102, "249:60")
    mk = box(89, 17, 16, 31, 94, "249:60")
    h = compute(hemel, mk, monkeypatch)
    m = compute(mk, hemel, monkeypatch)

    shared = (103.4 + 110.8) / 2
    assert round(h["off_rating"], 2) == round(100 * 102 / shared, 2)
    assert round(h["def_rating"], 2) == round(100 * 94 / shared, 2)
    # One team's offence is exactly the other's defence.
    assert h["off_rating"] == m["def_rating"]
    assert h["def_rating"] == m["off_rating"]
    # Raw possession estimates are still stored as they were.
    assert round(h["possessions"], 1) == 103.4
    assert round(h["opp_possessions"], 1) == 110.8


def test_pace_is_per_40_minutes_in_overtime(monkeypatch):
    hemel = box(93, 26, 20, 20, 102, "249:60")
    mk = box(89, 17, 16, 31, 94, "249:60")
    h = compute(hemel, mk, monkeypatch)
    assert round(h["pace"], 1) == round(107.1 * 40 / 50, 1)  # 85.7, not 107.1


def test_pace_unchanged_in_regulation(monkeypatch):
    a = box(78, 15, 13, 22, 86)
    b = box(78, 15, 8, 19, 103)
    assert round(compute(a, b, monkeypatch)["pace"], 1) == 94.0
