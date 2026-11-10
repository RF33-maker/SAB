"""
Tests for reading a season from a competition name, and for when a team found
under another season of its league is moved onto the competition being parsed.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.seasons import season_start_year, should_move_team_forward


def test_season_formats():
    assert season_start_year("NBL Division One 2026-27") == 2026
    assert season_start_year("NBL Mens National Cup 26-27") == 2026
    assert season_start_year("NBL Division One 2018/19") == 2018
    assert season_start_year("BCB Trophy 2025-2026") == 2025
    assert season_start_year("BCB Trophy 2027") == 2027
    assert season_start_year("Future Stars") is None
    assert season_start_year(None) is None


def test_new_season_moves_team_forward():
    assert should_move_team_forward("NBL Division One 2025-26", "NBL Division One 2026-27")


def test_same_season_still_moves():
    assert should_move_team_forward("BCB Trophy 2026-2027", "BCB Trophy 26-27")


def test_history_import_does_not_move_team_back():
    assert not should_move_team_forward("NBL Division One 2026-27", "NBL Division One 2018/19")
    assert not should_move_team_forward("WNBL Division One 2026-27", "WNBL Division One 2024-25")


def test_unknown_season_keeps_old_behaviour():
    assert should_move_team_forward("Future Stars", "NBL Division One 2018/19")
    assert should_move_team_forward("NBL Division One 2026-27", None)
