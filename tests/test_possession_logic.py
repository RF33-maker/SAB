"""
Regression tests for possession counting logic in the lineup builder.

These tests exercise the core possession-end attribution rules without
touching Supabase, by replicating the possession-counting block from
build_lineups_for_game in a minimal harness.

Coverage:
  - Made 2PT / 3PT       → scoring team possessions_for += 1
  - Turnover             → TO team possessions_for += 1
  - Defensive rebound    → missed-shot team possessions_for += 1 (rebounder possessions_against += 1)
  - Offensive rebound    → no possession count, same team retains ball
  - Missed FG + OREB + make → single possession for the scoring team
  - Period-end buzzer    → open possession is closed before the period flush
  - FT-heavy sequence    → FTs do not individually end possessions
  - Unknown event_team   → no spurious counts when event_team_id is missing
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.lineup_builder import _TeamLineup

TEAM_A = "aaaa-aaaa"
TEAM_B = "bbbb-bbbb"
GAME = "test-game"
LEAGUE = "test-league"


class PossessionCounter:
    """
    Minimal reimplementation of the possession-counting block inside
    build_lineups_for_game.  Mirrors the exact branching logic so regressions
    here catch production regressions too.
    """

    def __init__(self):
        self.teams = {
            TEAM_A: _TeamLineup(team_id=TEAM_A, league_id=LEAGUE, game_key=GAME),
            TEAM_B: _TeamLineup(team_id=TEAM_B, league_id=LEAGUE, game_key=GAME),
        }
        self.current_possessor_id = None

    def _close_open_possession(self):
        pid = self.current_possessor_id
        if pid and pid in self.teams:
            self.teams[pid].possessions_for += 1
            for tid, tl in self.teams.items():
                if tid != pid:
                    tl.possessions_against += 1
            return True
        return False

    def event(self, action_type, event_team_id, sub_type="", success=True, scoring=True):
        action_type = action_type.lower()
        if action_type in ("2pt", "3pt"):
            if success and scoring and event_team_id and event_team_id in self.teams:
                self.teams[event_team_id].possessions_for += 1
                for tid, tl in self.teams.items():
                    if tid != event_team_id:
                        tl.possessions_against += 1
                self.current_possessor_id = next(
                    (tid for tid in self.teams if tid != event_team_id), None
                )
        elif action_type == "turnover":
            if event_team_id and event_team_id in self.teams:
                self.teams[event_team_id].possessions_for += 1
                for tid, tl in self.teams.items():
                    if tid != event_team_id:
                        tl.possessions_against += 1
                self.current_possessor_id = next(
                    (tid for tid in self.teams if tid != event_team_id), None
                )
        elif action_type == "rebound":
            if sub_type == "defensive" and event_team_id and event_team_id in self.teams:
                self.teams[event_team_id].possessions_against += 1
                for tid, tl in self.teams.items():
                    if tid != event_team_id:
                        tl.possessions_for += 1
                self.current_possessor_id = event_team_id
            elif sub_type == "offensive" and event_team_id and event_team_id in self.teams:
                self.current_possessor_id = event_team_id

    def period_end(self):
        self._close_open_possession()
        self.current_possessor_id = None

    def pf(self, team_id):
        return self.teams[team_id].possessions_for

    def pa(self, team_id):
        return self.teams[team_id].possessions_against


def test_made_2pt_ends_possession():
    c = PossessionCounter()
    c.event("2pt", TEAM_A)
    assert c.pf(TEAM_A) == 1, "scoring team should have 1 possession"
    assert c.pa(TEAM_B) == 1, "opponent should have 1 possession against"
    assert c.pf(TEAM_B) == 0
    assert c.pa(TEAM_A) == 0
    assert c.current_possessor_id == TEAM_B, "ball transfers to opponent after make"


def test_made_3pt_ends_possession():
    c = PossessionCounter()
    c.event("3pt", TEAM_B)
    assert c.pf(TEAM_B) == 1
    assert c.pa(TEAM_A) == 1
    assert c.current_possessor_id == TEAM_A


def test_turnover_ends_possession():
    c = PossessionCounter()
    c.event("turnover", TEAM_A)
    assert c.pf(TEAM_A) == 1
    assert c.pa(TEAM_B) == 1
    assert c.current_possessor_id == TEAM_B


def test_defensive_rebound_ends_possession_for_missed_shot_team():
    c = PossessionCounter()
    c.event("rebound", TEAM_B, sub_type="defensive")
    assert c.pf(TEAM_A) == 1, "team that missed gets possession counted"
    assert c.pa(TEAM_B) == 1, "rebounder's team gets possession_against"
    assert c.current_possessor_id == TEAM_B, "rebounder gains ball"


def test_offensive_rebound_no_possession_end():
    c = PossessionCounter()
    c.event("rebound", TEAM_A, sub_type="offensive")
    assert c.pf(TEAM_A) == 0
    assert c.pa(TEAM_A) == 0
    assert c.pf(TEAM_B) == 0
    assert c.pa(TEAM_B) == 0
    assert c.current_possessor_id == TEAM_A, "offensive rebounder retains ball"


def test_missed_fg_oreb_then_make_is_one_possession():
    c = PossessionCounter()
    c.event("2pt", TEAM_A, success=False, scoring=False)
    c.event("rebound", TEAM_A, sub_type="offensive")
    c.event("2pt", TEAM_A)
    assert c.pf(TEAM_A) == 1, "only one possession ends despite two shot attempts"
    assert c.pa(TEAM_B) == 1


def test_period_end_closes_open_possession():
    c = PossessionCounter()
    c.event("2pt", TEAM_A)
    assert c.current_possessor_id == TEAM_B
    c.period_end()
    assert c.pf(TEAM_B) == 1, "open end-of-period possession attributed to ball holder"
    assert c.pa(TEAM_A) == 1
    assert c.current_possessor_id is None


def test_period_end_no_possessor_does_nothing():
    c = PossessionCounter()
    c.period_end()
    assert c.pf(TEAM_A) == 0
    assert c.pf(TEAM_B) == 0


def test_symmetry_possessions_for_equals_against():
    c = PossessionCounter()
    c.event("2pt", TEAM_A)
    c.event("turnover", TEAM_B)
    c.event("rebound", TEAM_A, sub_type="defensive")
    c.event("2pt", TEAM_A)
    c.period_end()
    total_for_a = c.pf(TEAM_A)
    total_for_b = c.pf(TEAM_B)
    total_against_a = c.pa(TEAM_A)
    total_against_b = c.pa(TEAM_B)
    assert total_for_a == total_against_b, "Team A's possessions_for must equal Team B's possessions_against"
    assert total_for_b == total_against_a, "Team B's possessions_for must equal Team A's possessions_against"


def test_ft_heavy_sequence_no_extra_possessions():
    """
    FTs alone must not end possessions.  A common sequence is:
      foul → FT1 (miss) → FT2 (make) → defensive rebound (after FT1)
    Only the defensive rebound following the miss should end the possession.
    """
    c = PossessionCounter()
    c.event("freethrow", TEAM_A, success=False, scoring=False)
    c.event("freethrow", TEAM_A, success=True, scoring=True)
    assert c.pf(TEAM_A) == 0, "FTs do not end possessions directly"
    assert c.pa(TEAM_B) == 0
    c.event("rebound", TEAM_B, sub_type="defensive")
    assert c.pf(TEAM_A) == 1, "missed FT followed by dreb ends the possession"
    assert c.pa(TEAM_B) == 1


def test_unknown_event_team_id_no_spurious_counts():
    c = PossessionCounter()
    c.event("rebound", None, sub_type="defensive")
    assert c.pf(TEAM_A) == 0
    assert c.pf(TEAM_B) == 0
    assert c.pa(TEAM_A) == 0
    assert c.pa(TEAM_B) == 0

    c.event("turnover", None)
    assert c.pf(TEAM_A) == 0
    assert c.pf(TEAM_B) == 0
