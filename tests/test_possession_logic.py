"""
Possession counting in the lineup builder, driven through compute_stints with
short synthetic games so the tests exercise the code that runs in production.

Rules covered:
  - the ball changes hands on a made FG, a turnover, a defensive rebound, or a
    made last free throw; an offensive rebound keeps it
  - single free throws (and-ones, technicals), unsportsmanlike free throws
    and dead-ball team rebounds between free throws don't change possession
  - free throws, and the possession they end, belong to the lineups on court
    for the foul, even when substitutions come in before the shots
  - possessions alternate even when the feed misses an event in between
  - an open possession counts at the end of a period only if the team did
    something with it
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.lineup_builder import compute_stints  # noqa: E402

A, B = "team-a", "team-b"
TEAM_NO = {A: "1", B: "2"}


def roster(team):
    rows = []
    for n in range(1, 8):
        rows.append({
            "team_id": team, "team_no": TEAM_NO[team], "player_name": f"{team} {n}",
            "shirt_number": str(n), "pno": n, "starter": n <= 5, "active": True,
            "player_id": f"{team}-p{n}",
        })
    return rows


class Game:
    def __init__(self):
        self.events = []
        self.secs = 600  # seconds left in the period
        self.period = 1

    def _add(self, team, action_type, sub_type="", success=None, pno=1, tick=True):
        if tick:
            self.secs -= 5
        self.events.append({
            "action_number": len(self.events) + 1, "period": self.period,
            "clock": f"{self.secs // 60:02d}:{self.secs % 60:02d}",
            "team_no": TEAM_NO.get(team, ""), "team_id": team,
            "player_id": f"{team}-p{pno}" if pno else None, "pno": pno,
            "player_name": f"{team} {pno}" if pno else "",
            "action_type": action_type, "sub_type": sub_type,
            "success": success, "scoring": bool(success),
        })
        return self

    def shot(self, team, made, three=False):
        return self._add(team, "3pt" if three else "2pt", "jumpshot", made)

    def ft(self, team, n, of, made):
        return self._add(team, "freethrow", f"{n}of{of}", made, tick=False)

    def foul(self, team, kind="personal", tick=True):
        return self._add(team, "foul", kind, tick=tick)

    def reb(self, team, kind, pno=1):
        return self._add(team, "rebound", kind, pno=pno, tick=False)

    def turnover(self, team):
        return self._add(team, "turnover", "badpass")

    def steal(self, team):
        return self._add(team, "steal", tick=False)

    def sub(self, team, out_pno, in_pno):
        self._add(team, "substitution", "out", pno=out_pno, tick=False)
        return self._add(team, "substitution", "in", pno=in_pno, tick=False)

    def next_period(self):
        self.period += 1
        self.secs = 600
        return self

    def stints(self):
        return compute_stints({A: roster(A), B: roster(B)}, self.events, "game", "league")


def totals(stints, team, key):
    return sum(s[key] for s in stints if s["team_id"] == team)


def poss(stints):
    return {t: totals(stints, t, "possessions_for") for t in (A, B)}


def assert_balanced(stints):
    assert totals(stints, A, "possessions_for") == totals(stints, B, "possessions_against")
    assert totals(stints, B, "possessions_for") == totals(stints, A, "possessions_against")


def test_made_field_goal_ends_the_possession():
    s = Game().shot(A, True).stints()
    assert poss(s) == {A: 1, B: 0}
    assert_balanced(s)


def test_offensive_rebound_keeps_the_possession():
    s = Game().shot(A, False).reb(A, "offensive").shot(A, False).reb(A, "offensive").shot(A, True).stints()
    assert poss(s) == {A: 1, B: 0}


def test_defensive_rebound_ends_the_possession():
    s = Game().shot(A, False).reb(B, "defensive").shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}
    assert_balanced(s)


def test_turnover_and_steal_are_one_possession():
    s = Game().turnover(A).steal(B).shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}


def test_made_last_free_throw_ends_the_possession():
    s = Game().shot(A, True).foul(A).ft(B, 1, 2, True).ft(B, 2, 2, True).shot(A, False).reb(B, "defensive").stints()
    assert poss(s) == {A: 2, B: 1}
    assert totals(s, B, "points_for") == 2
    assert_balanced(s)


def test_missed_last_free_throw_then_defensive_rebound_is_one_possession():
    s = Game().foul(B).ft(A, 1, 2, True).ft(A, 2, 2, False).reb(B, "defensive").stints()
    assert poss(s) == {A: 1, B: 0}


def test_and_one_is_one_possession():
    # The feed logs the and-one foul at the same second as the basket.
    s = Game().shot(A, True).foul(B, tick=False).ft(A, 1, 1, True).shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}
    assert totals(s, A, "points_for") == 3


def test_technical_free_throw_does_not_change_possession():
    # B has the ball; a technical on A's coach gives B one shot and B keeps it.
    s = Game().shot(A, True).foul(A, "coachTechnical").ft(B, 1, 1, True).shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}
    assert totals(s, B, "points_for") == 3

    # A technical on the team with the ball: the shooters are on defence.
    s = Game().shot(A, True).foul(B, "technical").ft(A, 1, 1, True).shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}


def test_free_throws_after_the_other_side_gained_the_ball_are_a_new_possession():
    # A scores, B inbounds, and later A shoots bonus free throws: B's
    # possession ended somewhere the feed didn't log, and A has a new one.
    s = Game().shot(A, True).foul(B).ft(A, 1, 2, True).ft(A, 2, 2, True).stints()
    assert poss(s) == {A: 2, B: 1}
    assert_balanced(s)


def test_possessions_alternate_when_the_feed_misses_an_event():
    # A scores, then A shoots again with no B event in between.
    s = Game().shot(A, True).shot(A, False).reb(B, "defensive").stints()
    assert poss(s) == {A: 2, B: 1}


def test_single_free_throw_never_moves_the_ball():
    # A technical shot by the team with the ball, with no technical foul logged.
    s = Game().foul(B).ft(A, 1, 1, True).shot(A, True).stints()
    assert poss(s) == {A: 1, B: 0}
    # An and-one logged a few seconds after the basket.
    s = Game().shot(A, True).foul(B).ft(A, 1, 1, True).shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}


def test_unsportsmanlike_free_throws_keep_the_ball():
    s = Game().foul(B, "unsportsmanlike").ft(A, 1, 2, True).ft(A, 2, 2, True).shot(A, True).stints()
    assert poss(s) == {A: 1, B: 0}
    assert totals(s, A, "points_for") == 4


def test_dead_ball_team_rebound_between_free_throws_is_ignored():
    g = Game().foul(B).ft(A, 1, 2, False)
    g.reb(B, "defensive", pno=None)  # team rebound credited to the defence
    s = g.ft(A, 2, 2, True).shot(B, True).stints()
    assert poss(s) == {A: 1, B: 1}


def test_free_throws_go_to_the_lineup_on_court_for_the_foul():
    # B fouls A's p1, then both teams substitute before the shots.
    s = Game().foul(B).sub(A, 1, 6).sub(B, 2, 7).ft(A, 1, 2, True).ft(A, 2, 2, True).shot(B, True).stints()
    a_first, a_second = sorted((x for x in s if x["team_id"] == A), key=lambda x: x["start_game_secs"])
    b_first, b_second = sorted((x for x in s if x["team_id"] == B), key=lambda x: x["start_game_secs"])
    assert "team-a-p1" in a_first["lineup_player_ids"]
    assert (a_first["points_for"], a_first["possessions_for"], a_first["ft_attempted"]) == (2, 1, 2)
    assert (a_second["points_for"], a_second["possessions_for"]) == (0, 0)
    assert (b_first["points_against"], b_first["possessions_against"], b_first["opp_ft_made"]) == (2, 1, 2)
    assert (b_second["points_for"], b_second["possessions_for"]) == (2, 1)
    assert_balanced(s)


def test_open_possession_counts_at_period_end_only_if_used():
    # A scores, B takes a shot and the quarter ends: B's possession counts.
    s = Game().shot(A, True).shot(B, False).next_period().shot(A, True).stints()
    assert poss(s)[B] == 1
    # A scores and B runs the clock out without doing anything: nothing for B.
    s = Game().shot(A, True).next_period().shot(A, True).stints()
    assert poss(s) == {A: 2, B: 0}


def test_unknown_team_adds_nothing():
    g = Game()
    g._add("nobody", "rebound", "defensive")
    g._add("nobody", "turnover", "badpass")
    assert poss(g.stints()) == {A: 0, B: 0}


def test_possessions_alternate_through_a_mixed_sequence():
    g = (Game().shot(A, True).turnover(B).steal(A).shot(A, False).reb(B, "defensive")
         .foul(A).ft(B, 1, 2, True).ft(B, 2, 2, True).shot(A, False).reb(A, "offensive")
         .shot(A, True, three=True).foul(B, tick=False).ft(A, 1, 1, False).reb(B, "defensive").shot(B, True))
    s = g.stints()
    assert poss(s) == {A: 3, B: 3}
    assert_balanced(s)
