"""
Tests for choosing which existing player a feed name belongs to. The cases are the
ones that split players into two ids on 17 Oct 2026, when box scores started landing
on the "X. Surname" stubs the play-by-play had left behind.
"""
import itertools
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.player_names import clean_feed_name, is_initial_name, name_key, pick_player


def player(id, full_name, shirt=None, league_id="L1", aliases=None, created_at="2026-09-01"):
    return {"id": id, "full_name": full_name, "shirtNumber": shirt, "league_id": league_id,
            "aliases": aliases, "created_at": created_at}


def picked(feed_name, shirt, candidates, **kwargs):
    match, _ = pick_player(feed_name, shirt, candidates, **kwargs)
    return match["id"] if match else None


def picks_regardless_of_order(feed_name, shirt, candidates, **kwargs):
    answers = {picked(feed_name, shirt, list(order), **kwargs) for order in itertools.permutations(candidates)}
    assert len(answers) == 1, f"answer depends on row order: {answers}"
    return answers.pop()


def test_full_name_goes_to_the_full_name_record_not_the_play_by_play_stub():
    # Ethan Albrecht: record created wearing 22, now wears 18, and the stub "E. Albrecht" #18.
    candidates = [player("stub", "E. Albrecht", 18, created_at="2026-10-10"), player("real", "Ethan Albrecht", 22)]
    assert picks_regardless_of_order("Ethan Albrecht", 18, candidates) == "real"


def test_spacing_and_case_do_not_stop_an_exact_match():
    # Dylan Ameen is stored as "Dylan  Ameen"; the stub is "D. AMEEN".
    candidates = [player("stub", "D. AMEEN", 0), player("real", "Dylan  Ameen", 0)]
    assert picks_regardless_of_order("Dylan Ameen", 0, candidates) == "real"


def test_repolling_with_a_new_stub_on_the_team_keeps_the_same_id():
    # Myerscough #14: the pre-game poll matched "Alexander Smith" (record says #4); a
    # stub "A. Smith" #14 appeared mid-game. The next poll must not move to the stub.
    before = [player("real", "Alexander Smith", 4)]
    after = before + [player("stub", "A. Smith", 14, created_at="2026-10-17")]
    assert picked("Alexander Smith", 14, before) == "real"
    assert picks_regardless_of_order("Alexander Smith", 14, after) == "real"


def test_abbreviation_reaches_the_full_name_record():
    candidates = [player("real", "Dzana Crnac", 7), player("other", "Maya Ball", 7)]
    assert picks_regardless_of_order("D. Crnac", 7, candidates) == "real"


def test_initial_only_record_matches_a_full_name_on_a_shirt_they_have_worn():
    # "R Quidam" was created wearing 10 but has worn 11 since; the feed now says "Ramsay Quidam" #11.
    candidates = [player("old", "R Quidam", 10, league_id="L0")]
    worn = lambda ids: {"old": {"10", "11", "24"}}
    assert picked("Ramsay Quidam", 11, candidates, league_id="L1", shirts_worn=worn) == "old"


def test_initial_only_record_on_a_shirt_never_worn_is_someone_else():
    candidates = [player("noah", "N Saa", 9)]
    worn = lambda ids: {"noah": {"9"}}
    assert picked("Nestor Saa", 6, candidates, shirts_worn=worn) is None


def test_abbreviation_shared_by_brothers_needs_the_shirt():
    candidates = [player("noah", "Noah Saa", 9), player("nestor", "Nestor Saa", 6)]
    worn = lambda ids: {}
    assert picks_regardless_of_order("N. Saa", 6, candidates, shirts_worn=worn) == "nestor"
    assert picks_regardless_of_order("N. Saa", None, candidates, shirts_worn=worn) is None


def test_two_stubs_for_the_same_person_are_not_ambiguous():
    candidates = [player("a", "D. Ameen", 0, created_at="2026-09-19"), player("b", "D. AMEEN", 0, created_at="2026-10-01")]
    assert picks_regardless_of_order("Dylan Ameen", 0, candidates) == "a"


def test_alias_matches_a_corrected_name():
    candidates = [player("ben", "Ben Baker", 5, aliases=["Benedict Baker-Mccann"])]
    assert picked("Benedict Baker-McCann", 5, candidates) == "ben"


def test_close_spelling_of_a_full_name_matches():
    candidates = [player("real", "Danny Mcdonald", 8)]
    assert picked("Danny McDonald", 3, candidates) == "real"


def test_same_name_prefers_the_matching_shirt_then_this_competition():
    candidates = [player("old", "Corey Johnson", 5, league_id="L0", created_at="2025-09-01"),
                  player("new", "Corey Johnson", 5, league_id="L1", created_at="2026-09-01")]
    assert picks_regardless_of_order("Corey Johnson", 5, candidates, league_id="L1") == "new"


def test_nobody_on_the_team_means_a_new_player():
    assert picked("Someone New", 4, [player("x", "Different Person", 4)]) is None
    assert picked("Someone New", 4, []) is None


def test_names_are_cleaned_before_storing():
    assert clean_feed_name("Dylan  Ameen ") == "Dylan Ameen"
    assert clean_feed_name("Laâ€™Quarne Santana Sayles") == "La’Quarne Santana Sayles"
    assert clean_feed_name("JosÃ© Silva") == "José Silva"
    assert clean_feed_name("Þóranna Kika") == "Þóranna Kika"
    assert name_key("D. AMEEN") == name_key("d  ameen")
    assert is_initial_name("S. Walker") and is_initial_name("S Walker") and not is_initial_name("Sean Walker")
