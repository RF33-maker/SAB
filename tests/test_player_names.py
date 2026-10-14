"""
Tests for matching a feed name against a player we already hold: the
play-by-play's "D. Crnac" is the box score's "Dzana Crnac".
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.utils.player_names import fold_name_part, initial_matches_full_name


def test_abbreviation_with_a_full_stop_matches_the_full_name():
    assert initial_matches_full_name("D. Crnac", "Dzana Crnac")
    assert initial_matches_full_name("Dzana Crnac", "D. Crnac")
    assert initial_matches_full_name("A. Cerino", "Alyssa Marie Cerino")


def test_abbreviation_without_a_full_stop_matches_too():
    assert initial_matches_full_name("D Crnac", "Dzana Crnac")


def test_icelandic_letters_fold_to_the_feeds_spelling():
    assert initial_matches_full_name("T. Hodge-Carr", "Þóranna Kika Hodge-Carr")
    assert initial_matches_full_name("B. Magnusdottir", "Bergdís Anna Magnúsdóttir")
    assert initial_matches_full_name("A. Thordardottir", "Auður Ósk Þórðardóttir")
    assert fold_name_part("Þóranna") == "thoranna"
    assert fold_name_part("Magnúsdóttir") == "magnusdottir"


def test_different_initial_or_surname_does_not_match():
    assert not initial_matches_full_name("M. Ball", "Dana Ball")
    assert not initial_matches_full_name("D. Crnac", "Dzana Crnic")


def test_two_full_names_are_not_an_abbreviation_match():
    # "Marlee Ball" vs "Maya Ball" share an initial and surname but are both full names.
    assert not initial_matches_full_name("Marlee Ball", "Maya Ball")


def test_different_shirt_numbers_mean_different_people():
    assert not initial_matches_full_name("M. Ball", "Marlee Ball", shirt_a=5, shirt_b=7)
    assert initial_matches_full_name("M. Ball", "Marlee Ball", shirt_a=5, shirt_b=5)
    assert initial_matches_full_name("M. Ball", "Marlee Ball", shirt_a=None, shirt_b=5)


def test_single_word_names_never_match():
    assert not initial_matches_full_name("Crnac", "D. Crnac")
    assert not initial_matches_full_name("", "D. Crnac")
