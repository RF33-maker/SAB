"""
The away team in a Genius Sports PDF header. Short names share the score line
("Team 3 66 – 84 Team 4"); long ones wrap onto a later line. The old parser only
looked at later lines and skipped any with a digit, so REBA's "Team Gold 15U"
was passed over and box score labels ("Coach:", "Quarter Starters:") were
saved as the away team.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ.setdefault("SUPABASE_URL", "http://localhost")
os.environ.setdefault("SUPABASE_KEY", "test")

from app.utils.pdf_parser import _parse_header


def header(*lines):
    return "\n".join(lines)


def test_both_teams_on_the_score_line():
    meta = _parse_header(header(
        "REBA Summer League 19+ FIBA Box Score",
        "UEL Sports Dock, Mon 30 Mar 2026 Start time: 09:00",
        "Game No.: 2026033009002",
        "Game Duration: 00:47",
        "Team 3 66 – 84 Team 4",
        "Report Generated: Mon 30 Mar 2026 13:10",
        "(14-29, 29-20, 4-17, 19-18)",
        "Q1 Q2 Q3 Q4",
        "Coach:",
        "Team 3 (TM3)​ Assistant Coach(es):",
    ))
    assert (meta["home_team_full"], meta["away_team_full"]) == ("Team 3", "Team 4")
    assert (meta["home_score"], meta["away_score"]) == (66, 84)


def test_away_name_with_digits():
    meta = _parse_header(header(
        "REBA SL Bristol stop FIBA Box Score",
        "Game No.: 281001",
        "Team Black 15U 68 – 70 Team Gold 15U",
        "Report Generated: Tue 28 Oct 2026 12:00",
        "Q1 Q2 Q3 Q4",
        "Coach:",
    ))
    assert (meta["home_team_full"], meta["away_team_full"]) == ("Team Black 15U", "Team Gold 15U")


def test_away_team_on_a_later_line():
    meta = _parse_header(header(
        "WEABL 2025-26 FIBA Box Score",
        "Essex Sport Arena, Thu 26 Mar 2026 Start time: 14:00",
        "Game No.: 5193600",
        "Game Duration: 01:39",
        "City of London Academy - Southwark WEABL 66 – 56",
        "Report Generated: Thu 26 Mar 2026 16:20",
        "Copleston High School WEABL",
        "(20-16, 8-17, 17-9, 21-14)",
        "Coach: Jackson Gibbons",
    ))
    assert meta["away_team_full"] == "Copleston High School WEABL"


def test_header_fields_beside_the_names():
    # Plus/Minus summaries put Game Duration after the score and Report Generated after the away team.
    meta = _parse_header(header(
        "WEABL 2025-26 Player Plus/Minus",
        "Essex Sport Arena, Thu 26 Mar 2026 Start time: 14:00 Summary",
        "Game No.: 5193600",
        "City of London Academy - Southwark WEABL 66 – 56 Game Duration: 01:39",
        "Copleston High School WEABL Report Generated: Thu 26 Mar 2026 16:21",
        "(20-16, 8-17, 17-9, 21-14)",
    ))
    assert meta["home_team_full"] == "City of London Academy - Southwark WEABL"
    assert meta["away_team_full"] == "Copleston High School WEABL"


def test_labels_are_never_the_away_team():
    for label in ("Coach:", "Coach: Sam Gilbert", "Quarter Starters:"):
        meta = _parse_header(header(
            "REBA Summer League FIBA Box Score",
            "Game No.: 1",
            "Home Side 60 – 50",
            "Report Generated: Mon 30 Mar 2026 13:10",
            "Q1 Q2 Q3 Q4",
            label,
        ))
        assert meta["away_team_full"] is None, label
