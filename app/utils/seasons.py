"""
Season helpers for competition names.

Kept free of database and environment access so it can be unit tested.
"""

import re


def season_start_year(name: str):
    """
    A competition's season start year from its name, handling the formats
    used across leagues: '2026-27', '2026-2027', '2025/26', a 2-digit start
    year like '26-27' (assumed 20xx), or a single trailing year ('BCB Trophy
    2027'). None if the name carries no season.
    """
    match = re.search(r"(\d{2,4})\s*[/–-]\s*\d{2,4}\s*$", name or "")
    if not match:
        match = re.search(r"(\d{4})\s*$", name or "")
    if not match:
        return None
    start = match.group(1)
    start_num = int(start)
    if len(start) == 2:
        start_num += 2000
    return start_num


def should_move_team_forward(team_competition_name: str, new_competition_name: str) -> bool:
    """
    A team found under another season of the same league is moved onto the
    competition being parsed, so the newest season's Teams tab lists it. Only
    do that when the game being parsed is from the same or a later season:
    parsing an old season (a history import) must not drag a current team
    back into 2018 and off this season's Teams tab. When either season can't
    be read from the name, keep the old behaviour and move it.
    """
    current = season_start_year(team_competition_name)
    new = season_start_year(new_competition_name)
    if current is None or new is None:
        return True
    return new >= current
