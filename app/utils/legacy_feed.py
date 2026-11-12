"""
Bring an older LiveStats feed (2015-2020 era) into the shape the parser reads.

Feeds up to 2018-19 carry the same play-by-play events as today's, but:
  - no event has an actionNumber, so play-by-play, shot chart and lineups were
    all skipped (they are keyed on it) and the final whistle was never seen;
  - shots have no actionNumber to tie them to their play-by-play event;
  - team minutes (tot_sMinutes) are 0, which breaks pace and ratings.

The oldest (2015-17) also give shots only "p" for the player and no team
number, and write the player as "14, A. Smith" with the shirt in front.

Every feed before 2020-21, numbered or not, has the game clock only as "gt"
("MM:SS"), never "clock". The parser and the lineup builder read "clock", so
without it each quarter's minutes all landed on its last lineup.

normalize_legacy_feed fills those in. Only missing fields are filled, so a
current feed comes back unchanged and it is safe to call on every feed.

Kept free of database and environment access so it can be unit tested.
"""

import re

FIELD_GOALS = ("2pt", "3pt")
_SHIRT_IN_NAME = re.compile(r"^\s*(\d{1,3})\s*,\s*(\S.*)$")


def _is_legacy(pbp) -> bool:
    return bool(pbp) and all(e.get("actionNumber") in (None, "") for e in pbp)


def _period_of(event) -> int:
    """Period counted through overtime: the feed restarts at 1 for OT1."""
    try:
        period = int(event.get("period") or 0)
    except (TypeError, ValueError):
        return 0
    if str(event.get("periodType") or "").upper() == "OVERTIME" and period <= 4:
        period += 4
    return period


def _points_of(event) -> int:
    try:
        return int(event.get("s1") or 0) + int(event.get("s2") or 0)
    except (TypeError, ValueError):
        return 0


def _chronological(pbp):
    """Events oldest first. Feeds list them newest first (game end at index 0)."""
    first, last = pbp[0], pbp[-1]
    if _period_of(first) != _period_of(last):
        return list(reversed(pbp)) if _period_of(first) > _period_of(last) else list(pbp)
    # One period only: the score only goes up, and the game "start" event
    # marks the oldest end.
    if _points_of(first) != _points_of(last):
        return list(reversed(pbp)) if _points_of(first) > _points_of(last) else list(pbp)
    if last.get("actionType") == "game" and last.get("subType") == "start":
        return list(reversed(pbp))
    return list(pbp)


def _seconds(clock) -> int:
    try:
        mins, secs = str(clock).split(":")[:2]
        return int(mins) * 60 + int(secs)
    except (TypeError, ValueError):
        return 0


def _same_shot(event, shot) -> bool:
    return (
        str(event.get("pno")) == str(shot.get("pno"))
        and str(event.get("period")) == str(shot.get("per"))
        and event.get("actionType") == shot.get("actionType")
        and int(event.get("success") or 0) == int(shot.get("r") or 0)
    )


def _number_shots(team_no: str, shots, events):
    """Give each shot the actionNumber of its field-goal event, in order."""
    fg_events = [
        e for e in events
        if e.get("actionType") in FIELD_GOALS and str(e.get("tno")) == team_no
    ]
    used = set()
    start = 0
    for shot in shots:
        if shot.get("actionNumber") not in (None, ""):
            continue
        # Shots are listed in game order, so look forward from the last match;
        # fall back to any unused matching event if the lists drift apart.
        match = next(
            (i for i in range(start, len(fg_events)) if i not in used and _same_shot(fg_events[i], shot)),
            None,
        )
        if match is None:
            match = next(
                (i for i in range(len(fg_events)) if i not in used and _same_shot(fg_events[i], shot)),
                None,
            )
        if match is None:
            continue
        used.add(match)
        start = match + 1
        shot["actionNumber"] = fg_events[match]["actionNumber"]


def _fill_team_minutes(team: dict):
    if _seconds(team.get("tot_sMinutes")) > 0:
        return
    total = sum(_seconds(p.get("sMinutes")) for p in (team.get("pl") or {}).values())
    if total > 0:
        team["tot_sMinutes"] = f"{total // 60}:{total % 60:02d}"


def _fill_clock(pbp):
    """Copy "gt" ("MM:SS") into "clock", in the current "MM:SS:cc" shape."""
    for event in pbp:
        if event.get("clock") in (None, "") and re.fullmatch(r"\d{1,2}:\d{2}", str(event.get("gt") or "")):
            event["clock"] = f"{event['gt']}:00"


def _split_shirt_from_name(item: dict):
    """'14, A. Smith' -> player 'A. Smith', shirtNumber '14' (if it has none)."""
    match = _SHIRT_IN_NAME.match(str(item.get("player") or ""))
    if not match:
        return
    item["player"] = match.group(2).strip()
    if item.get("shirtNumber") in (None, ""):
        item["shirtNumber"] = match.group(1)


def _fill_shot_ids(team_no: str, shots):
    """2015-17 shots name their player only as "p" and carry no team number."""
    for shot in shots:
        if shot.get("pno") in (None, "") and shot.get("p") not in (None, ""):
            shot["pno"] = shot["p"]
        if shot.get("tno") in (None, ""):
            shot["tno"] = int(team_no)
        _split_shirt_from_name(shot)


def _blank_shirt_to_none(item: dict):
    """Some old feeds give a player shirtNumber "" (players.shirtNumber is an
    integer, so the insert fails and takes the play-by-play with it)."""
    if isinstance(item, dict) and str(item.get("shirtNumber", "x")).strip() == "":
        item["shirtNumber"] = None


def normalize_legacy_feed(data):
    """Fill in what an older feed leaves out. Mutates and returns `data`."""
    if not isinstance(data, dict):
        return data
    pbp = data.get("pbp") or []
    _fill_clock(pbp)
    if not _is_legacy(pbp):
        return data

    for team in (data.get("tm") or {}).values():
        for player in ((team or {}).get("pl") or {}).values() if isinstance(team, dict) else ():
            _blank_shirt_to_none(player)

    for event in pbp:
        _split_shirt_from_name(event)
        _blank_shirt_to_none(event)

    events = _chronological(pbp)
    for number, event in enumerate(events, start=1):
        event["actionNumber"] = number

    for team_no, team in (data.get("tm") or {}).items():
        if str(team_no) not in ("1", "2") or not isinstance(team, dict):
            continue
        shots = team.get("shot") or []
        _fill_shot_ids(str(team_no), shots)
        _number_shots(str(team_no), shots, events)
        _fill_team_minutes(team)

    return data
