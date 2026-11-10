"""
Bring an older LiveStats feed (2017-2019 era) into the shape the parser reads.

Those feeds carry the same play-by-play events as today's, but:
  - no event has an actionNumber, so play-by-play, shot chart and lineups were
    all skipped (they are keyed on it) and the final whistle was never seen;
  - shots have no actionNumber to tie them to their play-by-play event;
  - team minutes (tot_sMinutes) are 0, which breaks pace and ratings.

normalize_legacy_feed fills those in. A feed that already has action numbers
is returned untouched, so it is safe to call on every feed.

Kept free of database and environment access so it can be unit tested.
"""

FIELD_GOALS = ("2pt", "3pt")


def _is_legacy(pbp) -> bool:
    return bool(pbp) and all(e.get("actionNumber") in (None, "") for e in pbp)


def _chronological(pbp):
    """Events oldest first. Feeds list them newest first (game end at index 0)."""
    def period_of(e):
        try:
            return int(e.get("period") or 0)
        except (TypeError, ValueError):
            return 0
    if period_of(pbp[0]) > period_of(pbp[-1]):
        return list(reversed(pbp))
    if period_of(pbp[0]) < period_of(pbp[-1]):
        return list(pbp)
    # One period only: the game "start" event marks the oldest end.
    if pbp[-1].get("actionType") == "game" and pbp[-1].get("subType") == "start":
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


def normalize_legacy_feed(data):
    """Fill in what an older feed leaves out. Mutates and returns `data`."""
    if not isinstance(data, dict):
        return data
    pbp = data.get("pbp") or []
    if not _is_legacy(pbp):
        return data

    events = _chronological(pbp)
    for number, event in enumerate(events, start=1):
        event["actionNumber"] = number

    for team_no, team in (data.get("tm") or {}).items():
        if not isinstance(team, dict):
            continue
        _number_shots(str(team_no), team.get("shot") or [], events)
        _fill_team_minutes(team)

    return data
