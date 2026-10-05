"""
Post-game reconciliation helpers.

Scorers keep correcting a LiveStats feed after the game has gone final (a
block credited to the wrong player, a missed rebound added). Once a game is
final and parsed the worker used to stop looking, so those corrections never
reached the site. These helpers let the worker keep re-checking a finished
game for a few days and re-parse only when the feed has actually changed.

Kept free of database and environment access so it can be unit tested.
"""

import hashlib
import json
from datetime import datetime, timedelta, timezone

RECONCILE_WINDOW = timedelta(hours=72)
RECONCILE_FAST_WINDOW = timedelta(hours=6)
RECONCILE_FAST_INTERVAL = timedelta(minutes=30)
RECONCILE_SLOW_INTERVAL = timedelta(hours=6)


def _parse_ts(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        ts = value
    else:
        try:
            ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def reconcile_next_poll(final_detected_at, now: datetime | None = None) -> str | None:
    """
    When to look at a finished game again, or None once the window has closed.
    Every 30 minutes for the first 6 hours after full time, then every 6 hours
    until 72 hours have passed.
    """
    now = now or datetime.now(timezone.utc)
    detected = _parse_ts(final_detected_at)
    if detected is None:
        return None
    age = now - detected
    if age >= RECONCILE_WINDOW:
        return None
    interval = RECONCILE_FAST_INTERVAL if age < RECONCILE_FAST_WINDOW else RECONCILE_SLOW_INTERVAL
    next_at = now + interval
    # Don't schedule past the end of the window; make the last look land on it.
    return min(next_at, detected + RECONCILE_WINDOW).isoformat()


def feed_fingerprint(data: dict) -> str:
    """
    Stable hash of everything we store from a LiveStats feed: every play-by-play
    action and every per-player and per-team stat total. Any scorer correction
    changes it; polling noise (clock, timestamps) does not.
    """
    pbp = [
        [
            e.get("actionNumber"),
            e.get("period"),
            e.get("gt"),
            e.get("actionType"),
            e.get("subType"),
            e.get("tno"),
            e.get("pno"),
            e.get("player"),
            e.get("success"),
            e.get("previousAction"),
        ]
        for e in (data.get("pbp") or [])
    ]
    pbp.sort(key=lambda row: (row[0] is None, row[0] or 0))

    teams = {}
    for tno, team in sorted((data.get("tm") or {}).items()):
        stat_keys = sorted(k for k in team if k.startswith("tot_s"))
        players = {
            pno: {k: p.get(k) for k in sorted(p) if k.startswith("s") or k in ("name", "shirtNumber")}
            for pno, p in sorted((team.get("pl") or {}).items())
        }
        teams[tno] = {"totals": {k: team[k] for k in stat_keys}, "players": players}

    payload = json.dumps({"pbp": pbp, "teams": teams}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
