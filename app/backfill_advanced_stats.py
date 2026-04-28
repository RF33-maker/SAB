#!/usr/bin/env python3
"""
backfill_advanced_stats.py
--------------------------
Backfill advanced player & team stats for all (or a subset of) leagues.

Usage examples:
  # Process all leagues
  python -m app.backfill_advanced_stats

  # Single league
  python -m app.backfill_advanced_stats --league-id <uuid>

  # Reprocess even leagues that already have advanced stats
  python -m app.backfill_advanced_stats --force
"""

import argparse
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("backfill_advanced_stats")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")


def get_db():
    from supabase import create_client
    from supabase.lib.client_options import ClientOptions
    return create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
        options=ClientOptions(schema=DB_SCHEMA),
    )


_PAGE_SIZE = 1000


def fetch_league_ids(db, league_id: str = None) -> list:
    """
    Return distinct league_id values found in player_stats.
    Optionally restricted to a single league_id.

    Paginates through player_stats in chunks of _PAGE_SIZE to ensure
    all leagues are discovered even when the table has more rows than
    the PostgREST default limit.

    Propagates the underlying exception so callers can distinguish a
    genuine empty result from a DB/network failure.
    """
    seen = set()
    ids = []
    offset = 0

    while True:
        q = (
            db.table("player_stats")
            .select("league_id")
            .range(offset, offset + _PAGE_SIZE - 1)
        )
        if league_id:
            q = q.eq("league_id", league_id)

        res = q.execute()
        page = res.data or []

        for row in page:
            lid = row.get("league_id")
            if lid and lid not in seen:
                seen.add(lid)
                ids.append(lid)

        if len(page) < _PAGE_SIZE:
            break

        offset += _PAGE_SIZE

    return ids


def league_has_advanced_stats(db, league_id: str) -> bool:
    """
    Return True if at least one player_stats row in this league
    already has a non-null efg_percent (indicating advanced stats
    have already been computed).
    """
    try:
        res = (
            db.table("player_stats")
            .select("id")
            .eq("league_id", league_id)
            .not_.is_("efg_percent", "null")
            .limit(1)
            .execute()
        )
        return bool(res.data)
    except Exception as exc:
        log.warning("Could not check advanced stats for league %s: %s", league_id, exc)
        return False


def run_backfill(league_id: str = None, force: bool = False):
    """
    Main entry point for the backfill.

    Parameters
    ----------
    league_id : str   Restrict to a single league UUID.
    force     : bool  Re-run leagues that already have advanced stats.
    """
    from app.utils.compute_advanced_stats import compute_advanced_stats

    db = get_db()

    try:
        league_ids = fetch_league_ids(db, league_id=league_id)
    except Exception as exc:
        log.error("Failed to discover leagues from player_stats: %s", exc, exc_info=True)
        sys.exit(1)

    log.info("Found %d leagues to consider", len(league_ids))

    leagues_processed = 0
    leagues_skipped = 0
    errors = []

    for lid in league_ids:
        if not force and league_has_advanced_stats(db, lid):
            log.info(
                "league=%s already has advanced stats, skipping (use --force to reprocess)",
                lid,
            )
            leagues_skipped += 1
            continue

        log.info("Processing league=%s", lid)
        try:
            result = compute_advanced_stats(lid)
            status = result.get("status", "unknown")
            if status == "success":
                leagues_processed += 1
                log.info(
                    "league=%s done — teams=%s players=%s",
                    lid,
                    result.get("teams_processed"),
                    result.get("players_processed"),
                )
            else:
                leagues_skipped += 1
                err_msg = result.get("error") or f"status={status}"
                log.warning("league=%s finished with status=%s: %s", lid, status, err_msg)
                errors.append({"league_id": lid, "status": status, "error": err_msg})
        except Exception as exc:
            log.error("Failed to compute advanced stats for league=%s: %s", lid, exc, exc_info=True)
            leagues_skipped += 1
            errors.append({"league_id": lid, "status": "exception", "error": str(exc)})

    summary = {
        "leagues_processed": leagues_processed,
        "leagues_skipped": leagues_skipped,
        "errors": errors,
    }

    print("\n" + "=" * 60)
    print("Advanced stats backfill complete")
    print(f"  Leagues processed : {leagues_processed}")
    print(f"  Leagues skipped   : {leagues_skipped}")
    print(f"  Errors            : {len(errors)}")
    if errors:
        for err in errors:
            print(f"    - {err['league_id']}: {err['error']}")
    print("=" * 60)

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Backfill advanced player & team stats for existing leagues"
    )
    parser.add_argument(
        "--league-id",
        dest="league_id",
        default=None,
        help="Restrict backfill to this single league UUID",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Reprocess leagues even if advanced stats already exist",
    )
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        log.error("SUPABASE_URL and SUPABASE_KEY must be set in the environment")
        sys.exit(1)

    run_backfill(
        league_id=args.league_id,
        force=args.force,
    )


if __name__ == "__main__":
    main()
