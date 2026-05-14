#!/usr/bin/env python3
"""
backfill_lineup_context_existing.py
------------------------------------
Backfill lineup_stints_context_v1 for an existing league by processing one
game at a time to avoid timeouts on large datasets (10k+ stints).

Uses a tracking table (lineup_context_backfill_status) to record progress
and allow safe resumption after failures.

lineup_stints_context_v1 is assumed to already exist.  This script only
inserts rows into it and creates the four supporting indexes (IF NOT EXISTS).
It does NOT drop or recreate the table.

Usage examples:

  # Check progress for a league
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --status

  # Process the next 10 pending games (ordered by ascending stint count)
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --limit 10

  # Process a single specific game
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --game-key <KEY>

  # Reset failed games to pending, then process up to 25
  python -m app.scripts.backfill_lineup_context_existing --league-id <UUID> --limit 25 --reset-failed
"""

import argparse
import logging
import os
import sys
import textwrap

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("backfill_lineup_context")

# Prefer SUPABASE_DB_URL (direct Postgres URL for the Supabase project).
# Falls back to DATABASE_URL for local dev / CI.
# Set SUPABASE_DB_URL in Replit Secrets:
#   postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres
# (Supabase Dashboard → Settings → Database → Connection string → URI)
DATABASE_URL = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def get_conn():
    """Open a psycopg2 connection using SUPABASE_DB_URL or DATABASE_URL."""
    if not DATABASE_URL:
        log.error(
            "No database URL found. Set SUPABASE_DB_URL in Replit Secrets.\n"
            "  Supabase Dashboard → Settings → Database → Connection string → URI\n"
            "  Format: postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres"
        )
        sys.exit(1)
    return psycopg2.connect(DATABASE_URL)


# ---------------------------------------------------------------------------
# DDL: tracking table + indexes on context table
# (lineup_stints_context_v1 itself is NOT created here — it already exists)
# ---------------------------------------------------------------------------

DDL_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS lineup_context_backfill_status (
    game_key        text        NOT NULL,
    league_id       uuid        NOT NULL,
    status          text        NOT NULL DEFAULT 'pending',
    stint_count     integer     NOT NULL DEFAULT 0,
    inserted_rows   integer,
    error_msg       text,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (game_key, league_id)
);
"""

# Four indexes on lineup_stints_context_v1.
# 1. Per-game lookups and deletes.
# 2. League + team analytics.
# 3. Lineup-key aggregation queries.
# 4. Composite flag filter: lets the planner push all four filter predicates
#    into a single index scan (e.g. WHERE is_competitive_stint = true AND
#    is_valid_lineup = true AND is_garbage_time = false ...).
DDL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS lscv1_game_key_idx"
    "  ON lineup_stints_context_v1 (game_key);",

    "CREATE INDEX IF NOT EXISTS lscv1_league_team_idx"
    "  ON lineup_stints_context_v1 (league_id, team_id);",

    "CREATE INDEX IF NOT EXISTS lscv1_lineup_player_ids_gin_idx"
    "  ON lineup_stints_context_v1 USING GIN (lineup_player_ids);",

    "CREATE INDEX IF NOT EXISTS lscv1_flags_idx"
    "  ON lineup_stints_context_v1"
    "  (is_valid_lineup, is_garbage_time, is_short_clock_end_period, is_competitive_stint);",
]


def ensure_schema(conn):
    """
    Create the tracking table if absent, then create the four supporting
    indexes on lineup_stints_context_v1 (IF NOT EXISTS — safe to re-run).
    """
    with conn.cursor() as cur:
        cur.execute(DDL_TRACKING_TABLE)
        for idx_sql in DDL_INDEXES:
            cur.execute(idx_sql)
    conn.commit()
    log.info("Tracking table and indexes ensured.")


# ---------------------------------------------------------------------------
# Tracking table population (upsert pending / preserve complete)
# ---------------------------------------------------------------------------

UPSERT_TRACKING_SQL = """
INSERT INTO lineup_context_backfill_status (game_key, league_id, status, stint_count, updated_at)
SELECT
    ls.game_key,
    ls.league_id,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM lineup_stints_context_v1 ctx
            WHERE ctx.game_key  = ls.game_key
              AND ctx.league_id = ls.league_id
        ) THEN 'complete'
        ELSE 'pending'
    END AS status,
    COUNT(*)  AS stint_count,
    now()     AS updated_at
FROM lineup_stints ls
WHERE ls.league_id = %(league_id)s
GROUP BY ls.game_key, ls.league_id
ON CONFLICT (game_key, league_id) DO UPDATE
    SET stint_count = EXCLUDED.stint_count,
        updated_at  = now(),
        status      = CASE
                          WHEN lineup_context_backfill_status.status = 'complete'
                          THEN 'complete'
                          ELSE EXCLUDED.status
                      END;
"""


def populate_tracking(conn, league_id: str):
    """Upsert tracking rows for all games in the league."""
    with conn.cursor() as cur:
        cur.execute(UPSERT_TRACKING_SQL, {"league_id": league_id})
    conn.commit()
    log.info("Tracking table populated/refreshed for league %s.", league_id)


# ---------------------------------------------------------------------------
# --status mode
# ---------------------------------------------------------------------------

STATUS_SQL = """
SELECT
    status,
    COUNT(*)                        AS game_count,
    SUM(stint_count)                AS source_stints,
    SUM(COALESCE(inserted_rows, 0)) AS inserted_rows
FROM lineup_context_backfill_status
WHERE league_id = %(league_id)s
GROUP BY status
ORDER BY status;
"""


def print_status(conn, league_id: str):
    """Print a summary table of backfill progress for the league."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(STATUS_SQL, {"league_id": league_id})
        rows = cur.fetchall()

    if not rows:
        print(f"No tracking rows found for league {league_id}.")
        print("Run without --status first to initialise the tracking table.")
        return

    print()
    print(f"Backfill status — league {league_id}")
    print("-" * 60)
    fmt = "{:<12} {:>10} {:>15} {:>15}"
    print(fmt.format("Status", "Games", "Source Stints", "Inserted Rows"))
    print("-" * 60)
    for row in rows:
        print(fmt.format(
            row["status"],
            row["game_count"],
            row["source_stints"] or 0,
            row["inserted_rows"] or 0,
        ))
    print("-" * 60)
    print()


# ---------------------------------------------------------------------------
# --reset-failed mode
# ---------------------------------------------------------------------------

RESET_FAILED_SQL = """
UPDATE lineup_context_backfill_status
SET status     = 'pending',
    error_msg  = NULL,
    updated_at = now()
WHERE league_id = %(league_id)s
  AND status    = 'failed';
"""


def reset_failed(conn, league_id: str):
    """Reset all failed rows back to pending for the given league."""
    with conn.cursor() as cur:
        cur.execute(RESET_FAILED_SQL, {"league_id": league_id})
        count = cur.rowcount
    conn.commit()
    log.info("Reset %d failed game(s) back to pending for league %s.", count, league_id)


# ---------------------------------------------------------------------------
# Game selection
# ---------------------------------------------------------------------------

SELECT_PENDING_SQL = """
SELECT game_key, stint_count
FROM lineup_context_backfill_status
WHERE league_id = %(league_id)s
  AND status    = 'pending'
ORDER BY stint_count ASC
LIMIT %(limit)s;
"""

SELECT_ONE_GAME_SQL = """
SELECT game_key, stint_count
FROM lineup_context_backfill_status
WHERE league_id = %(league_id)s
  AND game_key  = %(game_key)s;
"""


def select_games(conn, league_id: str, game_key: str = None, limit: int = 10):
    """
    Return a list of (game_key, stint_count) tuples to process.

    If game_key is provided, return only that game (ignoring limit).
    Otherwise return up to `limit` pending games ordered by stint_count ASC.
    """
    with conn.cursor() as cur:
        if game_key:
            cur.execute(SELECT_ONE_GAME_SQL, {"league_id": league_id, "game_key": game_key})
        else:
            cur.execute(SELECT_PENDING_SQL, {"league_id": league_id, "limit": limit})
        return cur.fetchall()


# ---------------------------------------------------------------------------
# Per-game processing
# ---------------------------------------------------------------------------

MARK_RUNNING_SQL = """
UPDATE lineup_context_backfill_status
SET status     = 'running',
    updated_at = now()
WHERE league_id = %(league_id)s
  AND game_key  = %(game_key)s;
"""

MARK_COMPLETE_SQL = """
UPDATE lineup_context_backfill_status
SET status        = 'complete',
    inserted_rows = %(inserted_rows)s,
    error_msg     = NULL,
    updated_at    = now()
WHERE league_id = %(league_id)s
  AND game_key  = %(game_key)s;
"""

MARK_FAILED_SQL = """
UPDATE lineup_context_backfill_status
SET status     = 'failed',
    error_msg  = %(error_msg)s,
    updated_at = now()
WHERE league_id = %(league_id)s
  AND game_key  = %(game_key)s;
"""

DELETE_CONTEXT_ROWS_SQL = """
DELETE FROM lineup_stints_context_v1
WHERE game_key  = %(game_key)s
  AND league_id = %(league_id)s;
"""

# Full INSERT ... WITH CTE.
#
# CTE chain:
#   starters    — Starting-five player IDs per team from game_rosters.
#                 Joined into base so the query uses game_rosters data;
#                 starter_ids propagates through but is not projected into
#                 the INSERT column list.
#   base        — All lineup_stints rows for this game, LEFT JOIN starters.
#   with_margin — score_margin = cumulative (points_for - points_against)
#                 UP TO BUT NOT INCLUDING the current stint (the margin the
#                 lineup inherited at its start).  Uses
#                 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING.
#                 COALESCE handles the first stint (no preceding rows → 0).
#   flagged     — Derives is_garbage_time and is_short_clock_end_period.
#   final       — Computes is_competitive_stint and feeds the INSERT.
#
# Garbage-time (40-minute FIBA game):
#   Q4 only (period = 4). Three tiered bands by start_game_secs:
#     1800 – 1949 s : |margin| >= 25
#     1950 – 2099 s : |margin| >= 20
#     >= 2100 s      : |margin| >= 10
#   OT (period >= 5) is never garbage time.
#
# Short-clock-end-period:
#   Exact second cutoffs per regulation quarter:
#     Q1 end : start_game_secs >= 598
#     Q2 end : start_game_secs >= 1198
#     Q3 end : start_game_secs >= 1798
#   Q4 end is handled by the garbage-time flag; OT is excluded.

INSERT_CONTEXT_SQL = """
WITH starters AS (
    SELECT
        gr.game_key,
        gr.team_id,
        array_agg(gr.player_id::text ORDER BY gr.player_id::text)
            FILTER (WHERE gr.starter = true AND gr.player_id IS NOT NULL) AS starter_ids
    FROM game_rosters gr
    WHERE gr.game_key = %(game_key)s
    GROUP BY gr.game_key, gr.team_id
),
base AS (
    SELECT
        ls.id,
        ls.game_key,
        ls.league_id,
        ls.team_id,
        ls.lineup_key,
        ls.lineup_player_ids,
        ls.lineup_names,
        ls.period,
        ls.start_action,
        ls.end_action,
        ls.start_clock,
        ls.end_clock,
        ls.start_game_secs,
        ls.end_game_secs,
        ls.seconds_played,
        ls.points_for,
        ls.points_against,
        ls.fg2_made,
        ls.fg2_attempted,
        ls.fg3_made,
        ls.fg3_attempted,
        ls.ft_made,
        ls.ft_attempted,
        ls.oreb,
        ls.dreb,
        ls.assists,
        ls.turnovers,
        ls.fouls,
        ls.steals,
        ls.blocks,
        ls.possessions_for,
        ls.possessions_against,
        ls.is_valid_lineup,
        s.starter_ids
    FROM lineup_stints ls
    LEFT JOIN starters s
           ON s.game_key = ls.game_key
          AND s.team_id  = ls.team_id
    WHERE ls.game_key  = %(game_key)s
      AND ls.league_id = %(league_id)s
),
with_margin AS (
    SELECT
        b.*,
        -- score_margin: cumulative net points up to but NOT including this
        -- stint — i.e. the score the lineup walked on to the court with.
        COALESCE(
            SUM(b.points_for - b.points_against) OVER (
                PARTITION BY b.game_key, b.team_id
                ORDER BY b.start_game_secs, b.start_action
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
        ) AS score_margin
    FROM base b
),
flagged AS (
    SELECT
        wm.*,
        -- Garbage time: Q4 only (period = 4), three tiered bands.
        -- Band cutoffs use start_game_secs; thresholds decrease as time runs out.
        -- OT (period >= 5) is never flagged.
        CASE
            WHEN wm.period = 4
                 AND wm.start_game_secs >= 2100
                 AND ABS(wm.score_margin) >= 10 THEN true
            WHEN wm.period = 4
                 AND wm.start_game_secs >= 1950
                 AND ABS(wm.score_margin) >= 20 THEN true
            WHEN wm.period = 4
                 AND wm.start_game_secs >= 1800
                 AND ABS(wm.score_margin) >= 25 THEN true
            ELSE false
        END AS is_garbage_time,
        -- Short-clock end of period: Q1/Q2/Q3 only.
        -- Each quarter has an exact start_game_secs cutoff for its final ~2 s:
        --   Q1 end: start_game_secs >= 598  (2 s before 600)
        --   Q2 end: start_game_secs >= 1198 (2 s before 1200)
        --   Q3 end: start_game_secs >= 1798 (2 s before 1800)
        -- Q4 end is already covered by the garbage-time flag; OT is excluded.
        CASE
            WHEN wm.period = 1 AND wm.start_game_secs >= 598  THEN true
            WHEN wm.period = 2 AND wm.start_game_secs >= 1198 THEN true
            WHEN wm.period = 3 AND wm.start_game_secs >= 1798 THEN true
            ELSE false
        END AS is_short_clock_end_period
    FROM with_margin wm
)
INSERT INTO lineup_stints_context_v1 (
    stint_id,
    game_key,
    league_id,
    team_id,
    lineup_key,
    lineup_player_ids,
    lineup_names,
    period,
    start_action,
    end_action,
    start_clock,
    end_clock,
    start_game_secs,
    end_game_secs,
    seconds_played,
    points_for,
    points_against,
    fg2_made,
    fg2_attempted,
    fg3_made,
    fg3_attempted,
    ft_made,
    ft_attempted,
    oreb,
    dreb,
    assists,
    turnovers,
    fouls,
    steals,
    blocks,
    possessions_for,
    possessions_against,
    is_valid_lineup,
    score_margin,
    is_garbage_time,
    is_short_clock_end_period,
    is_competitive_stint
)
SELECT
    f.id                   AS stint_id,
    f.game_key,
    f.league_id,
    f.team_id,
    f.lineup_key,
    f.lineup_player_ids,
    f.lineup_names,
    f.period,
    f.start_action,
    f.end_action,
    f.start_clock,
    f.end_clock,
    f.start_game_secs,
    f.end_game_secs,
    f.seconds_played,
    f.points_for,
    f.points_against,
    f.fg2_made,
    f.fg2_attempted,
    f.fg3_made,
    f.fg3_attempted,
    f.ft_made,
    f.ft_attempted,
    f.oreb,
    f.dreb,
    f.assists,
    f.turnovers,
    f.fouls,
    f.steals,
    f.blocks,
    f.possessions_for,
    f.possessions_against,
    f.is_valid_lineup,
    f.score_margin,
    f.is_garbage_time,
    f.is_short_clock_end_period,
    (f.is_valid_lineup AND NOT f.is_garbage_time AND NOT f.is_short_clock_end_period)
        AS is_competitive_stint
FROM flagged f;
"""

COUNT_INSERTED_SQL = """
SELECT COUNT(*) AS n
FROM lineup_stints_context_v1
WHERE game_key  = %(game_key)s
  AND league_id = %(league_id)s;
"""


def process_game(conn, league_id: str, game_key: str) -> int:
    """
    Process a single game inside one transaction.

    Sequence (all within the same transaction):
      1. Mark running in tracking table.
      2. Delete existing context rows for this game (idempotent re-run).
      3. INSERT new context rows via the CTE query.
      4. Count inserted rows.
      5. Mark complete in tracking table.

    On any exception the transaction is rolled back, then a separate
    single-statement transaction marks the game as failed with the error.

    Returns the number of rows inserted.
    """
    params = {"league_id": league_id, "game_key": game_key}

    try:
        with conn.cursor() as cur:
            cur.execute(MARK_RUNNING_SQL, params)
            cur.execute(DELETE_CONTEXT_ROWS_SQL, params)
            cur.execute(INSERT_CONTEXT_SQL, params)
            cur.execute(COUNT_INSERTED_SQL, params)
            row = cur.fetchone()
            inserted = row[0] if row else 0
            cur.execute(MARK_COMPLETE_SQL, {**params, "inserted_rows": inserted})
        conn.commit()

        log.info("game=%s  inserted=%d context rows", game_key, inserted)
        return inserted

    except Exception as exc:
        conn.rollback()
        err_msg = str(exc)[:1000]
        log.error("game=%s  FAILED: %s", game_key, err_msg)
        try:
            with conn.cursor() as cur:
                cur.execute(MARK_FAILED_SQL, {**params, "error_msg": err_msg})
            conn.commit()
        except Exception as mark_exc:
            log.error("Could not mark game=%s as failed: %s", game_key, mark_exc)
            conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=textwrap.dedent("""\
            Backfill lineup_stints_context_v1 for a large league one game at a time.
            Uses a tracking table to allow safe resumption after failures.
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--league-id",
        dest="league_id",
        required=True,
        help="UUID of the league to process.",
    )
    parser.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=10,
        help="Number of pending games to process (default: 10). Ignored when --game-key is set.",
    )
    parser.add_argument(
        "--game-key",
        dest="game_key",
        default=None,
        help="Process exactly this one game key and exit.",
    )
    parser.add_argument(
        "--reset-failed",
        dest="reset_failed",
        action="store_true",
        default=False,
        help="Reset failed games back to pending before selecting games to process.",
    )
    parser.add_argument(
        "--status",
        dest="status",
        action="store_true",
        default=False,
        help="Print a progress summary table for the league and exit.",
    )

    args = parser.parse_args()
    league_id = args.league_id

    conn = get_conn()

    try:
        ensure_schema(conn)
        populate_tracking(conn, league_id)

        if args.status:
            print_status(conn, league_id)
            return

        if args.reset_failed:
            reset_failed(conn, league_id)

        games = select_games(
            conn,
            league_id=league_id,
            game_key=args.game_key,
            limit=args.limit,
        )

        if not games:
            log.info("No pending games found for league %s.", league_id)
            print_status(conn, league_id)
            return

        log.info("Selected %d game(s) to process.", len(games))

        processed = 0
        errors = 0

        for (gk, stint_count) in games:
            log.info("Processing game_key=%s  (stint_count=%d)", gk, stint_count)
            try:
                process_game(conn, league_id, gk)
                processed += 1
            except Exception:
                errors += 1

        print()
        print("=" * 60)
        print("Lineup context backfill batch complete")
        print(f"  League    : {league_id}")
        print(f"  Processed : {processed}")
        print(f"  Errors    : {errors}")
        print("=" * 60)
        print()
        print_status(conn, league_id)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
