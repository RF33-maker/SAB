"""
pdf_parser.py
Genius Sports post-game PDF ingestion pipeline.

Supported types:
  - FIBA Box Score       → player_stats + team_stats (test schema)
  - Play by Play         → live_events (test schema)
  - Line Up Analysis     → lineup_stats (test schema)
  - Player Plus/Minus    → player_plus_minus (test schema)
  - Rotations Summary    → rotations_summary (test schema)
  - Shot Chart           → skip (image-based, no extractable data)
  - Shot Areas           → skip (image-based, no extractable data)

game_key format for PDFs: PDF_{game_no}  e.g. PDF_5193600
All data is written to the 'test' schema regardless of DB_SCHEMA env var.
"""

import os
import re
import json
import hashlib
import logging
from datetime import datetime

import pdfplumber
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

from app.utils.json_parser import (
    get_or_create_league,
    get_or_create_team,
    get_or_create_player,
    normalize_team_name,
)

log = logging.getLogger("pdf_parser")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

_pdf_game_db: Client = None
_pdf_ref_db: Client = None


def _get_pdf_game_db() -> Client:
    global _pdf_game_db
    if _pdf_game_db is None:
        _pdf_game_db = create_client(
            SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema="test")
        )
    return _pdf_game_db


def _get_pdf_ref_db() -> Client:
    global _pdf_ref_db
    if _pdf_ref_db is None:
        _pdf_ref_db = create_client(
            SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema="public")
        )
    return _pdf_ref_db


REPORT_TYPES = {
    "FIBA Box Score": "box_score",
    "Play by Play": "pbp",
    "Line Up Analysis": "lineup",
    "Player Plus/Minus": "plus_minus",
    "Rotations Summary": "rotations",
    "Shot Chart": "shot_chart",
    "Shot Areas": "shot_areas",
}

SKIP_TYPES = {"shot_chart", "shot_areas"}


def _strip_unknown_col(error_msg: str) -> str | None:
    """Extract the missing column name from a PGRST204 error message."""
    m = re.search(r"Could not find the '(\w+)' column", str(error_msg))
    return m.group(1) if m else None


def _drop_col(records: list, col: str) -> list:
    """Remove a key from every record dict."""
    return [{k: v for k, v in r.items() if k != col} for r in records]


def _upsert(table: str, records: list, conflict_col: str) -> int:
    """
    Upsert records into the test schema.
    - Auto-strips unknown columns (PGRST204) and retries.
    - Falls back to insert with ignore_duplicates if UNIQUE constraint missing (42P10).
    """
    if not records:
        return 0
    db = _get_pdf_game_db()
    max_retries = 20
    for attempt in range(max_retries):
        try:
            db.table(table).upsert(records, on_conflict=conflict_col).execute()
            log.info("PDF: Upserted %d rows into test.%s", len(records), table)
            return len(records)
        except Exception as e:
            err_str = str(e)
            if "PGRST204" in err_str:
                col = _strip_unknown_col(err_str)
                if col:
                    log.warning("PDF: test.%s missing column '%s' — stripping and retrying", table, col)
                    records = _drop_col(records, col)
                    continue
            if "42P10" in err_str:
                # No unique constraint on conflict column — fall back to insert ignore
                log.warning("PDF: test.%s has no UNIQUE constraint on '%s' — using insert ignore", table, conflict_col)
                db.table(table).insert(records, count="exact").execute()
                log.info("PDF: Inserted %d rows into test.%s (no-constraint fallback)", len(records), table)
                return len(records)
            raise
    raise RuntimeError(f"_upsert: too many retries for test.{table}")


def _insert_batch(table: str, records: list, chunk_size: int = 200) -> int:
    """
    Insert records in chunks into the test schema.
    Auto-strips any column the table doesn't have yet (PGRST204) and retries
    the entire batch from scratch with the offending column removed.
    """
    if not records:
        return 0
    db = _get_pdf_game_db()
    max_retries = 20

    for attempt in range(max_retries):
        try:
            inserted = 0
            for i in range(0, len(records), chunk_size):
                chunk = records[i : i + chunk_size]
                db.table(table).insert(chunk).execute()
                inserted += len(chunk)
            log.info("PDF: Inserted %d rows into test.%s", inserted, table)
            return inserted
        except Exception as e:
            col = _strip_unknown_col(str(e))
            if col and "PGRST204" in str(e):
                log.warning("PDF: test.%s missing column '%s' — stripping and retrying", table, col)
                records = _drop_col(records, col)
            else:
                raise
    raise RuntimeError(f"_insert_batch: too many retries for test.{table}")


def _short_hash(text: str, length: int = 8) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:length]


def _parse_ma(val: str):
    """Parse 'M/A' format. Returns (made, attempted) or (None, None)."""
    if not val or "/" not in str(val):
        return None, None
    try:
        parts = str(val).split("/")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        return None, None


def _safe_int(val):
    if val is None:
        return None
    try:
        return int(str(val).strip())
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(str(val).strip())
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Header Parsing
# ---------------------------------------------------------------------------

def _detect_report_type(first_page_text: str) -> str:
    """Return canonical report type key or 'unknown'."""
    for label, key in REPORT_TYPES.items():
        if label in first_page_text:
            return key
    return "unknown"


def _parse_header(first_page_text: str) -> dict:
    """
    Extract game metadata from the first page of any Genius Sports PDF.

    Returns dict with keys:
      competition, report_type, venue, game_date, start_time,
      game_no, game_key, game_duration,
      home_team_full, away_team_full,
      home_score, away_score, quarter_scores,
      home_abbr, away_abbr, officials
    """
    meta = {}

    lines = [l for l in first_page_text.split("\n") if l.strip()]

    meta["report_type"] = _detect_report_type(first_page_text)

    # Competition + report type (line 1)
    for rt_label in REPORT_TYPES:
        m = re.match(rf"^(.*?)\s{{2,}}{re.escape(rt_label)}", lines[0])
        if m:
            meta["competition"] = m.group(1).strip()
            break
    else:
        meta["competition"] = lines[0].split("  ")[0].strip()

    # Venue + date (line 2)
    m = re.search(
        r"^(.*?),\s+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(\d+\s+\w+\s+\d{4})\s+Start time:\s+(\d+:\d+)",
        lines[1] if len(lines) > 1 else "",
    )
    if m:
        meta["venue"] = m.group(1).strip()
        try:
            meta["game_date"] = datetime.strptime(m.group(2), "%d %b %Y").strftime(
                "%Y-%m-%d"
            )
        except ValueError:
            meta["game_date"] = None
        meta["start_time"] = m.group(3)
    else:
        meta["venue"] = None
        meta["game_date"] = None
        meta["start_time"] = None

    # Game No.
    m = re.search(r"Game No\.\s*:\s*(\d+)", first_page_text)
    meta["game_no"] = m.group(1) if m else None
    meta["game_key"] = f"PDF_{meta['game_no']}" if meta["game_no"] else None

    # Game Duration
    m = re.search(r"Game Duration\s*:\s*([\d:]+)", first_page_text)
    meta["game_duration"] = m.group(1) if m else None

    # Score line: "City ... WEABL 66 – 56" + possible "Report Generated: ..."
    # home team is on the first score line, away team on next line
    score_line = None
    for line in lines:
        m = re.search(r"(.+?)\s+(\d+)\s+[–\-]\s+(\d+)", line)
        if m and not score_line:
            score_line = m
            meta["home_team_full"] = m.group(1).strip()
            meta["home_score"] = _safe_int(m.group(2))
            meta["away_score"] = _safe_int(m.group(3))
            break

    # Away team: first non-empty line after the score line that contains no score/digits
    # (skip "Report Generated:" line which appears between them)
    if score_line:
        sl_idx = next(
            (i for i, l in enumerate(lines) if re.search(r"\d+\s+[–\-]\s+\d+", l)),
            None,
        )
        meta["away_team_full"] = None
        if sl_idx is not None:
            for candidate_line in lines[sl_idx + 1 :]:
                c = candidate_line.strip()
                if not c:
                    continue
                if c.startswith("(") or "Crew Chief" in c:
                    continue
                if re.search(r"\d+\s+[–\-]\s+\d+", c):
                    continue
                # Strip inline "Report Generated: ..." suffix before digit check
                c_clean = re.sub(r"\s+Report Generated:.*$", "", c).strip()
                if not c_clean or c_clean.startswith("Report Generated"):
                    continue
                if not re.search(r"\d", c_clean):
                    meta["away_team_full"] = c_clean
                    break
    else:
        meta["home_team_full"] = None
        meta["away_team_full"] = None
        meta["home_score"] = None
        meta["away_score"] = None

    # Quarter scores e.g. "(20-16, 8-17, 17-9, 21-14)"
    m = re.search(r"\((\d+-\d+(?:,\s*\d+-\d+)*)\)", first_page_text)
    if m:
        meta["quarter_scores"] = [s.strip() for s in m.group(1).split(",")]
    else:
        meta["quarter_scores"] = []

    # Officials: "Crew Chief: X Umpire(s): Y , Z"
    m = re.search(r"Crew Chief:\s*(.+?)(?:Umpire\(s\):\s*(.+))?$", first_page_text, re.MULTILINE)
    if m:
        meta["officials"] = {
            "crew_chief": m.group(1).strip().rstrip(",").strip(),
            "umpires": m.group(2).strip() if m.group(2) else None,
        }
    else:
        meta["officials"] = None

    # Team abbreviations from section headers e.g. "Team Name (SOU)"
    abbrs = re.findall(r"\(([A-Z]{2,5})\)", first_page_text)
    meta["home_abbr"] = abbrs[0] if len(abbrs) > 0 else "HOME"
    meta["away_abbr"] = abbrs[1] if len(abbrs) > 1 else "AWAY"

    return meta


# ---------------------------------------------------------------------------
# Box Score Parser
# ---------------------------------------------------------------------------

_BS_ROW_RE = re.compile(
    r"^(\*?)(\d+)\s+(.+?)\s+(\d{1,3}:\d{2})\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+(-?\d+)$"
)

_BS_TOTALS_RE = re.compile(
    r"^Totals\s+(\d{1,3}:\d{2})\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(\d+/\d+)\s+([\d.]+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+(-?\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+(-?\d+)$"
)

_BS_TEAM_RE = re.compile(r"^Team/Coach\s+(.*)")

_SECTION_HEADER_RE = re.compile(
    r"^(.+?)\s*\(([A-Z]{2,5})\)\s*(?:\u200b.*)?$"
)


def _parse_box_score_row(line: str):
    """
    Parse a player stat row from box score text.
    Returns dict or None.
    Columns: [*]Jersey Name Min FGM/A FG% 2PM/A 2P% 3PM/A 3P% FTM/A FT%
              OR DR TOT AS TO ST BS PF FD +/- EF PTS
    """
    line = line.strip()
    if not line:
        return None

    m = _BS_ROW_RE.match(line)
    if not m:
        return None

    starter = m.group(1) == "*"
    jersey = m.group(2)
    name = m.group(3).strip().rstrip(" (C)").strip()
    minutes = m.group(4)
    fgma = m.group(5)
    fgpct = _safe_float(m.group(6))
    tpma = m.group(7)
    tppct = _safe_float(m.group(8))
    thpma = m.group(9)
    thppct = _safe_float(m.group(10))
    ftma = m.group(11)
    ftpct = _safe_float(m.group(12))

    fgm, fga = _parse_ma(fgma)
    tpm, tpa = _parse_ma(tpma)
    thpm, thpa = _parse_ma(thpma)
    ftm, fta = _parse_ma(ftma)

    return {
        "starter": starter,
        "shirtnumber": jersey,
        "full_name": name,
        "sminutes": minutes,
        "sfieldgoalsmade": fgm,
        "sfieldgoalsattempted": fga,
        "sfieldgoalspercentage": fgpct,
        "stwopointersmade": tpm,
        "stwopointersattempted": tpa,
        "stwopointerspercentage": tppct,
        "sthreepointersmade": thpm,
        "sthreepointersattempted": thpa,
        "sthreepointerspercentage": thppct,
        "sfreethrowsmade": ftm,
        "sfreethrowsattempted": fta,
        "sfreethrowspercentage": ftpct,
        "sreboundsoffensive": _safe_int(m.group(13)),
        "sreboundsdefensive": _safe_int(m.group(14)),
        "sreboundstotal": _safe_int(m.group(15)),
        "sassists": _safe_int(m.group(16)),
        "sturnovers": _safe_int(m.group(17)),
        "ssteals": _safe_int(m.group(18)),
        "sblocks": _safe_int(m.group(19)),
        # BA (Blocks Against / sblocksreceived) is not present in Genius Sports
        # post-game PDF format — column does not appear in exported box scores.
        "sblocksreceived": None,
        "sfoulspersonal": _safe_int(m.group(20)),
        "sfoulson": _safe_int(m.group(21)),
        "splusminuspoints": _safe_int(m.group(22)),
        "eff_1": _safe_int(m.group(23)),
        "spoints": _safe_int(m.group(24)),
    }


def _parse_totals_row(line: str):
    """Parse the Totals row. Returns dict or None."""
    line = line.strip()
    m = _BS_TOTALS_RE.match(line)
    if not m:
        return None

    fgm, fga = _parse_ma(m.group(2))
    tpm, tpa = _parse_ma(m.group(4))
    thpm, thpa = _parse_ma(m.group(6))
    ftm, fta = _parse_ma(m.group(8))

    return {
        "tot_sminutes": m.group(1),
        "tot_sfieldgoalsmade": fgm,
        "tot_sfieldgoalsattempted": fga,
        "tot_sfieldgoalspercentage": _safe_float(m.group(3)),
        "tot_stwopointersmade": tpm,
        "tot_stwopointersattempted": tpa,
        "tot_stwopointerspercentage": _safe_float(m.group(5)),
        "tot_sthreepointersmade": thpm,
        "tot_sthreepointersattempted": thpa,
        "tot_sthreepointerspercentage": _safe_float(m.group(7)),
        "tot_sfreethrowsmade": ftm,
        "tot_sfreethrowsattempted": fta,
        "tot_sfreethrowspercentage": _safe_float(m.group(9)),
        "tot_sreboundsoffensive": _safe_int(m.group(10)),
        "tot_sreboundsdefensive": _safe_int(m.group(11)),
        "tot_sreboundstotal": _safe_int(m.group(12)),
        "tot_sassists": _safe_int(m.group(13)),
        "tot_sturnovers": _safe_int(m.group(14)),
        "tot_ssteals": _safe_int(m.group(15)),
        "tot_sblocks": _safe_int(m.group(16)),
        "tot_sfoulspersonal": _safe_int(m.group(17)),
        "tot_sfoulson": _safe_int(m.group(18)),
        "splusminuspoints": _safe_int(m.group(19)),
        "tot_eff_1": _safe_int(m.group(20)),
        "tot_spoints": _safe_int(m.group(21)),
    }


def _parse_footer_stats(text: str, home_abbr: str, away_abbr: str) -> tuple:
    """
    Parse the two-column footer stats block.
    Returns (home_extras, away_extras) dicts with tot_ keys.
    """
    home_e, away_e = {}, {}

    def _two_nums(pattern):
        m = re.search(pattern, text)
        if m:
            try:
                return int(m.group(1)), int(m.group(2))
            except (ValueError, IndexError):
                pass
        return None, None

    def _one_num(pattern):
        m = re.search(pattern, text)
        return _safe_int(m.group(1)) if m else None

    h, a = _two_nums(r"Points from Turnovers\s+(\d+)\s+(\d+)")
    if h is not None:
        home_e["tot_spointsfromturnovers"] = h
        away_e["tot_spointsfromturnovers"] = a

    m = re.search(r"Points in the Paint\s+(\d+)", text)
    m2 = re.search(r"Points in the Paint\s+\d+[^\d]+[\d.]+\s+(\d+)", text)
    if m:
        home_e["tot_spointsinthepaint"] = _safe_int(m.group(1))
    if m2:
        away_e["tot_spointsinthepaint"] = _safe_int(m2.group(1))

    h, a = _two_nums(r"Second Chance Points\s+(\d+)\s+(\d+)")
    if h is not None:
        home_e["tot_spointssecondchance"] = h
        away_e["tot_spointssecondchance"] = a

    m = re.search(r"Fast Break Points\s+(\d+)\s+(\d+)", text)
    if m:
        home_e["tot_spointsfastbreak"] = _safe_int(m.group(1))
        away_e["tot_spointsfastbreak"] = _safe_int(m.group(2))

    h, a = _two_nums(r"Bench Points\s+(\d+)\s+(\d+)")
    if h is not None:
        home_e["tot_sbenchpoints"] = h
        away_e["tot_sbenchpoints"] = a

    m = re.search(r"Biggest Lead\s+(\d+)[^\d]+(\d+)", text)
    if m:
        home_e["tot_sbiggestlead"] = m.group(1)
        away_e["tot_sbiggestlead"] = m.group(2)

    m = re.search(r"Biggest Scoring Run\s+(\d+)[^\d]+(\d+)", text)
    if m:
        home_e["tot_biggestscoringrun"] = m.group(1)
        away_e["tot_biggestscoringrun"] = m.group(2)

    n = _one_num(r"Lead Changes\s+(\d+)")
    if n is not None:
        home_e["tot_leadchanges"] = n
        away_e["tot_leadchanges"] = n

    n = _one_num(r"Times Tied\s+(\d+)")
    if n is not None:
        home_e["tot_timesscoreslevel"] = n
        away_e["tot_timesscoreslevel"] = n

    m = re.search(r"Time with Lead\s+([\d:]+)\s+([\d:]+)", text)
    if m:
        home_e["tot_timeleading"] = m.group(1)
        away_e["tot_timeleading"] = m.group(2)

    return home_e, away_e


def _parse_box_score(pdf, meta: dict, league_name: str, user_id: str) -> dict:
    """
    Parse FIBA Box Score PDF. Returns {player_count, team_count}.
    Writes to test.player_stats and test.team_stats.
    """
    game_key = meta["game_key"]
    league_id = get_or_create_league(league_name, user_id)
    ref_db = _get_pdf_ref_db()

    full_text = "\n".join(
        page.extract_text() or "" for page in pdf.pages
    )
    lines = full_text.split("\n")

    # We'll process the text to find two team sections
    # Section headers look like: "Team Name (ABR)" with optional zero-width space / assistant info
    # Player rows, Team/Coach row, Totals row follow

    team_sections = []
    current_section = None

    for line in lines:
        line_s = line.strip()
        if not line_s:
            continue

        # Section header detection: "SomeName (ABR)" pattern
        sh_m = re.match(r"^(.+?)\s*\(([A-Z]{2,5})\)\s*(?:\u200b.*)?$", line_s)
        if sh_m:
            raw_name = sh_m.group(1).strip()
            abbr = sh_m.group(2)
            # Must look like a team name (no stat-like content)
            if not re.search(r"\d{2}:\d{2}|\d+/\d+", raw_name):
                if current_section:
                    team_sections.append(current_section)
                current_section = {
                    "name": raw_name,
                    "abbr": abbr,
                    "players": [],
                    "totals": {},
                    "team_rebounds": {"or": 0, "dr": 0, "tot": 0},
                    "coach": None,
                }
                continue

        if current_section is None:
            continue

        # Coach line
        if line_s.startswith("Coach:"):
            current_section["coach"] = line_s.replace("Coach:", "").strip().split("  ")[0].strip()
            continue

        # DNP rows
        if line_s.endswith("DNP"):
            continue

        # Team/Coach rebounds row
        tc_m = _BS_TEAM_RE.match(line_s)
        if tc_m:
            parts = tc_m.group(1).strip().split()
            if len(parts) >= 3:
                current_section["team_rebounds"] = {
                    "or": _safe_int(parts[0]) or 0,
                    "dr": _safe_int(parts[1]) or 0,
                    "tot": _safe_int(parts[2]) or 0,
                }
            continue

        # Totals row
        if line_s.startswith("Totals "):
            totals = _parse_totals_row(line_s)
            if totals:
                current_section["totals"] = totals
            continue

        # Player row
        player_data = _parse_box_score_row(line_s)
        if player_data:
            current_section["players"].append(player_data)

    if current_section:
        team_sections.append(current_section)

    # Parse footer stats (below both team tables)
    # Footer appears after the last Totals row
    footer_idx = max(
        (i for i, l in enumerate(lines) if l.strip().startswith("Totals ")),
        default=len(lines) - 1,
    )
    footer_text = "\n".join(lines[footer_idx:])
    home_extras, away_extras = _parse_footer_stats(
        footer_text, meta.get("home_abbr", "HOME"), meta.get("away_abbr", "AWAY")
    )

    # Determine which section is home (side 1) and away (side 2)
    # First section = home, second = away (matches PDF order)
    home_abbr = meta.get("home_abbr", "")
    away_abbr = meta.get("away_abbr", "")

    def _get_side(section, idx):
        if section["abbr"] == home_abbr:
            return "1"
        if section["abbr"] == away_abbr:
            return "2"
        return str(idx + 1)

    player_records = []
    team_records = []
    extras_by_abbr = {
        home_abbr: home_extras,
        away_abbr: away_extras,
    }

    for idx, section in enumerate(team_sections):
        side = _get_side(section, idx)
        team_name = normalize_team_name(section["name"])
        team_id = get_or_create_team(league_id, team_name, user_id)

        # Team stats record
        team_rec = {
            "game_key": game_key,
            "league_id": league_id,
            "team_id": team_id,
            "name": team_name,
            "side": side,
            "score": meta.get("home_score") if side == "1" else meta.get("away_score"),
            "identifier_duplicate": f"{game_key}_{team_id}",
        }
        team_rec.update(section["totals"])
        # Merge team rebounds into totals
        tr = section["team_rebounds"]
        team_rec["tot_sreboundsteam"] = tr.get("tot", 0)
        team_rec["tot_sreboundsteamoffensive"] = tr.get("or", 0)
        team_rec["tot_sreboundsteamdefensive"] = tr.get("dr", 0)
        # Merge footer extras
        extras = extras_by_abbr.get(section["abbr"], {})
        team_rec.update(extras)

        # Quarter scores from meta
        qs = meta.get("quarter_scores", [])
        for qi, qs_val in enumerate(qs[:4], 1):
            parts = qs_val.split("-")
            if len(parts) == 2:
                if side == "1":
                    team_rec[f"p{qi}_score"] = _safe_int(parts[0])
                else:
                    team_rec[f"p{qi}_score"] = _safe_int(parts[1])

        if section.get("coach"):
            team_rec["coach"] = section["coach"]

        team_records.append(team_rec)

        # Player stats records
        for p in section["players"]:
            full_name = p["full_name"]
            player_id = get_or_create_player(
                full_name, team_id, p.get("shirtnumber"), team_name, league_id, user_id
            )

            p_rec = {
                "game_key": game_key,
                "league_id": league_id,
                "team_id": team_id,
                "player_id": player_id,
                "full_name": full_name,
                "team_name": team_name,
                "side": side,
                "identifier_duplicate": f"{game_key}_{player_id}",
            }
            for k, v in p.items():
                if k not in ("full_name",):
                    p_rec[k] = v

            player_records.append(p_rec)

    pc = _upsert("player_stats", player_records, "identifier_duplicate")
    tc = _upsert("team_stats", team_records, "identifier_duplicate")

    return {"player_count": pc, "team_count": tc}


# ---------------------------------------------------------------------------
# Play-by-Play Parser
# ---------------------------------------------------------------------------

_PBP_HEADER_SENTINEL = "Game Time"
_QUARTER_RE = re.compile(r"^Quarter\s+(\d+)$", re.IGNORECASE)
_CLOCK_RE = re.compile(r"^\s{0,6}(\d{2}:\d{2})\s*(.*)", re.DOTALL)
_STARTERS_RE = re.compile(
    r"^((?:[A-Z]{2,5}|SOU|COP))\s+(\d+\s+\S+\s+\S+(?:\s+\d+\s+\S+\s+\S+)*)$"
)
_PLAYER_IN_EVENT_RE = re.compile(r"(\d+)\s+([A-Z][A-Z\-]+)\s+[A-Z]")
_SCORE_RE = re.compile(r"\b(\d+)[-–](\d+)\b")


def _action_type_from_desc(desc: str) -> str:
    desc_l = desc.lower()
    if "2pt fg" in desc_l or "3pt fg" in desc_l:
        if "made" in desc_l:
            return "2pts" if "2pt" in desc_l else "3pts"
        return "shot"
    if "free throw" in desc_l:
        return "freethrow"
    if "rebound" in desc_l:
        return "rebound"
    if "turnover" in desc_l:
        return "turnover"
    if "steal" in desc_l:
        return "steal"
    if "foul" in desc_l:
        return "foul"
    if "assist" in desc_l:
        return "assist"
    if "block" in desc_l:
        return "block"
    if "substitution" in desc_l:
        return "sub"
    if "jumpball" in desc_l or "jump ball" in desc_l:
        return "jumpball"
    return "event"


def _build_player_team_map(all_starters_lines: list) -> dict:
    """
    Build SURNAME.upper() → team_abbr mapping from starters lines like:
    "SOU 5 Dioramma A 8 Habbal A ..."
    """
    player_map = {}
    for line in all_starters_lines:
        parts = line.strip().split()
        if not parts:
            continue
        abbr = parts[0]
        i = 1
        while i < len(parts):
            if parts[i].isdigit():
                i += 1
                if i < len(parts):
                    surname = parts[i].upper()
                    player_map[surname] = abbr
                    i += 1
                    if i < len(parts) and len(parts[i]) == 1 and parts[i].isalpha():
                        i += 1
            else:
                i += 1
    return player_map


def _is_page_header_line(line: str) -> bool:
    """True if the line is part of the repeated per-page header block."""
    line_s = line.strip()
    triggers = [
        "Scoring by 5 Minute intervals",
        "Quarter Starters:",
        "Crew Chief:",
        "Game Duration:",
        "Report Generated:",
        "Start time:",
        "Game No.:",
        "Q1 Q2 Q3 Q4",
    ]
    for t in triggers:
        if line_s.startswith(t) or t in line_s:
            return True
    # The column header "Game Time   SOU   Score   Diff   COP"
    if re.match(r"^\s*Game Time\s+\S+\s+Score\s+Diff\s+\S+", line_s):
        return True
    # Scoring intervals data line
    if re.match(r"^\s*(SOU|COP)\s+\d+\s+\d+", line_s) and "Scoring" not in line_s:
        return True
    return False


def _parse_pbp(pdf, meta: dict, league_id: str, home_team_id: str, away_team_id: str) -> dict:
    """
    Parse Play by Play PDF into live_events records.

    Layout (layout=True extraction):
      Col 0-6:   clock (MM:SS) — always in home column
      Col ~7-43: home-side action text
      Col ~44+:  away-side action text
    Score+diff appear inline in the same line as their clock (standalone score lines)
    or appended to an action line.

    Returns {event_count}.
    """
    game_key = meta["game_key"]
    home_abbr = meta.get("home_abbr", "HOME")
    away_abbr = meta.get("away_abbr", "AWAY")

    # Column boundary: chars 0..COL_SPLIT = home side, chars COL_SPLIT.. = away side
    COL_SPLIT = 44

    # Abbr → team_id
    abbr_to_team = {home_abbr: home_team_id, away_abbr: away_team_id}

    # Surname → abbr, built from "Quarter Starters:" blocks
    player_team_map: dict = {}

    # Collect per-period lines with side attribution
    class _RawLine:
        __slots__ = ("period", "clock", "side", "text")
        def __init__(self, period, clock, side, text):
            self.period = period
            self.clock = clock
            self.side = side        # "home" | "away" | "score" | None
            self.text = text

    raw_lines: list = []
    current_period = 1
    current_clock = None

    for page in pdf.pages:
        text_layout = page.extract_text(layout=True) or ""
        skip_header = True

        for raw_line in text_layout.split("\n"):
            # Skip until the column header "Game Time ..."
            if skip_header:
                if re.match(r"^\s*Game Time", raw_line):
                    skip_header = False
                continue

            if _is_page_header_line(raw_line):
                continue

            stripped = raw_line.strip()
            if not stripped:
                continue

            # Quarter starters line: "SOU 5 SURNAME F 8 SURNAME F ..."
            if re.match(r"^[A-Z]{2,5}\s+\d+\s+[A-Z]", stripped):
                _collect_starters(stripped, player_team_map)
                continue

            # Quarter header: "Quarter N"
            qm = _QUARTER_RE.match(stripped)
            if qm:
                current_period = int(qm.group(1))
                current_clock = None
                continue

            # Clock in first ~7 chars
            clock_m = re.match(r"^\s{0,6}(\d{2}:\d{2})", raw_line)
            if clock_m:
                current_clock = clock_m.group(1)

            home_text = raw_line[:COL_SPLIT].strip()
            away_text = raw_line[COL_SPLIT:].strip() if len(raw_line) > COL_SPLIT else ""

            # Determine which side has actual action content
            # Remove clock token from home_text for action detection
            home_action = re.sub(r"^\d{2}:\d{2}\s*", "", home_text).strip()
            # Score-only line: the home_action is just "SCORE DIFF" like "20-16 4"
            # or "4-7  -3", detected by absence of letters
            score_only = bool(home_action and not re.search(r"[A-Za-z]", home_action))

            if home_action and not score_only:
                raw_lines.append(_RawLine(current_period, current_clock, "home", home_action))
            if away_text:
                raw_lines.append(_RawLine(current_period, current_clock, "away", away_text))
            if score_only:
                raw_lines.append(_RawLine(current_period, current_clock, "score", home_action))

    # Build events from raw_lines — merge continuation lines (no clock) into previous
    events = []
    action_counter = 0

    # Merge lines with same (period, clock, side) into combined text
    merged: list = []  # list of (period, clock, side, text)
    for rl in raw_lines:
        if merged and merged[-1][2] == rl.side and merged[-1][1] == rl.clock and merged[-1][0] == rl.period:
            merged[-1] = (merged[-1][0], merged[-1][1], merged[-1][2], merged[-1][3] + " " + rl.text)
        else:
            merged.append([rl.period, rl.clock, rl.side, rl.text])

    # Track running score so standalone score lines are available to next action
    running_score = None
    running_diff = None

    for period, clock, side, text in merged:
        text = text.strip()
        if not text:
            continue

        if side == "score":
            sm = re.search(r"(\d+-\d+)\s+(-?\d+)", text)
            if sm:
                running_score = sm.group(1)
                running_diff = _safe_int(sm.group(2))
            continue

        # Extract inline score+diff from action line
        inline_score_m = re.search(r"(\d+-\d+)\s+(-?\d+)", text)
        if inline_score_m:
            running_score = inline_score_m.group(1)
            running_diff = _safe_int(inline_score_m.group(2))
            # Remove score/diff from description
            text = re.sub(r"\s+\d+-\d+\s+-?\d+", "", text).strip()

        # Determine team from side or player surname
        if side == "home":
            team_id = home_team_id
        elif side == "away":
            team_id = away_team_id
        else:
            team_id = None

        # Player resolution from description: "15 SURNAME I" pattern
        player_id = None
        pm = re.search(r"\b(\d{1,2})\s+([A-Z][A-Z\-ÑÉÀÜÖ]+(?:-[A-Z]+)?)\s+([A-Z])\b", text)
        if pm:
            jersey_num = pm.group(1)
            surname = pm.group(2)
            initial = pm.group(3)
            # Try player map for this team
            player_abbr = player_team_map.get(surname)
            if player_abbr and player_abbr in abbr_to_team:
                team_id = abbr_to_team[player_abbr]
            # Build full name guess: "SURNAME I" → "SURNAME I" (we don't have first name)
            player_guess = f"{surname} {initial}"
            try:
                t_id_for_player = team_id
                if t_id_for_player:
                    player_id = get_or_create_player(
                        player_guess, t_id_for_player, jersey_num, None, league_id
                    )
            except Exception as e:
                log.debug("PBP player lookup failed for '%s': %s", player_guess, e)

        action_type = _action_type_from_desc(text)
        action_counter += 1

        events.append({
            "game_key": game_key,
            "league_id": league_id,
            "team_id": team_id,
            "player_id": player_id,
            "action_number": action_counter,
            "period": period,
            "clock": clock,
            "action_type": action_type,
            "description": text,
            "score": running_score,
            "score_diff": running_diff,
        })

    if not events:
        return {"event_count": 0}

    count = _insert_batch("live_events", events)
    return {"event_count": count}


def _collect_starters(line: str, player_map: dict) -> None:
    """Parse 'SOU 5 SURNAME I 8 SURNAME2 I2 ...' and populate player_map."""
    parts = line.strip().split()
    if not parts:
        return
    abbr = parts[0]
    i = 1
    while i < len(parts):
        if parts[i].isdigit():
            i += 1
            if i < len(parts):
                surname = parts[i].upper()
                player_map[surname] = abbr
                i += 1
                # Skip first-name initial if present
                if i < len(parts) and len(parts[i]) == 1 and parts[i].isalpha():
                    i += 1
        else:
            i += 1


# ---------------------------------------------------------------------------
# Lineup Analysis Parser
# ---------------------------------------------------------------------------

_LINEUP_STATS_RE = re.compile(
    r"^(.*?)\s+(\d{1,2}:\d{2})\s+(\d+-\d+)\s+(-?\d+)\s+([\d.]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$"
)
_LINEUP_HEADER_WORDS = {
    "Lineup", "Time", "Score", "Diff", "Pts/Min", "Reb", "Stl", "Tov", "Ass"
}


def _normalize_lineup(raw: str) -> str:
    """Strip zero-width spaces and normalise spacing."""
    s = raw.replace("\u200b", "").strip()
    parts = [p.strip().rstrip("/").strip() for p in s.split("/") if p.strip().rstrip("/").strip()]
    return " / ".join(parts)


def _resolve_team_from_meta(meta: dict, league_id: str, user_id=None) -> dict:
    """
    Pre-resolve home and away team IDs from the PDF header metadata.
    Returns {"home": (name, team_id), "away": (name, team_id)}.
    """
    result = {}
    for side in ("home", "away"):
        raw = meta.get(f"{side}_team_full") or ""
        name = normalize_team_name(raw) if raw else None
        tid = get_or_create_team(league_id, name, user_id) if name else None
        result[side] = (name, tid)
    return result


def _is_known_team_header(line_s: str, known_names: list) -> str | None:
    """
    Check if a stripped line exactly matches one of the known team full names
    (after normalisation). Returns the matched name or None.
    """
    norm = normalize_team_name(line_s)
    for kn in known_names:
        if kn and norm and kn.lower() == norm.lower():
            return kn
    return None


def _parse_lineup(pdf, meta: dict, league_id: str) -> dict:
    """
    Parse Line Up Analysis PDF into lineup_stats records.
    Uses metadata-derived team names as anchors for section detection.
    """
    game_key = meta["game_key"]
    team_map = _resolve_team_from_meta(meta, league_id)
    home_name, home_tid = team_map["home"]
    away_name, away_tid = team_map["away"]
    known_names = [n for n in (home_name, away_name) if n]

    current_team_name = None
    current_team_id = None
    records = []

    PAGE_HEADER_TOKENS = {
        "WEABL", "Essex", "Game", "Report", "Crew", "Scoring", "Q1", "Q2", "Q3", "Q4",
    }

    for page in pdf.pages:
        text = page.extract_text() or ""
        in_page_header = True

        for line in text.split("\n"):
            line_s = line.strip()
            if not line_s:
                continue

            # Skip the per-page report header block
            if in_page_header:
                words_first = line_s.split()[0] if line_s.split() else ""
                # Column header signals end of per-page header
                if {"Lineup", "Time", "Score"}.issubset(set(line_s.split())):
                    in_page_header = False
                    continue
                # Detect a known team section header inside the header block too
                matched = _is_known_team_header(line_s, known_names)
                if matched:
                    name_norm = normalize_team_name(matched)
                    if name_norm == normalize_team_name(home_name or ""):
                        current_team_name, current_team_id = home_name, home_tid
                    else:
                        current_team_name, current_team_id = away_name, away_tid
                continue

            # Column header row — skip
            if {"Lineup", "Time"}.issubset(set(line_s.split())):
                continue

            # Team section header: must match a known team name exactly
            matched = _is_known_team_header(line_s, known_names)
            if matched:
                name_norm = normalize_team_name(matched)
                if name_norm == normalize_team_name(home_name or ""):
                    current_team_name, current_team_id = home_name, home_tid
                else:
                    current_team_name, current_team_id = away_name, away_tid
                continue

            # Lineup data row
            m = _LINEUP_STATS_RE.match(line_s)
            if m:
                raw_lineup = m.group(1)
                lineup_str = _normalize_lineup(raw_lineup)
                if not lineup_str:
                    continue

                ident = f"{game_key}_{_short_hash(lineup_str)}_{current_team_id or 'none'}"

                records.append({
                    "game_key": game_key,
                    "league_id": league_id,
                    "team_id": current_team_id,
                    "team_name": current_team_name,
                    "lineup": lineup_str,
                    "time_on_court": m.group(2),
                    "score": m.group(3),
                    "score_diff": _safe_int(m.group(4)),
                    "pts_per_min": _safe_float(m.group(5)),
                    "rebounds": _safe_int(m.group(6)),
                    "steals": _safe_int(m.group(7)),
                    "turnovers": _safe_int(m.group(8)),
                    "assists": _safe_int(m.group(9)),
                    "source_type": "pdf",
                    "identifier_duplicate": ident,
                })

    count = _upsert("lineup_stats", records, "identifier_duplicate")
    return {"lineup_count": count}


# ---------------------------------------------------------------------------
# Player Plus/Minus Parser
# ---------------------------------------------------------------------------

_PM_DATA_RE = re.compile(
    r"^(\d+)\s+(.+?)\s+(\d{2}:\d{2})\s+(\d{2}:\d{2})\s+"
    r"(\d+-\d+)\s+(\d+-\d+)\s+"
    r"(-?\d+)\s+(-?\d+)\s+"
    r"([\d.]+)\s+([\d.]+)\s+"
    r"(\d+)\s+(\d+)\s+"
    r"(\d+)\s+(\d+)\s+"
    r"(\d+)\s+(\d+)\s+"
    r"(\d+)\s+(\d+)$"
)


def _parse_plus_minus(pdf, meta: dict, league_id: str) -> dict:
    """
    Parse Player Plus/Minus Summary PDF into player_plus_minus records.
    Uses metadata-derived team names as anchors for section detection.
    Identifier: game_key + player_id for correct deduplication semantics.
    """
    game_key = meta["game_key"]
    team_map = _resolve_team_from_meta(meta, league_id)
    home_name, home_tid = team_map["home"]
    away_name, away_tid = team_map["away"]
    known_names = [n for n in (home_name, away_name) if n]

    current_team_name = None
    current_team_id = None
    records = []
    in_section = False
    skip_col_header = 0

    DATA_HEADER_WORDS = {"Mins", "Score", "Points", "Diff", "Assists", "Rebounds", "Steals", "Turnovers", "On", "Off", "No", "Name"}

    for page in pdf.pages:
        text = page.extract_text() or ""

        for line in text.split("\n"):
            line_s = line.strip()
            if not line_s:
                continue

            # Team section header: matched against known names from PDF header
            matched = _is_known_team_header(line_s, known_names)
            if matched:
                name_norm = normalize_team_name(matched)
                if name_norm == normalize_team_name(home_name or ""):
                    current_team_name, current_team_id = home_name, home_tid
                else:
                    current_team_name, current_team_id = away_name, away_tid
                in_section = True
                skip_col_header = 2  # skip the 2 column header lines that follow
                continue

            if skip_col_header > 0:
                # Skip "Mins Score Points Diff..." and "On Off On Off..." header rows
                if set(line_s.split()) & DATA_HEADER_WORDS:
                    skip_col_header -= 1
                    continue
                skip_col_header = 0

            if not in_section:
                continue

            m = _PM_DATA_RE.match(line_s)
            if not m:
                # If we hit a line that looks like a new team header or page header, reset
                if not re.search(r"\d", line_s) and len(line_s) > 3:
                    if set(line_s.split()) & DATA_HEADER_WORDS:
                        skip_col_header = 1
                continue

            jersey = m.group(1)
            player_name = m.group(2).strip()

            player_id = None
            try:
                player_id = get_or_create_player(
                    player_name, current_team_id, jersey, current_team_name, league_id
                )
            except Exception as e:
                log.warning("Could not resolve player '%s': %s", player_name, e)

            # Identifier: game_key + player_id (required uniqueness semantics)
            ident = f"{game_key}_{player_id or _short_hash(f'{current_team_id}{player_name}')}"

            records.append({
                "game_key": game_key,
                "league_id": league_id,
                "team_id": current_team_id,
                "player_id": player_id,
                "full_name": player_name,
                "shirt_number": jersey,
                "team_name": current_team_name,
                "mins_on": m.group(3),
                "mins_off": m.group(4),
                "score_on": m.group(5),
                "score_off": m.group(6),
                "pts_diff_on": _safe_int(m.group(7)),
                "pts_diff_off": _safe_int(m.group(8)),
                "pts_per_min_on": _safe_float(m.group(9)),
                "pts_per_min_off": _safe_float(m.group(10)),
                "assists_on": _safe_int(m.group(11)),
                "assists_off": _safe_int(m.group(12)),
                "rebounds_on": _safe_int(m.group(13)),
                "rebounds_off": _safe_int(m.group(14)),
                "steals_on": _safe_int(m.group(15)),
                "steals_off": _safe_int(m.group(16)),
                "turnovers_on": _safe_int(m.group(17)),
                "turnovers_off": _safe_int(m.group(18)),
                "source_type": "pdf",
                "identifier_duplicate": ident,
            })

    count = _upsert("player_plus_minus", records, "identifier_duplicate")
    return {"plus_minus_count": count}


# ---------------------------------------------------------------------------
# Rotations Summary Parser
# ---------------------------------------------------------------------------

_ROT_STATS_RE = re.compile(
    r"^(\d+)\s+(\d{2}:\d{2})\s+(\d+)\s+(\d{2}:\d{2})\s+(\d{2}:\d{2})\s+"
    r"(\d+-\d+)\s+(-?\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)$"
)


def _parse_rotations(pdf, meta: dict, league_id: str) -> dict:
    """
    Parse Rotations Summary PDF into rotations_summary records.
    Uses metadata-derived team names as anchors for section detection.
    The lineup can wrap over multiple text lines; a stats line (matching
    _ROT_STATS_RE) always follows the complete lineup.
    """
    game_key = meta["game_key"]
    team_map = _resolve_team_from_meta(meta, league_id)
    home_name, home_tid = team_map["home"]
    away_name, away_tid = team_map["away"]
    known_names = [n for n in (home_name, away_name) if n]

    current_team_name = None
    current_team_id = None
    records = []
    lineup_buffer = []

    COL_HEADER_WORDS = {"Quarter", "Time", "Lineup", "Score", "Reb", "Stl", "Tov", "Ass",
                        "On", "Off", "Court", "Diff", "RebStlTovAss"}

    def flush_lineup():
        nonlocal lineup_buffer
        lineup_buffer = []

    for page in pdf.pages:
        text = page.extract_text() or ""

        for line in text.split("\n"):
            line_s = line.strip()
            if not line_s:
                continue

            # Team section header: matched against known names from PDF header
            matched = _is_known_team_header(line_s, known_names)
            if matched:
                flush_lineup()
                name_norm = normalize_team_name(matched)
                if name_norm == normalize_team_name(home_name or ""):
                    current_team_name, current_team_id = home_name, home_tid
                else:
                    current_team_name, current_team_id = away_name, away_tid
                continue

            # Skip pure column header rows
            words_set = set(line_s.split())
            if words_set & {"Quarter", "Time", "Lineup", "RebStlTovAss"}:
                flush_lineup()
                continue

            # Stats line: starts with a digit (quarter number) + MM:SS + ...
            m = _ROT_STATS_RE.match(line_s)
            if m:
                raw_lineup = " ".join(lineup_buffer)
                lineup_str = _normalize_lineup(raw_lineup)
                flush_lineup()

                if not lineup_str or current_team_id is None:
                    continue

                ident = f"{game_key}_{_short_hash(lineup_str + m.group(1) + m.group(2))}_{current_team_id}"

                records.append({
                    "game_key": game_key,
                    "league_id": league_id,
                    "team_id": current_team_id,
                    "team_name": current_team_name,
                    "lineup": lineup_str,
                    "quarter_on": _safe_int(m.group(1)),
                    "time_on": m.group(2),
                    "quarter_off": _safe_int(m.group(3)),
                    "time_off": m.group(4),
                    "time_on_court": m.group(5),
                    "score": m.group(6),
                    "score_diff": _safe_int(m.group(7)),
                    "rebounds": _safe_int(m.group(8)),
                    "steals": _safe_int(m.group(9)),
                    "turnovers": _safe_int(m.group(10)),
                    "assists": _safe_int(m.group(11)),
                    "source_type": "pdf",
                    "identifier_duplicate": ident,
                })
                continue

            # Lineup text line (contains zero-width space or "/" separators)
            if "\u200b" in line_s or re.search(r"\d+-\s+\w+\s+\w+\s*/", line_s):
                lineup_buffer.append(line_s)
                continue

            # Short continuation of lineup (e.g. "Ensoll H/" or "Yebila E/")
            if current_team_id and lineup_buffer and re.match(r"^[A-Z][a-z\-]+\s+[A-Z]/?$", line_s):
                lineup_buffer.append(line_s)

    count = _upsert("rotations_summary", records, "identifier_duplicate")
    return {"rotation_count": count}


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

def parse_pdf(pdf_file, league_name: str, provided_game_key: str = None, user_id: str = None) -> dict:
    """
    Main entry point for PDF ingestion.

    Args:
        pdf_file:           File-like object (binary) or path string.
        league_name:        Used for entity resolution via get_or_create_league.
        provided_game_key:  Override game_key if already known (e.g. from prior upload).
        user_id:            Optional user UUID for created_by tracking.

    Returns:
        dict with keys: skipped, message, game_key, report_type, counts
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {"error": "Missing Supabase credentials"}

    try:
        with pdfplumber.open(pdf_file) as pdf:
            if not pdf.pages:
                return {"error": "PDF has no pages"}

            first_page_text = pdf.pages[0].extract_text() or ""
            report_type = _detect_report_type(first_page_text)

            # --- Skip types ---
            if report_type in SKIP_TYPES:
                return {
                    "skipped": True,
                    "message": f"Skipping '{report_type}' — embedded image, no extractable data",
                    "report_type": report_type,
                    "counts": {},
                }

            if report_type == "unknown":
                return {
                    "skipped": True,
                    "message": "Could not detect Genius Sports report type",
                    "report_type": "unknown",
                    "counts": {},
                }

            meta = _parse_header(first_page_text)

            # Override game_key if provided
            if provided_game_key:
                meta["game_key"] = provided_game_key

            if not meta.get("game_key"):
                return {"error": "Could not extract Game No. from PDF header"}

            game_key = meta["game_key"]

            # If no league_name provided (or generic fallback), derive from PDF header.
            # meta["competition"] is e.g. "WEABL 2025-26" — use it directly.
            if not league_name or league_name.lower() in ("unknown", ""):
                competition = meta.get("competition") or ""
                # Strip common report-type suffixes to get the base league name
                for suffix in [
                    " Play by Play", " Line Up Analysis", " Player Plus/Minus",
                    " Rotations Summary", " Shot Chart", " Shot Areas", " Box Score",
                ]:
                    competition = competition.replace(suffix, "")
                league_name = competition.strip() or "Unknown"

            print(f"🏀 PDF parse: type={report_type}, game_key={game_key}, league={league_name}")

            league_id = get_or_create_league(league_name, user_id)

            counts = {}

            if report_type == "box_score":
                counts = _parse_box_score(pdf, meta, league_name, user_id)

            elif report_type == "pbp":
                # Resolve teams for PBP
                home_team_name = normalize_team_name(meta.get("home_team_full") or "")
                away_team_name = normalize_team_name(meta.get("away_team_full") or "")
                home_team_id = get_or_create_team(league_id, home_team_name, user_id) if home_team_name else None
                away_team_id = get_or_create_team(league_id, away_team_name, user_id) if away_team_name else None
                counts = _parse_pbp(pdf, meta, league_id, home_team_id, away_team_id)

            elif report_type == "lineup":
                counts = _parse_lineup(pdf, meta, league_id)

            elif report_type == "plus_minus":
                counts = _parse_plus_minus(pdf, meta, league_id)

            elif report_type == "rotations":
                counts = _parse_rotations(pdf, meta, league_id)

            return {
                "skipped": False,
                "message": f"Parsed {report_type} for game {game_key}",
                "report_type": report_type,
                "game_key": game_key,
                "game_date": meta.get("game_date"),
                "competition": meta.get("competition"),
                "venue": meta.get("venue"),
                "home_team": meta.get("home_team_full"),
                "away_team": meta.get("away_team_full"),
                "home_score": meta.get("home_score"),
                "away_score": meta.get("away_score"),
                "counts": counts,
            }

    except Exception as e:
        log.error("PDF parse error: %s", e, exc_info=True)
        return {"error": str(e)}
