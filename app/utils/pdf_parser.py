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


def _upsert(table: str, records: list, conflict_col: str) -> int:
    if not records:
        return 0
    db = _get_pdf_game_db()
    try:
        db.table(table).upsert(records, on_conflict=conflict_col).execute()
        print(f"✅ PDF: Upserted {len(records)} rows into test.{table}")
        return len(records)
    except Exception as e:
        print(f"❌ PDF: Upsert failed for test.{table}: {e}")
        return 0


def _insert_batch(table: str, records: list, chunk_size: int = 200) -> int:
    if not records:
        return 0
    db = _get_pdf_game_db()
    inserted = 0
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        try:
            db.table(table).insert(chunk).execute()
            inserted += len(chunk)
        except Exception as e:
            print(f"❌ PDF: Insert failed for test.{table} chunk {i}: {e}")
    print(f"✅ PDF: Inserted {inserted} rows into test.{table}")
    return inserted


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
                if c.startswith("Report Generated") or c.startswith("(") or "Crew Chief" in c:
                    continue
                if re.search(r"\d+\s+[–\-]\s+\d+", c):
                    continue
                if not re.search(r"\d", c):
                    meta["away_team_full"] = c
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
            "source_type": "pdf",
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
                "source_type": "pdf",
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
    Returns {event_count}.
    """
    game_key = meta["game_key"]
    home_abbr = meta.get("home_abbr", "HOME")
    away_abbr = meta.get("away_abbr", "AWAY")

    # First pass: collect all text, strip per-page headers, track periods
    all_lines = []
    starters_lines = []
    current_period = 1

    for page in pdf.pages:
        text = page.extract_text(layout=True) or ""
        skip_until_gametime = True

        for line in text.split("\n"):
            # Skip blank lines in header region
            if skip_until_gametime:
                if re.match(r"^\s*Game Time", line):
                    skip_until_gametime = False
                continue

            if _is_page_header_line(line):
                continue

            # Capture starters
            line_s = line.strip()
            if re.match(r"^(SOU|COP|HOME|AWAY|[A-Z]{2,5})\s+\d+", line_s):
                starters_lines.append(line_s)
                continue

            all_lines.append(line)

    player_map = _build_player_team_map(starters_lines)

    # Abbr → team_id
    abbr_to_team = {home_abbr: home_team_id, away_abbr: away_team_id}

    # Second pass: parse events
    events = []
    action_counter = 0
    current_period = 1
    pending_clock = None
    pending_lines = []

    def flush_pending():
        nonlocal pending_clock, pending_lines, action_counter
        if not pending_clock and not pending_lines:
            return

        clock = pending_clock
        desc = " ".join(l.strip() for l in pending_lines if l.strip())

        if not desc and not clock:
            pending_clock = None
            pending_lines = []
            return

        # Extract score
        score_m = _SCORE_RE.search(desc)
        score = f"{score_m.group(1)}-{score_m.group(2)}" if score_m else None

        # Clean description (remove score/diff tokens)
        desc_clean = re.sub(r"\b\d+[-–]\d+\b", "", desc)
        desc_clean = re.sub(r"\s{2,}", " ", desc_clean).strip()
        desc_clean = re.sub(r"^\s*-?\d+\s*", "", desc_clean).strip()

        # Find player surname in description (CAPS pattern)
        team_id = None
        player_id = None
        player_name_str = None
        pm = _PLAYER_IN_EVENT_RE.search(desc)
        if pm:
            surname = pm.group(2).upper()
            abbr = player_map.get(surname)
            if abbr:
                team_id = abbr_to_team.get(abbr)

        action_type = _action_type_from_desc(desc_clean)
        action_counter += 1

        if desc_clean or clock:
            events.append({
                "game_key": game_key,
                "league_id": league_id,
                "team_id": team_id,
                "player_id": player_id,
                "action_number": action_counter,
                "period": current_period,
                "clock": clock,
                "action_type": action_type,
                "description": desc_clean or desc,
                "score": score,
                "source_type": "pdf",
            })

        pending_clock = None
        pending_lines = []

    for line in all_lines:
        line_s = line.strip()
        if not line_s:
            continue

        # Quarter marker
        qm = _QUARTER_RE.match(line_s)
        if qm:
            flush_pending()
            current_period = int(qm.group(1))
            continue

        # Timestamp line
        cm = _CLOCK_RE.match(line)
        if cm:
            flush_pending()
            pending_clock = cm.group(1)
            rest = cm.group(2).strip()
            if rest:
                pending_lines.append(rest)
            continue

        # Continuation line
        if line_s:
            pending_lines.append(line_s)

    flush_pending()

    if not events:
        return {"event_count": 0}

    count = _insert_batch("live_events", events)
    return {"event_count": count}


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


def _parse_lineup(pdf, meta: dict, league_id: str) -> dict:
    """
    Parse Line Up Analysis PDF into lineup_stats records.
    """
    game_key = meta["game_key"]
    current_team_name = None
    current_team_id = None
    records = []
    HEADER_SENTINEL = {"Lineup", "Time", "Score"}

    for page in pdf.pages:
        text = page.extract_text() or ""
        lines = text.split("\n")
        in_header = True

        for line in lines:
            line_s = line.strip()
            if not line_s:
                continue

            # Skip per-page header block (first N lines before data)
            if in_header:
                words = set(line_s.split())
                if HEADER_SENTINEL.issubset(words):
                    in_header = False
                # Detect team section header before "Lineup Time..." column header
                team_m = re.match(r"^(.+?)\s*(?:\([A-Z]{2,5}\))?\s*$", line_s)
                if team_m and not re.search(r"\d", line_s) and len(line_s) > 5:
                    candidate = normalize_team_name(line_s)
                    if candidate and "Scoring" not in candidate and "Crew" not in candidate and "Q1" not in candidate:
                        current_team_name = candidate
                        try:
                            current_team_id = get_or_create_team(league_id, current_team_name)
                        except Exception as e:
                            log.warning("Could not resolve team '%s': %s", current_team_name, e)
                            current_team_id = None
                continue

            # Team section header (no digits, after header)
            if not re.search(r"\d", line_s):
                candidate = normalize_team_name(line_s)
                if candidate and len(candidate) > 5 and "Lineup" not in candidate:
                    current_team_name = candidate
                    try:
                        current_team_id = get_or_create_team(league_id, current_team_name)
                    except Exception:
                        current_team_id = None
                continue

            # Skip column header row
            words_set = set(line_s.split())
            if {"Lineup", "Time"}.issubset(words_set):
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
    """
    game_key = meta["game_key"]
    current_team_name = None
    current_team_id = None
    records = []

    DATA_HEADERS = {"Mins", "Score", "Points", "Diff", "Assists", "Rebounds", "Steals", "Turnovers"}

    for page in pdf.pages:
        text = page.extract_text() or ""
        in_section = False
        skip_header = False

        for line in text.split("\n"):
            line_s = line.strip()
            if not line_s:
                continue

            # Skip per-page report header lines
            if any(
                line_s.startswith(x)
                for x in ["WEABL", "Essex Sport Arena", "Game No.", "Game Duration",
                           "City of London", "Copleston", "Report Generated",
                           "Crew Chief", "Scoring by", "Q1 Q2", "(20-", "("]
            ):
                in_section = False
                continue

            # Team section header (no digits, reasonable length)
            if not re.search(r"\d", line_s) and len(line_s) > 5:
                candidate = normalize_team_name(line_s)
                if candidate and "Player" not in candidate and "Plus" not in candidate:
                    current_team_name = candidate
                    try:
                        current_team_id = get_or_create_team(league_id, current_team_name)
                    except Exception:
                        current_team_id = None
                    in_section = True
                    skip_header = True
                    continue

            if skip_header:
                # Skip the two header rows (Mins, Score... and On, Off...)
                words = set(line_s.split())
                if words & DATA_HEADERS:
                    continue
                if words & {"On", "Off", "No", "Name"}:
                    continue
                skip_header = False

            if not in_section:
                continue

            m = _PM_DATA_RE.match(line_s)
            if not m:
                continue

            jersey = m.group(1)
            player_name = m.group(2).strip()
            ident = f"{game_key}_{_short_hash(f'{current_team_id}{player_name}')}"

            player_id = None
            try:
                player_id = get_or_create_player(
                    player_name, current_team_id, jersey, current_team_name, league_id
                )
            except Exception as e:
                log.warning("Could not resolve player '%s': %s", player_name, e)

            records.append({
                "game_key": game_key,
                "league_id": league_id,
                "team_id": current_team_id,
                "player_id": player_id,
                "player_name": player_name,
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

    The lineup can wrap over multiple text lines; a stats line (matching
    _ROT_STATS_RE) always follows the complete lineup.
    """
    game_key = meta["game_key"]
    current_team_name = None
    current_team_id = None
    records = []
    lineup_buffer = []

    SKIP_WORDS = {
        "Quarter", "Time", "Lineup", "Score", "Reb", "Stl", "Tov", "Ass",
        "On", "Off", "Court", "Diff", "RebStlTovAss",
    }

    def flush_lineup():
        nonlocal lineup_buffer
        lineup_buffer = []

    for page in pdf.pages:
        text = page.extract_text() or ""

        for line in text.split("\n"):
            line_s = line.strip()
            if not line_s:
                continue

            # Skip per-page report header
            if any(
                line_s.startswith(x)
                for x in ["WEABL", "Essex Sport Arena", "Game No.", "Game Duration",
                           "City of London", "Copleston", "Report Generated",
                           "Crew Chief", "Scoring by", "Q1 Q2", "(20-", "("]
            ):
                flush_lineup()
                continue

            # Team section header
            if not re.search(r"\d", line_s) and len(line_s) > 5:
                words_set = set(line_s.split())
                if not (words_set & SKIP_WORDS):
                    candidate = normalize_team_name(line_s)
                    if candidate and len(candidate) > 5:
                        flush_lineup()
                        current_team_name = candidate
                        try:
                            current_team_id = get_or_create_team(league_id, current_team_name)
                        except Exception:
                            current_team_id = None
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

                if not lineup_str:
                    continue

                ident = f"{game_key}_{_short_hash(lineup_str + m.group(1) + m.group(2))}_{current_team_id or 'none'}"

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
