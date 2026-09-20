import os
import logging
import requests
import pandas as pd
from io import BytesIO
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
from app.utils.compute_advanced_stats import compute_advanced_stats

log = logging.getLogger("json_parser")

# ----------------------------
# Type-safety helpers
# ----------------------------
def _safe_int(v):
    """Return int(v), or None for None / empty-string / non-numeric values."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

def _safe_float(v):
    """Return float(v), or None for None / empty-string / non-numeric values."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None

def _coerce_empty(v):
    """Convert empty strings to None; leave all other values untouched."""
    return None if v == "" else v

# ✅ Env variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment variables")

# Schema routing for test vs production
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

# Create schema-scoped clients
# game_db: for game data tables (game_schedule, team_stats, player_stats, live_events, shots)
# ref_db: for reference tables (leagues, teams, players) - always public
game_db: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema=DB_SCHEMA))
ref_db: Client = create_client(SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema="public"))

# Backward-compatible alias for storage and any legacy references
# Storage is not schema-specific, so ref_db works fine
supabase = ref_db

# ----------------------------
# Field Mappings
# ----------------------------
PLAYER_FIELD_MAP = {
    "sMinutes": "sminutes",
    "sFieldGoalsMade": "sfieldgoalsmade",
    "sFieldGoalsAttempted": "sfieldgoalsattempted",
    "sFieldGoalsPercentage": "sfieldgoalspercentage",
    "sThreePointersMade": "sthreepointersmade",
    "sThreePointersAttempted": "sthreepointersattempted",
    "sThreePointersPercentage": "sthreepointerspercentage",
    "sTwoPointersMade": "stwopointersmade",
    "sTwoPointersAttempted": "stwopointersattempted",
    "sTwoPointersPercentage": "stwopointerspercentage",
    "sFreeThrowsMade": "sfreethrowsmade",
    "sFreeThrowsAttempted": "sfreethrowsattempted",
    "sFreeThrowsPercentage": "sfreethrowspercentage",
    "sReboundsDefensive": "sreboundsdefensive",
    "sReboundsOffensive": "sreboundsoffensive",
    "sReboundsTotal": "sreboundstotal",
    "sAssists": "sassists",
    "sTurnovers": "sturnovers",
    "sSteals": "ssteals",
    "sBlocks": "sblocks",
    "sBlocksReceived": "sblocksreceived",
    "sFoulsPersonal": "sfoulspersonal",
    "sFoulsOn": "sfoulson",
    "sPoints": "spoints",
    "sPointsSecondChance": "spointssecondchance",
    "sPointsFastBreak": "spointsfastbreak",
    "sPlusMinusPoints": "splusminuspoints",
    "sPointsInThePaint": "spointsinthepaint",
    "eff_1": "eff_1",
    "eff_2": "eff_2",
    "eff_3": "eff_3",
    "eff_4": "eff_4",
    "eff_5": "eff_5",
    "eff_6": "eff_6",
    "eff_7": "eff_7",
    "firstName": "firstname",
    "familyName": "familyname",
    "shirtNumber": "shirtnumber",
    "playingPosition": "playingposition",
    "starter": "starter",
    "active": "active"
}

TEAM_FIELD_MAP = {
    "name": "name",
    "shortName": "shortname",
    "code": "code",
    "coach": "coach",
    "score": "score",
    "full_score": "full_score",  # must exist in schema
    "tot_sMinutes": "tot_sminutes",
    "tot_sFieldGoalsMade": "tot_sfieldgoalsmade",
    "tot_sFieldGoalsAttempted": "tot_sfieldgoalsattempted",
    "tot_sFieldGoalsPercentage": "tot_sfieldgoalspercentage",
    "tot_sThreePointersMade": "tot_sthreepointersmade",
    "tot_sThreePointersAttempted": "tot_sthreepointersattempted",
    "tot_sThreePointersPercentage": "tot_sthreepointerspercentage",
    "tot_sTwoPointersMade": "tot_stwopointersmade",
    "tot_sTwoPointersAttempted": "tot_stwopointersattempted",
    "tot_sTwoPointersPercentage": "tot_stwopointerspercentage",
    "tot_sFreeThrowsMade": "tot_sfreethrowsmade",
    "tot_sFreeThrowsAttempted": "tot_sfreethrowsattempted",
    "tot_sFreeThrowsPercentage": "tot_sfreethrowspercentage",
    "tot_sReboundsDefensive": "tot_sreboundsdefensive",
    "tot_sReboundsOffensive": "tot_sreboundsoffensive",
    "tot_sReboundsTotal": "tot_sreboundstotal",
    "tot_sAssists": "tot_sassists",
    "tot_sTurnovers": "tot_sturnovers",
    "tot_sSteals": "tot_ssteals",
    "tot_sBlocks": "tot_sblocks",
    "tot_sBlocksReceived": "tot_sblocksreceived",
    "tot_sFoulsPersonal": "tot_sfoulspersonal",
    "tot_sPoints": "tot_spoints",
    "tot_sPointsFromTurnovers": "tot_spointsfromturnovers",
    "tot_sPointsSecondChance": "tot_spointssecondchance",
    "tot_sPointsFastBreak": "tot_spointsfastbreak",
    "tot_sBenchPoints": "tot_sbenchpoints",
    "tot_sPointsInThePaint": "tot_spointsinthepaint",
    "tot_sTimeLeading": "tot_timeleading",
    "tot_sBiggestScoringRun": "tot_biggestscoringrun",
    "tot_sLeadChanges": "tot_leadchanges",
    "tot_sTimesScoresLevel": "tot_timesscoreslevel",
    "tot_sBiggestLead": "tot_sbiggestlead",
    "tot_sFoulsOn": "tot_sfoulson",
    "tot_sReboundsTeam": "tot_sreboundsteam",
    "tot_sReboundsTeamDefensive": "tot_sreboundsteamdefensive",
    "tot_sReboundsTeamOffensive": "tot_sreboundsteamoffensive",
    "tot_eff_1": "tot_eff_1",
    "p1_score": "p1_score",
    "p2_score": "p2_score",
    "p3_score": "p3_score",
    "p4_score": "p4_score"
}

SHOT_FIELD_MAP = {
    "r": "r",
    "x": "x",
    "y": "y",
    "p": "p",
    "pno": "pno",
    "tno": "tno",
    "per": "per",
    "pertype": "pertype",
    "actiontype": "actiontype",
    "actionnumber": "actionnumber",
    "subtype": "subtype",
    "player": "player",
    "shirtnumber": "shirtnumber"
}


PBP_FIELD_MAP = {
    "evt": "evt",
    "per": "per",
    "cl": "cl",
    "tm": "tm",
    "pid": "pid",
    "pn": "pn",
    "etype": "etype",
    "txt": "txt",
    "pts": "pts",
    "score": "score"
}


# ----------------------------
# Helpers
# ----------------------------
def build_data_url(numeric_id: str) -> str:
    return f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"

def _strip_col_from_error(error_msg: str):
    """Extract missing column name from a PGRST204 error, or None."""
    import re as _re
    m = _re.search(r"Could not find the '(\w+)' column", str(error_msg))
    return m.group(1) if m else None


def _drop_col(records: list, col: str) -> list:
    """Remove a key from every record dict."""
    return [{k: v for k, v in r.items() if k != col} for r in records]


def insert_supabase(table: str, records: list, conflict_keys: str):
    """Insert game data records using game_db (respects DB_SCHEMA).
    Auto-strips columns that Supabase reports as unknown (PGRST204) and retries,
    so schema drift never silently kills an entire upsert batch.
    """
    if not records:
        log.debug("insert_supabase: no records for %s — skipping", table)
        return
    for attempt in range(20):
        try:
            game_db.table(table) \
                .upsert(records, on_conflict=conflict_keys) \
                .execute()
            print(f"✅ Upserted {len(records)} into {DB_SCHEMA}.{table}")
            return
        except Exception as e:
            err_str = str(e)
            if "PGRST204" in err_str:
                col = _strip_col_from_error(err_str)
                if col:
                    log.warning("⚠️  %s.%s missing column '%s' — stripping and retrying",
                                DB_SCHEMA, table, col)
                    records = _drop_col(records, col)
                    continue
            log.error("❌ Supabase upsert FAILED for %s.%s (%d records): %s",
                      DB_SCHEMA, table, len(records), e, exc_info=True)
            raise
    raise RuntimeError(f"insert_supabase: too many retries for {DB_SCHEMA}.{table}")

# ----------------------------
# Team Name Normalization
# ----------------------------
TEAM_ALIASES = {
    "MK Breakers": "Milton Keynes Breakers",
    "MK Lions": "Milton Keynes Lions",
    "MKB": "Milton Keynes Breakers",
}

def normalize_team_name(name: str) -> str:
    if not name:
        return name
    
    import re
    normalized = name.strip()
    
    normalized = re.sub(r'\s+1$', '', normalized)
    normalized = re.sub(r'\s+I$', '', normalized)
    
    normalized = re.sub(r'\s*\([MmWw]\)\s*$', '', normalized)
    normalized = re.sub(r'\s*\(Men\)\s*$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s*\(Women\)\s*$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s*\(Male\)\s*$', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'\s*\(Female\)\s*$', '', normalized, flags=re.IGNORECASE)
    
    normalized = ' '.join(normalized.split())
    
    if normalized in TEAM_ALIASES:
        normalized = TEAM_ALIASES[normalized]
    
    return normalized

# ----------------------------
# Player Name Normalization & Fuzzy Matching
# ----------------------------
def normalize_player_name(name: str) -> str:
    if not name:
        return name
    
    import re
    normalized = name.strip()
    
    normalized = re.sub(r'\s+', ' ', normalized)
    
    return normalized

def find_similar_player(full_name: str, team_id: str, similarity_threshold: float = 0.85):
    from difflib import SequenceMatcher
    
    normalized_search = normalize_player_name(full_name)
    
    result = ref_db.table("players").select("id, full_name, shirtNumber").eq("team_id", team_id).execute()
    
    if not result.data:
        return None
    
    best_match = None
    best_score = 0.0
    match_type = None
    
    search_parts = normalized_search.split()
    
    for player in result.data:
        existing_name = normalize_player_name(player["full_name"])
        existing_parts = existing_name.split()
        
        if len(search_parts) >= 2 and len(existing_parts) >= 2:
            search_last = search_parts[-1].lower()
            existing_last = existing_parts[-1].lower()
            search_first = search_parts[0].lower()
            existing_first = existing_parts[0].lower()
            
            if search_last == existing_last:
                if (len(search_first) == 1 and existing_first.startswith(search_first)) or \
                   (len(existing_first) == 1 and search_first.startswith(existing_first)):
                    best_match = player
                    best_score = 1.0
                    match_type = "initial"
                    break
        
        similarity = SequenceMatcher(None, normalized_search.lower(), existing_name.lower()).ratio()
        
        if similarity > best_score and similarity >= similarity_threshold:
            best_score = similarity
            best_match = player
            match_type = "fuzzy"
    
    return best_match

# ----------------------------
# Entity Get-or-Create
# ----------------------------
def _slugify(text: str) -> str:
    """Convert a league name to a URL-safe slug, e.g. 'WEABL 2025-26' → 'weabl-2025-26'."""
    import re as _re
    slug = text.lower().strip()
    slug = _re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug or "league"


def _escape_ilike(value: str) -> str:
    """Escape %, _ and \\ so a name can be used safely inside an ilike pattern."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _find_league_id(name: str, slug: str, organisation: str = None, team_names: list = None):
    """
    Look up an existing league by, in order:
      1. Exact name match (fast path — matches how rows are normally created)
      2. Case-insensitive / trimmed name match (catches "Finals" vs "FINALS")
      3. Slug match (catches punctuation/whitespace differences that still
         normalize to the same slug)
      4. Fuzzy season match (catches "NBL Division One 26-27" matching an
         existing "NBL Division One 2026-27" — same league name and season
         year, just formatted differently — corroborated with organisation
         and team names where available; see _find_league_id_fuzzy_season)

    Returns the league_id if any strategy finds a row, else None.
    """
    res = ref_db.table("competitions").select("league_id").eq("name", name).execute()
    if res.data:
        return res.data[0]["league_id"]

    res = (
        ref_db.table("competitions")
        .select("league_id")
        .ilike("name", _escape_ilike(name.strip()))
        .limit(1)
        .execute()
    )
    if res.data:
        return res.data[0]["league_id"]

    res = ref_db.table("competitions").select("league_id").eq("slug", slug).limit(1).execute()
    if res.data:
        return res.data[0]["league_id"]

    return _find_league_id_fuzzy_season(name, organisation=organisation, team_names=team_names)


def get_or_create_league(name: str, user_id: str = None, organisation: str = None, team_names: list = None):
    name = name.strip()
    slug = _slugify(name)

    existing_id = _find_league_id(name, slug, organisation=organisation, team_names=team_names)
    if existing_id:
        return existing_id

    insert_data = {"name": name, "slug": slug}
    if user_id:
        insert_data["created_by"] = user_id
    if organisation:
        insert_data["organisation"] = organisation

    try:
        new = ref_db.table("competitions").insert(insert_data).execute()
        return new.data[0]["league_id"]
    except Exception as e:
        # Slug collision (23505) — slug already taken. Try appending a short suffix.
        err_str = str(e)
        if "23505" in err_str or "duplicate key" in err_str.lower():
            # Re-check first (race condition — another request created it
            # between our lookup above and this insert)
            existing_id = _find_league_id(name, slug, organisation=organisation, team_names=team_names)
            if existing_id:
                return existing_id
            # Try slug with numeric suffix
            import uuid as _uuid
            insert_data["slug"] = f"{slug}-{str(_uuid.uuid4())[:8]}"
            fallback = ref_db.table("competitions").insert(insert_data).execute()
            return fallback.data[0]["league_id"]
        raise

def _strip_season(name: str) -> str:
    """Strip a trailing season/year (e.g. '2026-27', '26-27', '2025/2026',
    '2026') from a competition name, so 'WNBL Division One 2026-27' and
    'WNBL Division One 2025-26' both normalize to 'WNBL Division One' for
    cross-season matching. Handles a 2-digit start year ('26-27') as well as
    4-digit, since names aren't always formatted consistently."""
    import re
    stripped = re.sub(r"\s*(?:\d{2,4}\s*[/–-]\s*\d{2,4}|\d{4})\s*$", "", name or "").strip()
    return stripped or (name or "").strip()


def _parse_season_start_year(name: str):
    """
    Extract a competition's season start year from its name, handling the
    formats actually used across leagues: '2026-27', '2026-2027', '2025/26',
    or a 2-digit start year like '26-27'. A 2-digit start year is assumed to
    be 20xx. Returns None if no trailing year/season pattern is found.
    """
    import re
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


def _find_league_id_fuzzy_season(name: str, exclude_league_id: str = None, organisation: str = None, team_names: list = None):
    """
    Last-resort match for a competition whose name represents the same
    league and season as `name`, just formatted differently — e.g. 'NBL
    Division One 26-27' vs an existing 'NBL Division One 2026-27'. Compares
    (base name, season start year) rather than the raw string, so it only
    matches when both the league name AND the year genuinely agree — two
    different seasons of the same league still get separate rows.

    Since a name+year match alone is still a heuristic, corroborate with
    whatever else is available before trusting it:
      - organisation (e.g. "Basketball England"), when both sides have one —
        a mismatch here rules the candidate out even if the name+year agree.
      - team_names — if we know the teams playing, an existing competition
        that already has at least one of them under it is strong evidence
        it's genuinely the same league, not a coincidental name collision.
    Both checks are skippable when the data simply isn't available (never
    treat "unknown" as a mismatch), but when a signal IS present on both
    sides, it must agree.
    """
    base_name = _strip_season(name).lower()
    start_year = _parse_season_start_year(name)
    if not base_name or start_year is None:
        return None

    res = ref_db.table("competitions").select("league_id,name,organisation").execute()
    candidates = [
        row for row in (res.data or [])
        if row["league_id"] != exclude_league_id
        and _strip_season(row["name"]).lower() == base_name
        and _parse_season_start_year(row["name"]) == start_year
    ]
    if not candidates:
        return None

    org_normalized = organisation.strip().lower() if organisation else None
    team_names_normalized = {
        normalize_team_name(t).lower() for t in (team_names or []) if t
    }

    for candidate in candidates:
        candidate_org = (candidate.get("organisation") or "").strip().lower() or None
        if org_normalized and candidate_org and candidate_org != org_normalized:
            continue  # organisation present on both sides and disagrees — reject

        if team_names_normalized:
            existing = ref_db.table("teams").select("name").eq("league_id", candidate["league_id"]).execute()
            existing_names = {normalize_team_name(t["name"]).lower() for t in (existing.data or [])}
            if existing_names and not (existing_names & team_names_normalized):
                continue  # this candidate has teams on record, none of which match — reject

        return candidate["league_id"]

    return None


def find_sibling_league_ids(league_id: str, league_name: str) -> list:
    """
    Find league_ids of other competitions that are the same league in a
    different season — matched by comparing competition names with the
    trailing season/year stripped (e.g. "WNBL Division One 2026-27" and
    "WNBL Division One 2025-26" both reduce to "WNBL Division One").

    Deliberately does NOT use competitions.competition_id for this — that
    field groups much more loosely (e.g. every REBA Summer League age-group
    and stop shares one competition_id despite having entirely separate team
    pools), so using it here would risk matching unrelated teams together.
    """
    base_name = _strip_season(league_name).lower()
    if not base_name:
        return []
    res = ref_db.table("competitions").select("league_id,name").execute()
    return [
        row["league_id"]
        for row in (res.data or [])
        if row["league_id"] != league_id and _strip_season(row["name"]).lower() == base_name
    ]


def get_or_create_team(league_id: str, name: str, user_id: str = None, sibling_league_ids: list = None):
    normalized_name = normalize_team_name(name)
    search_ids = [league_id] + [lid for lid in (sibling_league_ids or []) if lid != league_id]

    query = ref_db.table("teams").select("team_id,league_id,created_at").eq("name", normalized_name)
    query = query.in_("league_id", search_ids) if len(search_ids) > 1 else query.eq("league_id", search_ids[0])
    res = query.order("created_at").execute()

    if res.data:
        same_competition = next((r for r in res.data if r["league_id"] == league_id), None)
        if same_competition:
            return same_competition["team_id"]
        # Found under a sibling season — reuse the same team_id and move it
        # forward to this competition, so the current season's Teams tab
        # (which filters teams by league_id) picks it up. Historical
        # game/stat rows for the old season are unaffected — they carry
        # their own team name and this same stable team_id.
        matched = res.data[0]
        ref_db.table("teams").update({"league_id": league_id}).eq("team_id", matched["team_id"]).execute()
        return matched["team_id"]

    new = ref_db.table("teams").insert({"league_id": league_id, "name": normalized_name}).execute()
    return new.data[0]["team_id"]

def backfill_orphaned_schedule_rows(limit: int = 1000) -> int:
    """
    Resolves league_id/home_team_id/away_team_id for game_schedule rows that
    never went through parse_and_store_game — e.g. a fixture list imported
    straight into Supabase (a raw CSV import via the table editor, or any
    other bulk insert that bypasses /api/parse) rather than uploaded through
    the normal ingestion path. Without this, such rows sit with no
    league_id/team_ids until each individual game is polled to completion,
    which defeats pre-season population (teams/branding shown before a
    season's games have been played).

    This only LINKS rows to a competition that already exists (matched with
    the same _find_league_id lookup an upload uses) — it never creates one. A
    schedule for a brand-new competition stays untouched until the competition
    is created (e.g. via League Management, or by its first live game), since
    guessing and creating a league here is how near-miss names end up as
    duplicate leagues with fresh, unlinked teams. Teams are resolved with
    get_or_create_team plus the previous season's siblings, so returning
    teams keep their team_id.
    """
    res = (
        ref_db.table("game_schedule")
        .select("game_key,competitionname,hometeam,awayteam,organisation")
        .is_("league_id", "null")
        .limit(limit)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return 0

    league_cache: dict = {}
    sibling_cache: dict = {}
    updated = 0

    for row in rows:
        name = row.get("competitionname")
        if not name:
            continue
        team_names = [n for n in (row.get("hometeam"), row.get("awayteam")) if n]

        if name not in league_cache:
            league_cache[name] = _find_league_id(
                name.strip(), _slugify(name.strip()),
                organisation=row.get("organisation"), team_names=team_names,
            )
        league_id = league_cache[name]
        if not league_id:
            continue

        if league_id not in sibling_cache:
            sibling_cache[league_id] = find_sibling_league_ids(league_id, name)
        sibling_league_ids = sibling_cache[league_id]

        update_data = {"league_id": league_id}
        if row.get("hometeam"):
            update_data["home_team_id"] = get_or_create_team(
                league_id, row["hometeam"], sibling_league_ids=sibling_league_ids
            )
        if row.get("awayteam"):
            update_data["away_team_id"] = get_or_create_team(
                league_id, row["awayteam"], sibling_league_ids=sibling_league_ids
            )

        ref_db.table("game_schedule").update(update_data).eq("game_key", row["game_key"]).execute()
        updated += 1

    return updated

def _find_player_by_alias(full_name: str, team_id: str):
    """
    Match a feed name against players.aliases on the same team (case-insensitive).
    Aliases are the spellings a scorer keeps entering for a player whose name has
    been corrected or chosen by the player — e.g. "Benedict Baker-Mccann" for
    "Ben Baker", or "Manning Baumgardner III" for "Tre Baumgardner III" — so the
    feed's version resolves to the curated player row (and its photo) instead of
    creating a fresh one every game.
    """
    wanted = normalize_player_name(full_name).lower()
    if not wanted:
        return None
    res = ref_db.table("players").select("id, team_name, league_id, aliases").eq("team_id", team_id).not_.is_("aliases", "null").execute()
    for row in res.data or []:
        if any(normalize_player_name(a).lower() == wanted for a in (row.get("aliases") or []) if a):
            return row
    return None


def _is_initial_name(name: str) -> bool:
    """True for placeholder names like "S. Walker" that are less complete than a full name."""
    first = (name or "").split(" ")[0].replace(".", "")
    return len(first) <= 1


def _preferred_player_names(player_ids) -> dict:
    """
    players.full_name for each id: the name we hold for the player (their preferred name,
    or the one an admin corrected). Initial-only placeholders are skipped so a roster stub
    can never replace a fuller name typed for the game.
    """
    ids = sorted({pid for pid in player_ids if pid})
    names = {}
    for start in range(0, len(ids), 100):
        rows = ref_db.table("players").select("id, full_name").in_("id", ids[start:start + 100]).execute().data or []
        for row in rows:
            name = (row.get("full_name") or "").strip()
            if name and not _is_initial_name(name):
                names[row["id"]] = name
    return names


def get_or_create_player(full_name: str, team_id: str, shirtnumber=None, team_name=None, league_id=None, user_id: str = None):
    query = ref_db.table("players").select("id, team_name, league_id").eq("full_name", full_name).eq("team_id", team_id)
    if shirtnumber is not None:
        query = query.eq("shirtNumber", shirtnumber)
    res = query.execute()

    if not res.data:
        aliased = _find_player_by_alias(full_name, team_id)
        if aliased:
            res.data = [aliased]

    if res.data:
        player_id = res.data[0]["id"]
        existing_team_name = res.data[0].get("team_name")
        existing_league_id = res.data[0].get("league_id")
        
        update_data = {}
        if not existing_team_name and team_name:
            update_data["team_name"] = team_name
        if not existing_league_id and league_id:
            update_data["league_id"] = league_id
        
        if update_data:
            ref_db.table("players").update(update_data).eq("id", player_id).execute()
            print(f"✅ Updated player {full_name} with missing fields: {list(update_data.keys())}")
        
        return player_id
    
    similar_player = find_similar_player(full_name, team_id)
    if similar_player:
        player_id = similar_player["id"]
        
        update_data = {}
        existing_result = ref_db.table("players").select("team_name, league_id").eq("id", player_id).execute()
        if existing_result.data:
            existing_team_name = existing_result.data[0].get("team_name")
            existing_league_id = existing_result.data[0].get("league_id")
            
            if not existing_team_name and team_name:
                update_data["team_name"] = team_name
            if not existing_league_id and league_id:
                update_data["league_id"] = league_id
            
            if update_data:
                ref_db.table("players").update(update_data).eq("id", player_id).execute()
        
        return player_id
    
    insert_data = {
        "full_name": full_name,
        "team_id": team_id,
        "shirtNumber": shirtnumber
    }
    if team_name:
        insert_data["team_name"] = team_name
    if league_id:
        insert_data["league_id"] = league_id
    
    new = ref_db.table("players").insert(insert_data).execute()
    return new.data[0]["id"]

# ----------------------------
# Game Parser
# ----------------------------

def parse_and_store_game(numeric_id: str, league_name: str, game_date=None, home_team_name=None, away_team_name=None, game_key=None, livestats_url=None, user_id: str = None, pool=None, league_id: str = None, organisation: str = None):
    print(f"🔍 Processing game {numeric_id}")

    # --- Ensure league ---
    # If the caller already knows the league_id (e.g. the game was
    # pre-populated in game_schedule with one), use it directly instead of
    # re-resolving by name — this is the authoritative case, no lookup needed.
    if not league_id:
        league_id = get_or_create_league(
            league_name, user_id,
            organisation=organisation,
            team_names=[home_team_name, away_team_name],
        )

    # Backfill organisation (e.g. "Basketball England") if we have one and
    # the competition doesn't yet — display metadata only, never used for
    # team matching (see find_sibling_league_ids).
    if organisation:
        try:
            existing = ref_db.table("competitions").select("organisation").eq("league_id", league_id).maybe_single().execute()
            if existing.data and not existing.data.get("organisation"):
                ref_db.table("competitions").update({"organisation": organisation}).eq("league_id", league_id).execute()
        except Exception as _org_exc:
            print(f"⚠️  Could not backfill organisation for league {league_id}: {_org_exc}")

    # Teams belonging to this same league in a different season share one
    # stable team_id — found by comparing competition names with the season
    # stripped (see find_sibling_league_ids for why competition_id isn't used).
    sibling_league_ids = find_sibling_league_ids(league_id, league_name)

    # --- Ensure teams ---
    if home_team_name:
        home_team_id = get_or_create_team(league_id, home_team_name, user_id, sibling_league_ids)
    else:
        home_team_id = None
    if away_team_name:
        away_team_id = get_or_create_team(league_id, away_team_name, user_id, sibling_league_ids)
    else:
        away_team_id = None

    # --- Insert game schedule row (ALWAYS, even if stats unavailable) ---
    game_record = {
        "competitionname": league_name,
        "matchtime": game_date,
        "hometeam": home_team_name,
        "awayteam": away_team_name,
        "game_key": game_key,
        "LiveStats URL": livestats_url,
        "league_id": league_id,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id
    }
    # Add pool if present (for leagues with pools like NBL Division 1)
    if pool is not None:
        game_record["pool"] = pool
    if organisation:
        game_record["organisation"] = organisation
    game_db.table("game_schedule").upsert(game_record, on_conflict="game_key").execute()
    print(f"✅ Game schedule entry created for {game_key}")

    # --- Try to fetch LiveStats data ---
    url = build_data_url(numeric_id)
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            print(f"⏭️  No stats available yet (HTTP {r.status_code}) - game added to schedule")
            return
        data = r.json()
    except Exception as e:
        print(f"⏭️  No stats available yet ({e}) - game added to schedule")
        return

    teams = data.get("tm", {})

    # --- Update game_schedule with attendance and officials if present ---
    _sched_extra = {}
    _attendance = data.get("attendance")
    _officials = data.get("officials")
    if _attendance is not None:
        try:
            _sched_extra["attendance"] = int(_attendance)
        except (TypeError, ValueError):
            pass
    if _officials:
        _sched_extra["officials"] = _officials
    if _sched_extra:
        try:
            game_db.table("game_schedule").update(_sched_extra).eq("game_key", game_key).execute()
        except Exception as _e:
            print(f"⚠️  Could not update game_schedule with attendance/officials: {_e}")

    # --- Insert team stats ---
    team_records = []
    for side, team in teams.items():
        team_id = get_or_create_team(league_id, team.get("name"), user_id, sibling_league_ids)

        team_record = {
            "numeric_id": numeric_id,
            "side": side,
            "game_key": game_key,
            "team_id": team_id,
            "league_id": league_id,
            "source_type": "json",
            "identifier_duplicate": f"{numeric_id}_{team_id}_{side}"
        }
        for json_key, db_key in TEAM_FIELD_MAP.items():
            team_record[db_key] = _coerce_empty(team.get(json_key))
        lds = team.get("lds")
        if lds:
            team_record["game_leaders_json"] = lds
        team_records.append(team_record)

    insert_supabase("team_stats", team_records, conflict_keys="identifier_duplicate")

    # --- Insert player stats (build roster_map for shot linking) ---
    preferred_names = {}
    player_records = []
    roster_map = {}  # (side, pno_int) -> player_id
    try:
        for side, team in teams.items():
            team_id = get_or_create_team(league_id, team.get("name"), user_id, sibling_league_ids)
            team_name = team.get("name")
            for pid, player in team.get("pl", {}).items():
                try:
                    full_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
                    player_id = get_or_create_player(full_name, team_id, player.get("shirtNumber"), team_name, league_id, user_id)

                    # Build roster_map for shot linking: pno from roster is the dict key (pid)
                    try:
                        roster_map[(side, int(pid))] = player_id
                    except (ValueError, TypeError):
                        pass

                    player_record = {
                        "numeric_id": numeric_id,
                        "side": side,
                        "game_key": game_key,
                        "team_id": team_id,
                        "player_id": player_id,
                        "full_name": full_name,
                        "team_name": team.get("name"),
                        "league_id": league_id,
                        "identifier_duplicate": f"{numeric_id}_{player_id}"
                    }
                    for json_key, db_key in PLAYER_FIELD_MAP.items():
                        player_record[db_key] = _coerce_empty(player.get(json_key))
                    player_records.append(player_record)
                except Exception as e:
                    player_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip() or f"Player {pid}"
                    log.warning("Failed to process player %s: %s", player_name, e)
                    continue

        # Store players under the name we hold for them rather than whatever spelling the
        # scorer typed for this game ("Benedict Baker-Mccann" for "Ben Baker"). The feed's
        # spelling stays matchable through players.aliases; this keeps the name shown on
        # top-performance cards and box scores, and the photo tied to it, consistent.
        preferred_names = _preferred_player_names(rec["player_id"] for rec in player_records)
        for rec in player_records:
            preferred = preferred_names.get(rec["player_id"])
            if preferred and preferred != rec.get("full_name"):
                first, _, rest = preferred.partition(" ")
                rec["full_name"] = preferred
                rec["firstname"] = first
                rec["familyname"] = rest or rec.get("familyname")

        # Two feed entries can resolve to the same player_id (fuzzy name matching in
        # get_or_create_player). A single upsert batch can't contain the same conflict
        # key twice — Postgres rejects the whole batch ("cannot affect row a second
        # time") and the game ends up with no player stats at all — so keep the fuller
        # stat line for each key and warn about the merge.
        def _stat_weight(rec):
            return (rec.get("spoints") or 0) + (rec.get("sfieldgoalsattempted") or 0) + (rec.get("sreboundstotal") or 0) + (rec.get("sassists") or 0)

        deduped_records = {}
        for rec in player_records:
            key = rec["identifier_duplicate"]
            existing = deduped_records.get(key)
            if existing is None:
                deduped_records[key] = rec
                continue
            log.warning(
                "Game %s: feed players %r and %r resolved to the same player_id — keeping the fuller stat line",
                numeric_id, existing.get("full_name"), rec.get("full_name"),
            )
            if _stat_weight(rec) > _stat_weight(existing):
                deduped_records[key] = rec
        player_records = list(deduped_records.values())

        log.info("Prepared %d player records for game %s", len(player_records), numeric_id)
        insert_supabase("player_stats", player_records, conflict_keys="identifier_duplicate")
    except Exception as e:
        log.error("Failed to process player stats for game %s: %s", numeric_id, e, exc_info=True)

    # --- Build and upsert game_rosters ---
    try:
        roster_records = []
        for side, team in teams.items():
            team_id = get_or_create_team(league_id, team.get("name"), user_id, sibling_league_ids)
            for pid, player in team.get("pl", {}).items():
                full_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
                shirt = player.get("shirtNumber")

                # Resolve player_id from already-built roster_map or try lookup
                try:
                    _pno_int = int(pid)
                except (ValueError, TypeError):
                    _pno_int = None
                resolved_pid = roster_map.get((side, _pno_int))

                roster_records.append({
                    "game_key": game_key,
                    "league_id": league_id,
                    "team_id": team_id,
                    "team_no": side,
                    "player_name": preferred_names.get(resolved_pid, full_name),
                    "shirt_number": str(shirt) if shirt is not None else None,
                    "pno": _pno_int,
                    "starter": bool(player.get("starter")),
                    "active": bool(player.get("active", True)),
                    "player_id": resolved_pid,
                })

        if roster_records:
            game_db.table("game_rosters").upsert(
                roster_records,
                on_conflict="game_key,team_id,shirt_number",
            ).execute()
            print(f"✅ Upserted {len(roster_records)} game_rosters rows for {game_key}")
    except Exception as e:
        log.warning("Failed to upsert game_rosters for game %s: %s", numeric_id, e)

    # --- Insert shot chart (reads per-team shots from tm[side]["shot"]) ---
    # Build PBP clock map: actionNumber -> clock string (shots have no clock field)
    pbp_clock_map = {}
    for event in data.get("pbp", []):
        an = event.get("actionNumber")
        cl = event.get("clock")
        if an is not None and cl:
            pbp_clock_map[an] = cl

    shot_records = []
    try:
        for side, team in teams.items():
            team_id = get_or_create_team(league_id, team.get("name"), user_id, sibling_league_ids)
            team_shots = team.get("shot") or []
            log.debug("Side %s: %d shots found", side, len(team_shots))
            for s in team_shots:
                action_number = s.get("actionNumber")
                if action_number is None:
                    continue  # cannot dedupe without action_number
                pno_raw = s.get("pno")
                try:
                    pno = int(pno_raw) if pno_raw is not None else None
                except (ValueError, TypeError):
                    pno = None
                linked_player_id = roster_map.get((side, pno)) if pno is not None else None
                record = {
                    "league_id": league_id,
                    "game_key": game_key,
                    "team_id": team_id,
                    "player_id": linked_player_id,
                    "player_name": s.get("player"),
                    "team_no": s.get("tno"),
                    "period": s.get("per"),
                    "shot_type": s.get("actionType"),
                    "sub_type": s.get("subType"),
                    "success": s.get("r") == 1,
                    "x": s.get("x"),
                    "y": s.get("y"),
                    "action_number": action_number,
                    "clock": pbp_clock_map.get(action_number),
                }
                shot_records.append(record)

        log.info("Prepared %d shot records for game %s", len(shot_records), numeric_id)
        if shot_records:
            insert_supabase("shot_chart", shot_records, conflict_keys="game_key,action_number")
    except Exception as e:
        log.error("Failed to process shot chart for game %s: %s", numeric_id, e, exc_info=True)

    # --- Incremental play-by-play insertion ---
    # Query the latest action_number for this game to only insert new events
    try:
        last_action = 0
        last_action_result = (
            game_db.table("live_events")
            .select("action_number")
            .eq("game_key", game_key)
            .order("action_number", desc=True)
            .limit(1)
            .execute()
        )
        if last_action_result.data and len(last_action_result.data) > 0:
            last_action = last_action_result.data[0].get("action_number") or 0
        
        pbp = data.get("pbp", [])
        total_events_in_json = len(pbp)
        print(f"📊 PBP: last_action={last_action}, total_events_in_json={total_events_in_json}")
        
        # Filter to only new events (actionNumber > last_action)
        pbp_records = []
        for e in pbp:
            action_num = e.get("actionNumber")
            if action_num is None or action_num <= last_action:
                continue
            
            team_id = None
            team_name = None
            tno = e.get("tno")
            if tno and str(tno) in teams:
                team_name = teams[str(tno)].get("name")
                team_id = get_or_create_team(league_id, team_name, user_id, sibling_league_ids)

            player_id = None
            player_name = e.get("player")
            if player_name and team_id:
                player_id = get_or_create_player(player_name, team_id, e.get("shirtNumber"), team_name, league_id, user_id)

            # Build score string from s1 and s2
            s1 = e.get("s1", "")
            s2 = e.get("s2", "")
            score = f"{s1}-{s2}" if s1 and s2 else None

            # team_score / opp_score: s1 is team-1 score, s2 is team-2 score
            try:
                _s1_int = int(s1) if s1 != "" else None
                _s2_int = int(s2) if s2 != "" else None
            except (ValueError, TypeError):
                _s1_int = _s2_int = None
            if tno == 1 or str(tno) == "1":
                _team_score, _opp_score = _s1_int, _s2_int
            elif tno == 2 or str(tno) == "2":
                _team_score, _opp_score = _s2_int, _s1_int
            else:
                _team_score, _opp_score = None, None

            # Keep qualifiers as array
            qualifiers = e.get("qualifier", [])

            # pno: raw numeric player roster key from the API
            _pno = e.get("pno")
            try:
                _pno = int(_pno) if _pno is not None else None
            except (ValueError, TypeError):
                _pno = None

            pbp_record = {
                "league_id": league_id,
                "game_key": game_key,
                "team_id": team_id,
                "player_id": player_id,
                "action_number": action_num,
                "period": _safe_int(e.get("period")),
                "clock": e.get("clock"),
                "player_name": player_name,
                "team_no": _safe_int(tno),
                "action_type": e.get("actionType"),
                "sub_type": e.get("subType"),
                "qualifiers": qualifiers if qualifiers else None,
                "success": _coerce_empty(e.get("success")),
                "scoring": _coerce_empty(e.get("scoring")),
                "points": None,
                "score": score,
                "x_coord": None,
                "y_coord": None,
                "description": None,
                "shirt_number": str(e.get("shirtNumber")) if e.get("shirtNumber") is not None else None,
                "pno": _pno,
                "period_type": _coerce_empty(e.get("periodType")),
                "previous_action": _safe_int(e.get("previousAction")),
                "team_score": _team_score,
                "opp_score": _opp_score,
            }
            pbp_records.append(pbp_record)

        if not pbp_records:
            print(f"⏭️  No new play-by-play events to insert")
        else:
            # Upsert in chunks of 200 to avoid payload/timeout issues. Uses
            # the same insert_supabase upsert helper as shot_chart rather
            # than a plain insert — two overlapping polls (or a retry after
            # a partial failure) can both see the same action_number as
            # "new" via the last_action check above, and a plain insert
            # would hard-fail the whole chunk on the live_events_game_action_unique
            # constraint instead of just no-op'ing the already-stored rows.
            CHUNK_SIZE = 200
            total_new = len(pbp_records)

            for i in range(0, total_new, CHUNK_SIZE):
                chunk = pbp_records[i:i + CHUNK_SIZE]
                insert_supabase("live_events", chunk, conflict_keys="game_key,action_number")
    except Exception as e:
        print(f"⚠️  Error in play-by-play processing: {e}")

    # --- Build lineup stints ---
    # Only run if roster and PBP data are present; log a warning rather than failing.
    try:
        from app.utils.lineup_builder import build_lineups_for_game
        build_lineups_for_game(game_key=game_key, league_id=league_id)
    except Exception as e:
        log.warning("Lineup builder failed for game %s (non-fatal): %s", game_key, e)

    return league_id

# ----------------------------
# Change Detection Helper
# ----------------------------
def has_game_changed(game_key: str, game_date: str, home_team: str, away_team: str, livestats_url: str, pool: str = None) -> bool:
    """
    Check if a game exists in game_schedule and if any key data has changed.
    Also returns True if team_stats are missing for the game, so that games
    uploaded before their LiveStats data was available get reprocessed on the
    next upload once the match has been played.
    Returns True if game is new, changed, or missing team_stats; False if fully up-to-date.
    """
    try:
        result = game_db.table("game_schedule").select(
            'game_key, matchtime, hometeam, awayteam, "LiveStats URL", pool'
        ).eq("game_key", game_key).execute()
        
        # Game doesn't exist - it's new
        if not result.data or len(result.data) == 0:
            return True
        
        existing = result.data[0]
        
        # Compare match time (full timestamp to catch time changes)
        existing_matchtime = existing.get("matchtime", "")
        if existing_matchtime != game_date:
            return True
        
        # Compare team names
        if existing.get("hometeam") != home_team:
            return True
        
        if existing.get("awayteam") != away_team:
            return True
        
        # Compare LiveStats URL
        if existing.get("LiveStats URL") != livestats_url:
            return True
        
        # Compare pool (handle None/null comparison)
        existing_pool = existing.get("pool")
        if (existing_pool or pool) and existing_pool != pool:
            return True
        
        # Schedule is unchanged — but check if team_stats are missing.
        # A game uploaded before it was played has a schedule row but no stats;
        # once the match is played we must reprocess it to populate team_stats.
        try:
            stats_result = game_db.table("team_stats").select("id").eq("game_key", game_key).limit(1).execute()
            if not stats_result.data:
                print(f"   🔄 {game_key}: schedule unchanged but team_stats missing — reprocessing")
                return True
        except Exception as stats_err:
            log.warning("Could not check team_stats for %s: %s", game_key, stats_err)
        
        # Fully up-to-date
        return False
        
    except Exception as e:
        # If we can't check, assume it changed to be safe
        print(f"   ⚠️ Error checking game changes: {e}")
        return True


# ----------------------------
# Excel runner
# ----------------------------
def run_from_excel(path: str, user_id: str = None):
    print("🚀 json_parser starting...")

    # Check if this is a Supabase Storage file path
    if path.startswith("supabase://") or "/" in path and not os.path.exists(path):
        print(f"📂 Fetching Excel from Supabase bucket: {path}")

        # Example: "uploads/my_games.xlsx"
        bucket, filename = path.split("/", 1)
        res = supabase.storage.from_(bucket).download(filename)

        if not res:
            print(f"❌ Could not download {filename} from {bucket}")
            return

        df = pd.read_excel(BytesIO(res))
    else:
        if not os.path.exists(path):
            print(f"❌ Excel file not found: {path}")
            return
        print(f"📂 Found local Excel file: {path}")
        df = pd.read_excel(path)

    print(f"📊 Loaded {len(df)} rows from Excel")

    # Different fixture exports name these columns differently (e.g. "Team 1"/
    # "Team 2"/"Match URL" vs "Home Team"/"Away Team"/"LiveStats URL") — accept
    # either so a file doesn't need renaming before upload.
    column_aliases = {
        "Competition Name": ["Competition Name"],
        "Match Time": ["Match Time"],
        "Home Team": ["Home Team", "Team 1"],
        "Away Team": ["Away Team", "Team 2"],
        "LiveStats URL": ["LiveStats URL", "Match URL"],
    }
    resolved_cols = {}
    for canonical, aliases in column_aliases.items():
        match = next((a for a in aliases if a in df.columns), None)
        if not match:
            raise ValueError(f"❌ Excel file must have a column named '{canonical}' (or one of: {', '.join(aliases)}).")
        resolved_cols[canonical] = match

    # Track processing stats
    skipped_count = 0
    processed_count = 0
    error_count = 0
    league_id_to_return = None

    for idx, row in df.iterrows():
        def safe_str(val):
            if pd.isna(val):
                return ""
            return str(val)
        
        league_name = safe_str(row[resolved_cols["Competition Name"]])

        # Optional: League Name column (e.g. "Basketball England") — display
        # metadata only, never used to decide which competition a row belongs to.
        organisation = None
        if "League Name" in df.columns:
            org_val = safe_str(row["League Name"])
            organisation = org_val if org_val and org_val != "nan" else None

        home_team_name = safe_str(row[resolved_cols["Home Team"]])
        away_team_name = safe_str(row[resolved_cols["Away Team"]])

        # Capture league_id from first row for advanced stats processing
        if league_id_to_return is None and league_name:
            league_id_to_return = get_or_create_league(
                league_name, user_id,
                organisation=organisation,
                team_names=[home_team_name, away_team_name],
            )

        from datetime import datetime

        def normalize_matchtime(value):
            if pd.isna(value) or not value:
                return None

                # Case 1: already a pandas Timestamp
            if isinstance(value, pd.Timestamp):
                return value.strftime("%Y-%m-%dT%H:%M:%S")

                # Case 2: string version
            value_str = str(value).strip()
            for fmt in ("%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y/%m/%d %H:%M", "%d/%m/%Y %H:%M"):
                try:
                    return datetime.strptime(value_str, fmt).strftime("%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    continue

            print(f"⚠️ Could not parse match time: {value_str}")
            return None


            # Replace the old section with this:
        game_date = normalize_matchtime(row[resolved_cols["Match Time"]])

        # Handle Game Key - use existing value or auto-generate if missing/empty
        game_key = safe_str(row.get("Game Key", "")) if "Game Key" in df.columns else ""
        if not game_key or game_key == "nan":
            date_part = game_date.split("T")[0] if game_date else "unknown"
            home_safe = home_team_name.replace(" ", "_")
            away_safe = away_team_name.replace(" ", "_")
            game_key = f"{date_part}_{home_safe}_vs_{away_safe}"
            print(f"   🔑 Auto-generated game_key: {game_key}")
        
        url = safe_str(row[resolved_cols["LiveStats URL"]])
        
        # Optional: Pool column (for leagues with multiple pools like NBL Division 1)
        pool = None
        pool_col = next((c for c in ("Pool", "Pool Number") if c in df.columns), None)
        if pool_col:
            pool_val = safe_str(row[pool_col])
            pool = pool_val if pool_val and pool_val != "nan" else None

        if not url or url == "nan":
            continue

        numeric_id = url.rstrip("/").split("/")[-1]
        
        row_num = int(idx) + 1 if isinstance(idx, (int, float)) else idx
        
        # Check if game has changed before processing
        if not has_game_changed(game_key, game_date, home_team_name, away_team_name, url, pool):
            skipped_count += 1
            print(f"⏭️  Row {row_num}: Skipping {game_key} (no changes)")
            continue
        
        print(f"\n➡️  Row {row_num}: {url}")
        print(f"   🎯 Extracted numeric_id: {numeric_id}")
        
        try:
            parse_and_store_game(
                numeric_id=numeric_id,
                league_name=league_name,
                game_date=game_date,
                home_team_name=home_team_name,
                away_team_name=away_team_name,
                game_key=game_key,
                livestats_url=url,
                user_id=user_id,
                pool=pool,
                league_id=league_id_to_return,
                organisation=organisation,
            )
            processed_count += 1
        except Exception as e:
            error_count += 1
            print(f"❌ Error processing row {row_num}: {e}")
            # Continue with next game instead of failing completely
            continue

    # Print summary
    print(f"\n{'='*60}")
    print(f"✅ Parsing Complete")
    print(f"{'='*60}")
    print(f"   Skipped (unchanged): {skipped_count}")
    print(f"   Processed (new/updated): {processed_count}")
    print(f"   Errors: {error_count}")
    print(f"   Total rows: {len(df)}")
    print(f"{'='*60}")

    # Compute advanced stats for all games in this league
    if league_id_to_return:
        try:
            compute_advanced_stats(league_id_to_return)
        except Exception as e:
            print("Error computing advanced stats:", e)

    return {
        "league_id": league_id_to_return,
        "total_rows": len(df),
        "processed": processed_count,
        "skipped": skipped_count,
        "errors": error_count,
    }

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("❌ Please provide the path to the Excel file.\n")
        print("Usage: python json_parser.py path/to/games.xlsx")
        sys.exit(1)

    excel_path = sys.argv[1]
    run_from_excel(excel_path)
