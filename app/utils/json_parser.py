import os
import logging
import requests
import pandas as pd
from io import BytesIO
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
from app.utils.compute_advanced_stats import compute_advanced_stats

log = logging.getLogger("json_parser")

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
    "tot_timeLeading": "tot_timeleading",
    "tot_biggestScoringRun": "tot_biggestscoringrun",
    "tot_leadChanges": "tot_leadchanges",
    "tot_timesScoresLevel": "tot_timesscoreslevel",
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

def insert_supabase(table: str, records: list, conflict_keys: str):
    """Insert game data records using game_db (respects DB_SCHEMA)."""
    if not records:
        return
    try:
        game_db.table(table) \
            .upsert(records, on_conflict=conflict_keys) \
            .execute()
        print(f"✅ Upserted {len(records)} into {DB_SCHEMA}.{table}")
    except Exception as e:
        print(f"❌ Supabase upsert failed for {table}: {e}")

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
def get_or_create_league(name: str, user_id: str = None):
    res = ref_db.table("leagues").select("league_id").eq("name", name).execute()
    if res.data:
        return res.data[0]["league_id"]
    insert_data = {"name": name}
    if user_id:
        insert_data["created_by"] = user_id
    new = ref_db.table("leagues").insert(insert_data).execute()
    return new.data[0]["league_id"]

def get_or_create_team(league_id: str, name: str, user_id: str = None):
    normalized_name = normalize_team_name(name)
    res = ref_db.table("teams").select("team_id").eq("league_id", league_id).eq("name", normalized_name).execute()
    if res.data:
        return res.data[0]["team_id"]
    new = ref_db.table("teams").insert({"league_id": league_id, "name": normalized_name}).execute()
    return new.data[0]["team_id"]

def get_or_create_player(full_name: str, team_id: str, shirtnumber=None, team_name=None, league_id=None, user_id: str = None):
    query = ref_db.table("players").select("id, team_name, league_id").eq("full_name", full_name).eq("team_id", team_id)
    if shirtnumber is not None:
        query = query.eq("shirtNumber", shirtnumber)
    res = query.execute()
    
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

def parse_and_store_game(numeric_id: str, league_name: str, game_date=None, home_team_name=None, away_team_name=None, game_key=None, livestats_url=None, user_id: str = None, pool=None):
    print(f"🔍 Processing game {numeric_id}")

    # --- Ensure league ---
    league_id = get_or_create_league(league_name, user_id)

    # --- Ensure teams ---
    if home_team_name:
        home_team_id = get_or_create_team(league_id, home_team_name, user_id)
    else:
        home_team_id = None
    if away_team_name:
        away_team_id = get_or_create_team(league_id, away_team_name, user_id)
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

    # --- Insert team stats ---
    team_records = []
    for side, team in teams.items():
        team_id = get_or_create_team(league_id, team.get("name"), user_id)

        team_record = {
            "numeric_id": numeric_id,
            "side": side,
            "game_key": game_key,
            "team_id": team_id,
            "league_id": league_id,
            "identifier_duplicate": f"{numeric_id}_{team_id}_{side}"
        }
        for json_key, db_key in TEAM_FIELD_MAP.items():
            team_record[db_key] = team.get(json_key)
        team_records.append(team_record)

    insert_supabase("team_stats", team_records, conflict_keys="identifier_duplicate")

    # --- Insert player stats (build roster_map for shot linking) ---
    player_records = []
    roster_map = {}  # (side, pno_int) -> player_id
    try:
        for side, team in teams.items():
            team_id = get_or_create_team(league_id, team.get("name"), user_id)
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
                        player_record[db_key] = player.get(json_key)
                    player_records.append(player_record)
                except Exception as e:
                    player_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip() or f"Player {pid}"
                    log.warning("Failed to process player %s: %s", player_name, e)
                    continue

        log.info("Prepared %d player records for game %s", len(player_records), numeric_id)
        insert_supabase("player_stats", player_records, conflict_keys="identifier_duplicate")
    except Exception as e:
        log.error("Failed to process player stats for game %s: %s", numeric_id, e, exc_info=True)

    # --- Insert shot chart (reads per-team shots from tm[side]["shot"]) ---
    shot_records = []
    try:
        for side, team in teams.items():
            team_id = get_or_create_team(league_id, team.get("name"), user_id)
            team_shots = team.get("shot") or []
            log.debug("Side %s: %d shots found", side, len(team_shots))
            for s in team_shots:
                action_number = s.get("actionNumber")
                if action_number is None:
                    continue  # cannot dedupe without action_number
                pno = s.get("pno")
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
                }
                clock_val = s.get("clock")
                if clock_val is not None:
                    record["clock"] = clock_val
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
                team_id = get_or_create_team(league_id, team_name, user_id)

            player_id = None
            player_name = e.get("player")
            if player_name and team_id:
                player_id = get_or_create_player(player_name, team_id, e.get("shirtNumber"), team_name, league_id, user_id)

            # Build score string from s1 and s2
            s1 = e.get("s1", "")
            s2 = e.get("s2", "")
            score = f"{s1}-{s2}" if s1 and s2 else None

            # Keep qualifiers as array
            qualifiers = e.get("qualifier", [])

            pbp_record = {
                "league_id": league_id,
                "game_key": game_key,
                "team_id": team_id,
                "player_id": player_id,
                "action_number": action_num,
                "period": e.get("period"),
                "clock": e.get("clock"),
                "player_name": player_name,
                "team_no": tno,
                "action_type": e.get("actionType"),
                "sub_type": e.get("subType"),
                "qualifiers": qualifiers if qualifiers else None,
                "success": e.get("success"),
                "scoring": e.get("scoring"),
                "points": None,
                "score": score,
                "x_coord": None,
                "y_coord": None,
                "description": None,
            }
            pbp_records.append(pbp_record)

        if not pbp_records:
            print(f"⏭️  No new play-by-play events to insert")
        else:
            # Insert in chunks of 200 to avoid payload/timeout issues
            CHUNK_SIZE = 200
            total_new = len(pbp_records)
            inserted_count = 0
            
            for i in range(0, total_new, CHUNK_SIZE):
                chunk = pbp_records[i:i + CHUNK_SIZE]
                try:
                    game_db.table("live_events").insert(chunk).execute()
                    inserted_count += len(chunk)
                    if total_new > CHUNK_SIZE:
                        print(f"   📦 Chunk {i // CHUNK_SIZE + 1}: inserted {len(chunk)} events ({inserted_count}/{total_new})")
                except Exception as e:
                    print(f"❌ Error inserting PBP chunk at {i}: {e}")
            
            print(f"✅ Inserted {inserted_count} new play-by-play events into live_events")
    except Exception as e:
        print(f"⚠️  Error in play-by-play processing: {e}")

# ----------------------------
# Change Detection Helper
# ----------------------------
def has_game_changed(game_key: str, game_date: str, home_team: str, away_team: str, livestats_url: str, pool: str = None) -> bool:
    """
    Check if a game exists in game_schedule and if any key data has changed.
    Returns True if game is new or has changed, False if unchanged.
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
        
        # No changes detected
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

    required_cols = ["Competition Name", "Match Time", "Home Team", "Away Team", "LiveStats URL"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"❌ Excel file must have a column named '{col}'.")

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
        
        league_name = safe_str(row["Competition Name"])
        
        # Capture league_id from first row for advanced stats processing
        if league_id_to_return is None and league_name:
            league_id_to_return = get_or_create_league(league_name, user_id)
        
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
        game_date = normalize_matchtime(row["Match Time"])

            
        home_team_name = safe_str(row["Home Team"])
        away_team_name = safe_str(row["Away Team"])
        
        # Handle Game Key - use existing value or auto-generate if missing/empty
        game_key = safe_str(row.get("Game Key", "")) if "Game Key" in df.columns else ""
        if not game_key or game_key == "nan":
            date_part = game_date.split("T")[0] if game_date else "unknown"
            home_safe = home_team_name.replace(" ", "_")
            away_safe = away_team_name.replace(" ", "_")
            game_key = f"{date_part}_{home_safe}_vs_{away_safe}"
            print(f"   🔑 Auto-generated game_key: {game_key}")
        
        url = safe_str(row["LiveStats URL"])
        
        # Optional: Pool column (for leagues with multiple pools like NBL Division 1)
        pool = None
        if "Pool" in df.columns:
            pool_val = safe_str(row["Pool"])
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
                pool=pool
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
    
    return league_id_to_return

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("❌ Please provide the path to the Excel file.\n")
        print("Usage: python json_parser.py path/to/games.xlsx")
        sys.exit(1)

    excel_path = sys.argv[1]
    run_from_excel(excel_path)
