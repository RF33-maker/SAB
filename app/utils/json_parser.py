import os
import requests
import pandas as pd
from io import BytesIO
from supabase import create_client, Client

# ✅ Env variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in environment variables")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    if not records:
        return
    try:
        supabase.table(table) \
            .upsert(records, on_conflict=conflict_keys) \
            .execute()
        print(f"✅ Upserted {len(records)} into {table}")
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
    
    result = supabase.table("players").select("id, full_name, shirtNumber").eq("team_id", team_id).execute()
    
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
    
    if best_match:
        if match_type == "initial":
            print(f"🔍 Initial match found: '{full_name}' → '{best_match['full_name']}'")
        else:
            print(f"🔍 Fuzzy match found: '{full_name}' → '{best_match['full_name']}' (score: {best_score:.2f})")
    
    return best_match

# ----------------------------
# Entity Get-or-Create
# ----------------------------
def get_or_create_league(name: str, user_id: str = None):
    res = supabase.table("leagues").select("league_id").eq("name", name).execute()
    if res.data:
        return res.data[0]["league_id"]
    insert_data = {"name": name}
    if user_id:
        insert_data["created_by"] = user_id
    new = supabase.table("leagues").insert(insert_data).execute()
    return new.data[0]["league_id"]

def get_or_create_team(league_id: str, name: str, user_id: str = None):
    normalized_name = normalize_team_name(name)
    res = supabase.table("teams").select("team_id").eq("league_id", league_id).eq("name", normalized_name).execute()
    if res.data:
        return res.data[0]["team_id"]
    new = supabase.table("teams").insert({"league_id": league_id, "name": normalized_name}).execute()
    return new.data[0]["team_id"]

def get_or_create_player(full_name: str, team_id: str, shirtnumber=None, team_name=None, league_id=None, user_id: str = None):
    query = supabase.table("players").select("id, team_name, league_id").eq("full_name", full_name).eq("team_id", team_id)
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
            supabase.table("players").update(update_data).eq("id", player_id).execute()
            print(f"✅ Updated player {full_name} with missing fields: {list(update_data.keys())}")
        
        return player_id
    
    similar_player = find_similar_player(full_name, team_id)
    if similar_player:
        player_id = similar_player["id"]
        
        update_data = {}
        existing_result = supabase.table("players").select("team_name, league_id").eq("id", player_id).execute()
        if existing_result.data:
            existing_team_name = existing_result.data[0].get("team_name")
            existing_league_id = existing_result.data[0].get("league_id")
            
            if not existing_team_name and team_name:
                update_data["team_name"] = team_name
            if not existing_league_id and league_id:
                update_data["league_id"] = league_id
            
            if update_data:
                supabase.table("players").update(update_data).eq("id", player_id).execute()
        
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
    
    new = supabase.table("players").insert(insert_data).execute()
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
    supabase.table("game_schedule").upsert(game_record, on_conflict="game_key").execute()
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

    # --- Insert player stats ---
    player_records = []
    for side, team in teams.items():
        team_id = get_or_create_team(league_id, team.get("name"), user_id)
        team_name = team.get("name")
        for pid, player in team.get("pl", {}).items():
            full_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
            player_id = get_or_create_player(full_name, team_id, player.get("shirtNumber"), team_name, league_id, user_id)

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

    insert_supabase("player_stats", player_records, conflict_keys="identifier_duplicate")

    # --- Insert shots ---
    shots = data.get("shot", [])
    shot_records = []
    for s in shots:
        team_name = teams.get(s.get("tno"), {}).get("name", "Unknown")
        team_id = get_or_create_team(league_id, team_name, user_id)
        player_id = get_or_create_player(s.get("player"), team_id, s.get("shirtNumber"), team_name, league_id, user_id)
        shot_record = {
            "numeric_id": numeric_id,
            "game_id": numeric_id,
            "team_id": team_id,
            "player_id": player_id,
            "identifier_duplicate": f"{numeric_id}_{s.get('actionnumber')}_{player_id}"
        }
        for json_key, db_key in SHOT_FIELD_MAP.items():
            shot_record[db_key] = s.get(json_key)
        shot_records.append(shot_record)

    insert_supabase("shots", shot_records, conflict_keys="identifier_duplicate")

    # --- Insert play-by-play ---
    pbp = data.get("pbp", [])
    pbp_records = []
    for e in pbp:
        team_id = None
        team_name = None
        if e.get("tm"):
            team_name = e.get("tm")
            team_id = get_or_create_team(league_id, team_name, user_id)

        player_id = None
        if e.get("pn") and team_id:
            player_id = get_or_create_player(e.get("pn"), team_id, None, team_name, league_id, user_id)

        pbp_record = {
            "numeric_id": numeric_id,
            "game_id": numeric_id,
            "team_id": team_id,
            "player_id": player_id,
            "identifier_duplicate": f"{numeric_id}_{e.get('evt')}_{e.get('per')}"
        }
        for json_key, db_key in PBP_FIELD_MAP.items():
            pbp_record[db_key] = e.get(json_key)
        pbp_records.append(pbp_record)

    # Deduplicate before insert
    unique_pbp = {rec["identifier_duplicate"]: rec for rec in pbp_records}
    insert_supabase("play_by_play", list(unique_pbp.values()), conflict_keys="identifier_duplicate")

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

    for idx, row in df.iterrows():
        def safe_str(val):
            if pd.isna(val):
                return ""
            return str(val)
        
        league_name = safe_str(row["Competition Name"])
        
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
        print(f"\n➡️  Row {row_num}: {url}")
        print(f"   🎯 Extracted numeric_id: {numeric_id}")

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

    print("✅ Finished parsing")

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("❌ Please provide the path to the Excel file.\n")
        print("Usage: python json_parser.py path/to/games.xlsx")
        sys.exit(1)

    excel_path = sys.argv[1]
    run_from_excel(excel_path)
