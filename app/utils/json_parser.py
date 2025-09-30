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
    res = supabase.table("teams").select("team_id").eq("league_id", league_id).eq("name", name).execute()
    if res.data:
        return res.data[0]["team_id"]
    insert_data = {"league_id": league_id, "name": name}
    if user_id:
        insert_data["created_by"] = user_id
    new = supabase.table("teams").insert(insert_data).execute()
    return new.data[0]["team_id"]

def get_or_create_player(full_name: str, team_id: str, jersey_number=None, user_id: str = None):
    query = supabase.table("players").select("id").eq("full_name", full_name).eq("team_id", team_id)
    if jersey_number:
        query = query.eq("jersey_number", jersey_number)
    res = query.execute()
    if res.data:
        return res.data[0]["id"]
    insert_data = {
        "full_name": full_name,
        "team_id": team_id,
        "jersey_number": jersey_number
    }
    if user_id:
        insert_data["created_by"] = user_id
    new = supabase.table("players").insert(insert_data).execute()
    return new.data[0]["id"]

# ----------------------------
# Game Parser
# ----------------------------

def parse_and_store_game(numeric_id: str, league_name: str, game_date=None, home_team_name=None, away_team_name=None, user_id: str = None):
    url = build_data_url(numeric_id)
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            print(f"❌ Failed to fetch {url} (HTTP {r.status_code})")
            return
        data = r.json()
    except Exception as e:
        print(f"❌ Error {url}: {e}")
        return

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

    # --- Insert game schedule row ---
    game_record = {
        "id": numeric_id,
        "league_id": league_id,
        "game_date": game_date,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id
    }
    supabase.table("game_schedule").upsert(game_record, on_conflict="id").execute()

    teams = data.get("tm", {})

    # --- Insert team stats ---
    team_records = []
    for side, team in teams.items():
        team_id = get_or_create_team(league_id, team.get("name"), user_id)

        team_record = {
            "numeric_id": numeric_id,
            "side": side,
            "game_id": numeric_id,
            "team_id": team_id,
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
        for pid, player in team.get("pl", {}).items():
            full_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
            player_id = get_or_create_player(full_name, team_id, player.get("shirtNumber"), user_id)

            player_record = {
                "numeric_id": numeric_id,
                "side": side,
                "game_id": numeric_id,
                "team_id": team_id,
                "player_id": player_id,
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
        team_id = get_or_create_team(league_id, teams.get(s.get("tno"), {}).get("name", "Unknown"), user_id)
        player_id = get_or_create_player(s.get("player"), team_id, s.get("shirtnumber"), user_id)
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
        if e.get("tm"):
            team_id = get_or_create_team(league_id, e.get("tm"), user_id)

        player_id = None
        if e.get("pn") and team_id:
            player_id = get_or_create_player(e.get("pn"), team_id, None, user_id)

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

    required_cols = ["Competition Name", "Match Time", "Home Team", "Away Team", "Game Key", "LiveStats URL"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"❌ Excel file must have a column named '{col}'.")

    for idx, row in df.iterrows():
        def safe_str(val):
            if pd.isna(val):
                return ""
            return str(val)
        
        league_name = safe_str(row["Competition Name"])
        game_date = row["Match Time"]
        home_team_name = safe_str(row["Home Team"])
        away_team_name = safe_str(row["Away Team"])
        game_key = safe_str(row["Game Key"])
        url = safe_str(row["LiveStats URL"])

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
            user_id=user_id
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
