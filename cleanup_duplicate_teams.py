import os
from supabase import create_client, Client
from app.utils.json_parser import normalize_team_name

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def find_duplicate_teams():
    result = supabase.table("teams").select("team_id, league_id, name").execute()
    teams = result.data
    
    league_teams = {}
    for team in teams:
        league_id = team["league_id"]
        normalized = normalize_team_name(team["name"])
        
        if league_id not in league_teams:
            league_teams[league_id] = {}
        
        if normalized not in league_teams[league_id]:
            league_teams[league_id][normalized] = []
        
        league_teams[league_id][normalized].append(team)
    
    duplicates = []
    for league_id, teams_dict in league_teams.items():
        for normalized_name, team_list in teams_dict.items():
            if len(team_list) > 1:
                duplicates.append({
                    "league_id": league_id,
                    "normalized_name": normalized_name,
                    "teams": team_list
                })
    
    return duplicates

def merge_teams(canonical_team_id, duplicate_team_ids, normalized_name):
    print(f"🔄 Merging teams into {canonical_team_id}")
    print(f"   Removing duplicates: {duplicate_team_ids}")
    
    tables_to_update = [
        "players",
        "player_stats",
        "team_stats",
        "shots",
        "play_by_play"
    ]
    
    for table in tables_to_update:
        for dup_id in duplicate_team_ids:
            try:
                supabase.table(table).update({"team_id": canonical_team_id}).eq("team_id", dup_id).execute()
                print(f"   ✅ Updated {table} records")
            except Exception as e:
                print(f"   ⚠️  Error updating {table}: {e}")
    
    for dup_id in duplicate_team_ids:
        try:
            supabase.table("game_schedule").update({"home_team_id": canonical_team_id}).eq("home_team_id", dup_id).execute()
            supabase.table("game_schedule").update({"away_team_id": canonical_team_id}).eq("away_team_id", dup_id).execute()
            print(f"   ✅ Updated game_schedule for team {dup_id}")
        except Exception as e:
            print(f"   ⚠️  Error updating game_schedule: {e}")
    
    try:
        supabase.table("teams").update({"name": normalized_name}).eq("team_id", canonical_team_id).execute()
        print(f"   ✅ Updated canonical team name to '{normalized_name}'")
    except Exception as e:
        print(f"   ⚠️  Error updating canonical team name: {e}")
    
    for dup_id in duplicate_team_ids:
        try:
            supabase.table("teams").delete().eq("team_id", dup_id).execute()
            print(f"   ✅ Deleted duplicate team {dup_id}")
        except Exception as e:
            print(f"   ⚠️  Error deleting team {dup_id}: {e}")

def run_team_cleanup():
    print("🔍 Finding duplicate teams...")
    duplicates = find_duplicate_teams()
    
    if not duplicates:
        print("✅ No duplicate teams found!")
        return
    
    print(f"Found {len(duplicates)} sets of duplicate teams:\n")
    
    for dup_set in duplicates:
        print(f"League: {dup_set['league_id']}")
        print(f"Normalized name: {dup_set['normalized_name']}")
        print("Duplicates:")
        for team in dup_set['teams']:
            print(f"  - {team['name']} (ID: {team['team_id']})")
        
        canonical = dup_set['teams'][0]
        duplicates_to_merge = [t['team_id'] for t in dup_set['teams'][1:]]
        
        print(f"\n👉 Will keep: {canonical['name']} → '{dup_set['normalized_name']}' (ID: {canonical['team_id']})")
        print(f"👉 Will merge: {[t['name'] for t in dup_set['teams'][1:]]}\n")
        
        merge_teams(canonical['team_id'], duplicates_to_merge, dup_set['normalized_name'])
        print("✅ Merge complete\n" + "="*50 + "\n")
    
    print("🎉 All team duplicates have been merged!")

if __name__ == "__main__":
    run_team_cleanup()
