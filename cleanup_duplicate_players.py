import os
from difflib import SequenceMatcher
from supabase import create_client, Client
from app.utils.json_parser import normalize_player_name

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def find_duplicate_players(similarity_threshold=0.85):
    result = supabase.table("players").select("id, full_name, team_id, shirtNumber").execute()
    players = result.data
    
    team_players = {}
    for player in players:
        team_id = player["team_id"]
        if team_id not in team_players:
            team_players[team_id] = []
        team_players[team_id].append(player)
    
    duplicates = []
    for team_id, player_list in team_players.items():
        checked = set()
        
        for i, player1 in enumerate(player_list):
            if player1["id"] in checked:
                continue
            
            similar_group = [player1]
            checked.add(player1["id"])
            
            normalized1 = normalize_player_name(player1["full_name"])
            
            for j, player2 in enumerate(player_list):
                if i == j or player2["id"] in checked:
                    continue
                
                normalized2 = normalize_player_name(player2["full_name"])
                similarity = SequenceMatcher(None, normalized1.lower(), normalized2.lower()).ratio()
                
                if similarity >= similarity_threshold:
                    similar_group.append(player2)
                    checked.add(player2["id"])
            
            if len(similar_group) > 1:
                duplicates.append({
                    "team_id": team_id,
                    "players": similar_group
                })
    
    return duplicates

def merge_players(canonical_player_id, duplicate_player_ids, canonical_name):
    print(f"🔄 Merging players into {canonical_player_id}")
    print(f"   Removing duplicates: {duplicate_player_ids}")
    
    tables_to_update = [
        "player_stats",
        "shots",
        "play_by_play"
    ]
    
    for table in tables_to_update:
        for dup_id in duplicate_player_ids:
            try:
                supabase.table(table).update({"player_id": canonical_player_id}).eq("player_id", dup_id).execute()
                print(f"   ✅ Updated {table} records")
            except Exception as e:
                print(f"   ⚠️  Error updating {table}: {e}")
    
    try:
        normalized = normalize_player_name(canonical_name)
        supabase.table("players").update({"full_name": normalized}).eq("id", canonical_player_id).execute()
        print(f"   ✅ Updated canonical player name to '{normalized}'")
    except Exception as e:
        print(f"   ⚠️  Error updating canonical player name: {e}")
    
    for dup_id in duplicate_player_ids:
        try:
            supabase.table("players").delete().eq("id", dup_id).execute()
            print(f"   ✅ Deleted duplicate player {dup_id}")
        except Exception as e:
            print(f"   ⚠️  Error deleting player {dup_id}: {e}")

def run_player_cleanup():
    print("🔍 Finding duplicate players (this may take a moment)...")
    duplicates = find_duplicate_players()
    
    if not duplicates:
        print("✅ No duplicate players found!")
        return
    
    print(f"Found {len(duplicates)} sets of duplicate players:\n")
    
    for dup_set in duplicates:
        print(f"Team ID: {dup_set['team_id']}")
        print("Duplicates:")
        for player in dup_set['players']:
            print(f"  - {player['full_name']} (ID: {player['id']}, Jersey: {player.get('shirtNumber', 'N/A')})")
        
        canonical = dup_set['players'][0]
        duplicates_to_merge = [p['id'] for p in dup_set['players'][1:]]
        
        print(f"\n👉 Will keep: {canonical['full_name']} (ID: {canonical['id']})")
        print(f"👉 Will merge: {[p['full_name'] for p in dup_set['players'][1:]]}\n")
        
        merge_players(canonical['id'], duplicates_to_merge, canonical['full_name'])
        print("✅ Merge complete\n" + "="*50 + "\n")
    
    print("🎉 All player duplicates have been merged!")

if __name__ == "__main__":
    run_player_cleanup()
