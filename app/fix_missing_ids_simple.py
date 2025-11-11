#!/usr/bin/env python3
"""
Simple upsert to fill missing player_id and team_id.
Targets only records with player_name NOT NULL but missing IDs.
"""

import time
from app.utils.json_parser import supabase, find_similar_player, normalize_player_name


# Caches
TEAMS_CACHE = {}
PLAYERS_CACHE = {}


def load_teams():
    """Load all teams into cache."""
    print("📥 Loading teams...")
    result = supabase.table("teams").select("team_id, name, league_id").execute()
    for team in result.data:
        league_id = team["league_id"]
        if league_id not in TEAMS_CACHE:
            TEAMS_CACHE[league_id] = {}
        TEAMS_CACHE[league_id][team["name"].lower()] = team["team_id"]
    print(f"   ✅ Cached {len(result.data)} teams\n")


def get_team_id(league_id, team_name):
    """Get team_id from cache."""
    if not team_name or league_id not in TEAMS_CACHE:
        return None
    return TEAMS_CACHE[league_id].get(team_name.lower())


def get_player_id(team_id, player_name):
    """Get player_id with fuzzy matching and caching."""
    if not player_name or not team_id:
        return None
    
    cache_key = f"{team_id}:{normalize_player_name(player_name).lower()}"
    if cache_key in PLAYERS_CACHE:
        return PLAYERS_CACHE[cache_key]
    
    # Try 0.85 threshold first, then 0.75
    player = find_similar_player(player_name, team_id, similarity_threshold=0.85)
    if not player:
        player = find_similar_player(player_name, team_id, similarity_threshold=0.75)
    
    if player:
        player_id = player["id"]
        PLAYERS_CACHE[cache_key] = player_id
        return player_id
    
    return None


def fix_ids():
    """Fix missing IDs in batches."""
    print("🔧 Missing ID Fix")
    print("="*70)
    
    load_teams()
    
    # Get all records with player_name but missing player_id
    print("📊 Fetching records to fix...")
    result = supabase.table("live_events").select(
        "id, game_key, league_id, team_no, player_name, team_id, player_id"
    ).not_.is_("player_name", "null").is_("player_id", "null").execute()
    
    records = result.data
    print(f"   Found {len(records):,} records missing player_id\n")
    print("="*70)
    
    if not records:
        print("✅ Nothing to fix!")
        return
    
    # Get unique game_keys and fetch their team data
    game_teams = {}  # game_key -> {team_no: team_name}
    unique_games = set(r["game_key"] for r in records)
    
    print(f"Fetching team data for {len(unique_games)} games...")
    for game_key in unique_games:
        schedule = supabase.table("game_schedule").select('"LiveStats URL"').eq("game_key", game_key).execute()
        if schedule.data:
            url = schedule.data[0].get("LiveStats URL")
            if url:
                try:
                    import requests
                    numeric_id = url.rstrip("/").split("/")[-1]
                    data_url = f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"
                    resp = requests.get(data_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 200:
                        data = resp.json()
                        teams = data.get("tm", {})
                        game_teams[game_key] = {tno: teams[tno].get("name") for tno in teams}
                except:
                    pass
        time.sleep(0.1)
    
    print(f"✅ Loaded team data for {len(game_teams)} games\n")
    
    # Process in batches
    BATCH_SIZE = 500
    total_fixed = 0
    
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"[Batch {batch_num}/{total_batches}] Processing {len(batch)} records...")
        
        updates = []
        for rec in batch:
            team_id = rec["team_id"]
            
            # Get team_id if missing
            if not team_id and rec["team_no"]:
                game_key = rec["game_key"]
                team_no = str(rec["team_no"])
                if game_key in game_teams and team_no in game_teams[game_key]:
                    team_name = game_teams[game_key][team_no]
                    team_id = get_team_id(rec["league_id"], team_name)
            
            # Get player_id
            player_id = None
            if team_id and rec["player_name"]:
                player_id = get_player_id(team_id, rec["player_name"])
            
            # Build update if we found IDs
            if player_id or (team_id and not rec["team_id"]):
                update = {"id": rec["id"]}
                if player_id:
                    update["player_id"] = player_id
                if team_id and not rec["team_id"]:
                    update["team_id"] = team_id
                updates.append(update)
        
        # Update batch
        if updates:
            try:
                supabase.table("live_events").upsert(updates).execute()
                total_fixed += len(updates)
                print(f"   ✅ Fixed {len(updates)} records")
            except Exception as e:
                print(f"   ❌ Error: {e}")
        else:
            print(f"   ⏭️  No updates")
        
        time.sleep(0.5)
    
    print(f"\n{'='*70}")
    print(f"✅ COMPLETE!")
    print(f"   Total fixed: {total_fixed:,}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    fix_ids()
