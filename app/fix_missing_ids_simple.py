#!/usr/bin/env python3
"""
Simple upsert to fill missing player_id and team_id.
Targets only records with player_name NOT NULL but missing IDs.
"""

import time
from app.utils.json_parser import supabase, find_similar_player, normalize_player_name, normalize_team_name


# Caches
TEAMS_CACHE = {}
PLAYERS_CACHE = {}
ALL_PLAYERS_BY_TEAM = {}  # team_id -> list of player dicts


def load_teams():
    """Load all teams into cache with normalization."""
    print("📥 Loading teams...")
    result = supabase.table("teams").select("team_id, name, league_id").execute()
    for team in result.data:
        league_id = team["league_id"]
        if league_id not in TEAMS_CACHE:
            TEAMS_CACHE[league_id] = {}
        # Normalize team name when storing to match normalized lookups
        normalized_name = normalize_team_name(team["name"]).lower()
        TEAMS_CACHE[league_id][normalized_name] = team["team_id"]
    print(f"   ✅ Cached {len(result.data)} teams\n")


def load_all_players():
    """Load ALL players into memory for fast fuzzy matching."""
    print("📥 Loading all players...")
    offset = 0
    batch_size = 1000
    total_players = 0
    
    while True:
        result = supabase.table("players").select("id, full_name, team_id").range(offset, offset + batch_size - 1).execute()
        if not result.data:
            break
        
        for player in result.data:
            team_id = player["team_id"]
            if team_id not in ALL_PLAYERS_BY_TEAM:
                ALL_PLAYERS_BY_TEAM[team_id] = []
            ALL_PLAYERS_BY_TEAM[team_id].append(player)
        
        total_players += len(result.data)
        
        if len(result.data) < batch_size:
            break
        
        offset += batch_size
    
    print(f"   ✅ Loaded {total_players:,} players across {len(ALL_PLAYERS_BY_TEAM)} teams\n")


def get_team_id(league_id, team_name):
    """Get team_id from cache with normalization."""
    if not team_name or league_id not in TEAMS_CACHE:
        return None
    normalized = normalize_team_name(team_name).lower()
    return TEAMS_CACHE[league_id].get(normalized)


def get_player_id(team_id, player_name):
    """Get player_id with in-memory fuzzy matching."""
    if not player_name or not team_id:
        return None
    
    cache_key = f"{team_id}:{normalize_player_name(player_name).lower()}"
    if cache_key in PLAYERS_CACHE:
        return PLAYERS_CACHE[cache_key]
    
    # Get players for this team from memory
    team_players = ALL_PLAYERS_BY_TEAM.get(team_id, [])
    if not team_players:
        return None
    
    # Do fuzzy matching in-memory
    from difflib import SequenceMatcher
    normalized_search = normalize_player_name(player_name).lower()
    
    best_match = None
    best_score = 0.0
    
    for player in team_players:
        existing_name = normalize_player_name(player["full_name"]).lower()
        similarity = SequenceMatcher(None, normalized_search, existing_name).ratio()
        
        if similarity > best_score:
            best_score = similarity
            best_match = player
    
    # Use threshold of 0.75
    if best_match and best_score >= 0.75:
        player_id = best_match["id"]
        PLAYERS_CACHE[cache_key] = player_id
        return player_id
    
    return None


def fix_ids():
    """Fix missing IDs in batches."""
    print("🔧 Missing ID Fix")
    print("="*70)
    
    load_teams()
    load_all_players()
    
    # Get ALL records with player_name but missing player_id (in batches)
    print("📊 Fetching records to fix...")
    all_records = []
    batch_size = 1000
    offset = 0
    
    while True:
        result = supabase.table("live_events").select(
            "id, game_key, league_id, team_no, player_name, team_id, player_id"
        ).not_.is_("player_name", "null").is_("player_id", "null").range(offset, offset + batch_size - 1).execute()
        
        if not result.data:
            break
        
        all_records.extend(result.data)
        print(f"   Fetched {len(all_records):,} records so far...")
        
        if len(result.data) < batch_size:
            break
        
        offset += batch_size
        time.sleep(0.2)  # Small delay to avoid rate limits
    
    records = all_records
    print(f"   Found {len(records):,} total records missing player_id\n")
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
        debug_has_team_id = 0
        debug_missing_team_id = 0
        debug_team_found = 0
        debug_player_found = 0
        debug_player_not_found = 0
        debug_sample_shown = False
        
        for rec in batch:
            team_id = rec["team_id"]
            
            # Get team_id if missing
            if not team_id and rec["team_no"]:
                debug_missing_team_id += 1
                game_key = rec["game_key"]
                team_no = str(rec["team_no"])
                if game_key in game_teams and team_no in game_teams[game_key]:
                    team_name = game_teams[game_key][team_no]
                    team_id = get_team_id(rec["league_id"], team_name)
                    if team_id:
                        debug_team_found += 1
                    elif not debug_sample_shown:
                        print(f"   Sample: game={game_key}, team_no={team_no}, team_name={team_name}, league={rec['league_id']}, lookup_result=None")
                        debug_sample_shown = True
            else:
                debug_has_team_id += 1
            
            # Get player_id
            player_id = None
            if team_id and rec["player_name"]:
                player_id = get_player_id(team_id, rec["player_name"])
                if player_id:
                    debug_player_found += 1
                else:
                    debug_player_not_found += 1
            
            # Build update if we found IDs
            if player_id or (team_id and not rec["team_id"]):
                update = {"id": rec["id"]}
                if player_id:
                    update["player_id"] = player_id
                if team_id and not rec["team_id"]:
                    update["team_id"] = team_id
                updates.append(update)
        
        print(f"   Debug: has_team={debug_has_team_id}, missing_team={debug_missing_team_id}, team_found={debug_team_found}, player_found={debug_player_found}, player_not_found={debug_player_not_found}")
        
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
