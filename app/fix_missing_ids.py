#!/usr/bin/env python3
"""
Upsert script to fill missing player_id and team_id in live_events.
Only targets records with player_name NOT NULL but missing IDs.
"""

import time
from app.utils.json_parser import supabase, find_similar_player, normalize_player_name


# Global caches
TEAMS_CACHE = {}  # league_id -> {name: team_id}
PLAYERS_CACHE = {}  # "team_id:normalized_name" -> player_id


def load_all_teams():
    """Load all teams into memory cache."""
    print("📥 Loading teams into cache...")
    result = supabase.table("teams").select("team_id, name, league_id").execute()
    
    for team in result.data:
        league_id = team["league_id"]
        if league_id not in TEAMS_CACHE:
            TEAMS_CACHE[league_id] = {}
        TEAMS_CACHE[league_id][team["name"].lower()] = team["team_id"]
    
    print(f"   ✅ Cached {len(result.data)} teams\n")


def get_team_id(league_id, team_name):
    """Get team_id from cache with exact matching."""
    if not team_name or league_id not in TEAMS_CACHE:
        return None
    
    return TEAMS_CACHE[league_id].get(team_name.lower())


def get_player_id_with_fallback(team_id, player_name):
    """
    Get player_id with two-pass fuzzy matching.
    Does NOT create new players - only links to existing ones.
    """
    if not player_name or not team_id:
        return None
    
    # Check cache first
    cache_key = f"{team_id}:{normalize_player_name(player_name).lower()}"
    if cache_key in PLAYERS_CACHE:
        return PLAYERS_CACHE[cache_key]
    
    # Pass 1: Try strict threshold (0.85)
    player = find_similar_player(player_name, team_id, similarity_threshold=0.85)
    
    # Pass 2: If no match, try relaxed threshold (0.75)
    if not player:
        player = find_similar_player(player_name, team_id, similarity_threshold=0.75)
    
    if player:
        player_id = player["id"]
        # Cache the result
        PLAYERS_CACHE[cache_key] = player_id
        return player_id
    
    return None


def get_team_name_from_json(game_key, team_no):
    """Get team name from game's JSON data."""
    try:
        # Get LiveStats URL from game_schedule
        schedule = supabase.table("game_schedule").select('"LiveStats URL"').eq("game_key", game_key).execute()
        if not schedule.data:
            return None
        
        livestats_url = schedule.data[0].get("LiveStats URL")
        if not livestats_url:
            return None
        
        # Fetch JSON
        import requests
        numeric_id = livestats_url.rstrip("/").split("/")[-1]
        data_url = f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"
        
        resp = requests.get(data_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        
        data = resp.json()
        teams = data.get("tm", {})
        
        if str(team_no) in teams:
            return teams[str(team_no)].get("name")
        
    except Exception as e:
        print(f"   ⚠️  Could not fetch team name: {e}")
    
    return None


def retry_with_backoff(func, max_retries=3, initial_delay=1):
    """Retry function with exponential backoff on connection errors."""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_msg = str(e).lower()
            if "connectionterminated" in error_msg or "connection" in error_msg:
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    print(f"   ⚠️  Connection error, retrying in {delay}s... (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    print(f"   ❌ Connection failed after {max_retries} attempts")
                    raise
            else:
                raise
    return None


def fix_missing_ids():
    """
    Fix missing player_id and team_id in live_events.
    Only processes records with player_name NOT NULL.
    """
    print("🔧 Starting Missing ID Fix...")
    print("="*70)
    
    # Load teams
    load_all_teams()
    
    # Get count of records to fix
    print("📊 Analyzing gaps...")
    count_result = supabase.table("live_events").select("id", count="exact").not_.is_("player_name", "null").filter("player_id", "is", "null").execute()  # type: ignore
    total_missing = count_result.count if count_result else 0
    
    print(f"   Records with player_name but missing player_id: {total_missing:,}")
    print()
    
    if total_missing == 0:
        print("✅ No gaps to fix!")
        return
    
    print("="*70)
    
    # Process in batches
    BATCH_SIZE = 1000
    offset = 0
    batch_num = 0
    total_player_ids_added = 0
    total_team_ids_added = 0
    total_processed = 0
    
    while True:
        batch_num += 1
        
        # Fetch batch of records missing IDs
        def fetch_batch():
            return supabase.table("live_events").select(
                "id, game_key, league_id, team_no, player_name, player_id, team_id"
            ).not_.is_("player_name", "null").or_(
                "player_id.is.null,team_id.is.null"
            ).range(offset, offset + BATCH_SIZE - 1).execute()
        
        result = retry_with_backoff(fetch_batch)
        
        if not result or not result.data:
            break
        
        print(f"\n[Batch {batch_num}] Processing {len(result.data)} records...")
        
        # Build updates
        updates = []
        player_ids_added = 0
        team_ids_added = 0
        
        for record in result.data:
            record_id = record["id"]
            game_key = record["game_key"]
            league_id = record["league_id"]
            team_no = record["team_no"]
            player_name = record["player_name"]
            current_player_id = record["player_id"]
            current_team_id = record["team_id"]
            
            needs_update = False
            update_data = {"id": record_id}
            
            # Fix missing team_id
            if not current_team_id and team_no:
                team_name = get_team_name_from_json(game_key, team_no)
                if team_name:
                    team_id = get_team_id(league_id, team_name)
                    if team_id:
                        update_data["team_id"] = team_id
                        team_ids_added += 1
                        needs_update = True
            else:
                update_data["team_id"] = current_team_id
            
            # Fix missing player_id
            if not current_player_id and player_name:
                # Use team_id from record or newly found one
                team_id = update_data.get("team_id") or current_team_id
                if team_id:
                    player_id = get_player_id_with_fallback(team_id, player_name)
                    if player_id:
                        update_data["player_id"] = player_id
                        player_ids_added += 1
                        needs_update = True
            
            if needs_update:
                updates.append(update_data)
        
        # Batch update
        if updates:
            def do_update():
                return supabase.table("live_events").upsert(updates).execute()
            
            retry_with_backoff(do_update)
            total_player_ids_added += player_ids_added
            total_team_ids_added += team_ids_added
            total_processed += len(updates)
            
            print(f"   ✅ Fixed {len(updates)} records ({player_ids_added} player_ids, {team_ids_added} team_ids)")
        else:
            print(f"   ⏭️  No updates needed for this batch")
        
        # Move to next batch
        offset += BATCH_SIZE
        
        # Small delay to avoid overwhelming Supabase
        time.sleep(0.5)
        
        # Stop if we got fewer records than batch size
        if len(result.data) < BATCH_SIZE:
            break
    
    print(f"\n{'='*70}")
    print(f"✅ FIX COMPLETE!")
    print(f"{'='*70}")
    print(f"   Total records processed: {total_processed:,}")
    print(f"   Player IDs added: {total_player_ids_added:,}")
    print(f"   Team IDs added: {total_team_ids_added:,}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    fix_missing_ids()
