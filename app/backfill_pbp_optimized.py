#!/usr/bin/env python3
"""
Optimized backfill script with team/player ID linking.
Uses in-memory caching and proven fuzzy matching from json_parser.
"""

import requests
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
    
    print(f"   ✅ Cached {len(result.data)} teams")


def get_team_id(league_id, team_name):
    """Get team_id from cache with exact matching."""
    if not team_name or league_id not in TEAMS_CACHE:
        return None
    
    return TEAMS_CACHE[league_id].get(team_name.lower())


def get_player_id_with_fallback(team_id, player_name):
    """
    Get player_id with two-pass fuzzy matching:
    1. Try strict threshold (0.85)
    2. If no match, try relaxed threshold (0.75)
    
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


def backfill_optimized():
    """
    Backfill with two-pass fuzzy matching (0.85 then 0.75 threshold).
    Does NOT create new players - only links to existing ones.
    """
    print("🔄 Starting OPTIMIZED play-by-play backfill...")
    print("="*70)
    
    # Load teams into memory
    load_all_teams()
    
    print("\n📊 Fetching games...")
    result = supabase.table("game_schedule").select(
        'game_key, league_id, "LiveStats URL"'
    ).execute()
    
    games = [g for g in result.data if g.get("LiveStats URL")]
    print(f"   Found {len(games)} games to process\n")
    print("="*70)
    
    processed = 0
    skipped = 0
    errors = 0
    
    for idx, game in enumerate(games, 1):
        game_key = game["game_key"]
        league_id = game["league_id"]
        livestats_url = game["LiveStats URL"]
        
        print(f"\n[{idx}/{len(games)}] 🎯 {game_key}")
        
        try:
            # Extract numeric ID and build URL
            numeric_id = livestats_url.rstrip("/").split("/")[-1]
            data_json_url = f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"
            
            # Fetch JSON
            response = requests.get(data_json_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            
            if response.status_code != 200:
                print(f"   ⏭️  Skipped (HTTP {response.status_code})")
                skipped += 1
                continue
            
            data = response.json()
            
            # Extract PBP
            pbp = data.get("pbp", [])
            if not pbp:
                print(f"   ⏭️  No PBP data")
                skipped += 1
                continue
            
            # Get teams
            teams = data.get("tm", {})
            
            # Process events with cached lookups
            pbp_records = []
            for e in pbp:
                tno = e.get("tno")
                team_id = None
                team_name = None
                
                # Get team_id from cache
                if tno and str(tno) in teams:
                    team_name = teams[str(tno)].get("name")
                    if team_name:
                        team_id = get_team_id(league_id, team_name)
                
                # Get player_id with two-pass fuzzy matching (does NOT create new players)
                player_id = None
                player_name = e.get("player")
                if player_name and team_id:
                    player_id = get_player_id_with_fallback(team_id, player_name)
                
                # Build score
                s1 = e.get("s1", "")
                s2 = e.get("s2", "")
                score = f"{s1}-{s2}" if s1 and s2 else None
                
                # Get qualifiers
                qualifiers = e.get("qualifier", [])
                
                pbp_record = {
                    "league_id": league_id,
                    "game_key": game_key,
                    "team_id": team_id,
                    "player_id": player_id,
                    "action_number": e.get("actionNumber"),
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
                    "x_coord": e.get("x"),
                    "y_coord": e.get("y"),
                    "description": None,
                }
                pbp_records.append(pbp_record)
            
            if pbp_records:
                try:
                    # Delete existing events for this game
                    supabase.table("live_events").delete().eq("game_key", game_key).execute()
                    # Insert new events
                    supabase.table("live_events").insert(pbp_records).execute()
                    print(f"   ✅ Inserted {len(pbp_records)} events")
                    processed += 1
                except Exception as e:
                    print(f"   ❌ Insert error: {e}")
                    errors += 1
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            errors += 1
    
    print(f"\n{'='*70}")
    print(f"✅ BACKFILL COMPLETE!")
    print(f"{'='*70}")
    print(f"   Processed: {processed} games")
    print(f"   Skipped: {skipped} games")
    print(f"   Errors: {errors} games")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    backfill_optimized()
