#!/usr/bin/env python3
"""
Optimized backfill script with team/player ID linking.
Uses in-memory caching to speed up fuzzy matching.
"""

import requests
from difflib import SequenceMatcher
from app.utils.json_parser import supabase


# Global caches
TEAMS_CACHE = {}  # league_id -> {name: team_id}
PLAYERS_CACHE = {}  # team_id -> {name: player_id}


def load_all_teams():
    """Load all teams into memory cache."""
    print("📥 Loading teams into cache...")
    result = supabase.table("teams").select("id, name, league_id").execute()
    
    for team in result.data:
        league_id = team["league_id"]
        if league_id not in TEAMS_CACHE:
            TEAMS_CACHE[league_id] = {}
        TEAMS_CACHE[league_id][team["name"].lower()] = team["id"]
    
    print(f"   ✅ Cached {len(result.data)} teams")


def load_all_players():
    """Load all players into memory cache."""
    print("📥 Loading players into cache...")
    result = supabase.table("players").select("id, full_name, team_id").execute()
    
    for player in result.data:
        team_id = player["team_id"]
        if team_id and player["full_name"]:
            if team_id not in PLAYERS_CACHE:
                PLAYERS_CACHE[team_id] = {}
            PLAYERS_CACHE[team_id][player["full_name"].lower()] = player["id"]
    
    print(f"   ✅ Cached {len(result.data)} players")


def fuzzy_match(name, candidates, threshold=0.85):
    """Find best fuzzy match from candidates."""
    if not name or not candidates:
        return None
    
    name_lower = name.lower()
    
    # Exact match first
    if name_lower in candidates:
        return candidates[name_lower]
    
    # Fuzzy match
    best_score = 0
    best_match = None
    
    for candidate_name, candidate_id in candidates.items():
        score = SequenceMatcher(None, name_lower, candidate_name).ratio()
        if score > best_score and score >= threshold:
            best_score = score
            best_match = candidate_id
    
    return best_match


def get_team_id(league_id, team_name):
    """Get team_id from cache with fuzzy matching."""
    if not team_name or league_id not in TEAMS_CACHE:
        return None
    
    return fuzzy_match(team_name, TEAMS_CACHE[league_id])


def get_player_id(team_id, player_name):
    """Get player_id from cache with fuzzy matching."""
    if not player_name or not team_id or team_id not in PLAYERS_CACHE:
        return None
    
    return fuzzy_match(player_name, PLAYERS_CACHE[team_id])


def backfill_optimized():
    """
    Fast backfill with team/player ID linking using cached lookups.
    """
    print("🔄 Starting OPTIMIZED play-by-play backfill...")
    print("="*70)
    
    # Load all data into memory
    load_all_teams()
    load_all_players()
    
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
                
                # Get team_id from cache
                if tno and str(tno) in teams:
                    team_name = teams[str(tno)].get("name")
                    if team_name:
                        team_id = get_team_id(league_id, team_name)
                
                # Get player_id from cache
                player_id = None
                player_name = e.get("player")
                if player_name and team_id:
                    player_id = get_player_id(team_id, player_name)
                
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
                    "x_coord": None,
                    "y_coord": None,
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
