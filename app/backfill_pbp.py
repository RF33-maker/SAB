#!/usr/bin/env python3
"""
Backfill script to populate play-by-play data for all existing games.
"""

import requests
from app.utils.json_parser import (
    supabase,
    get_or_create_team,
    get_or_create_player
)


def backfill_all_games():
    """
    Fetch all games from game_schedule and extract play-by-play data.
    This will populate the live_events table with historical play-by-play data.
    """
    print("🔄 Starting play-by-play backfill...")
    
    # Fetch all games with LiveStats URLs
    result = supabase.table("game_schedule").select(
        'game_key, league_id, home_team_id, away_team_id, "LiveStats URL", hometeam, awayteam'
    ).execute()
    
    print(f"📥 Total games in schedule: {len(result.data)}")
    
    # Debug: show a sample game
    if result.data:
        print(f"   Sample game: {result.data[0]}")
    
    games = [
        g for g in result.data
        if g.get("LiveStats URL")
    ]
    
    print(f"📊 Found {len(games)} games with LiveStats URLs")
    
    processed = 0
    skipped = 0
    errors = 0
    
    for game in games:
        game_key = game["game_key"]
        league_id = game["league_id"]
        livestats_url = game["LiveStats URL"]
        home_team_id = game.get("home_team_id")
        away_team_id = game.get("away_team_id")
        
        print(f"\n🎯 Processing {game_key}...")
        
        try:
            # Extract numeric ID from URL and build data.json URL
            # URL format: https://fibalivestats.dcd.shared.geniussports.com/u/HBBC/2716679/
            numeric_id = livestats_url.rstrip("/").split("/")[-1]
            data_json_url = f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"
            
            # Fetch the JSON data
            response = requests.get(data_json_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            
            if response.status_code != 200:
                print(f"   ⏭️  Skipped (HTTP {response.status_code})")
                skipped += 1
                continue
            
            data = response.json()
            
            # Extract play-by-play from pbp array
            pbp = data.get("pbp", [])
            if not pbp:
                skipped += 1
                continue
            
            # Get teams data for mapping tno to team names
            teams = data.get("tm", {})
            
            # Process each play-by-play event
            pbp_records = []
            for e in pbp:
                team_id = None
                team_name = None
                tno = e.get("tno")
                if tno and str(tno) in teams:
                    team_name = teams[str(tno)].get("name")
                    team_id = get_or_create_team(league_id, team_name)

                player_id = None
                player_name = e.get("player")
                if player_name and team_id:
                    player_id = get_or_create_player(player_name, team_id, e.get("shirtNumber"), team_name, league_id)

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
                    # Delete existing events for this game to ensure idempotency
                    supabase.table("live_events").delete().eq("game_key", game_key).execute()
                    # Insert new events
                    supabase.table("live_events").insert(pbp_records).execute()
                    print(f"   ✅ Inserted {len(pbp_records)} play-by-play events")
                except Exception as e:
                    print(f"   ❌ Error: {e}")
            
            processed += 1
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            errors += 1
            import traceback
            traceback.print_exc()
    
    print(f"\n" + "="*60)
    print(f"✅ Backfill complete!")
    print(f"   Processed: {processed}")
    print(f"   Skipped: {skipped}")
    print(f"   Errors: {errors}")
    print("="*60)


if __name__ == "__main__":
    backfill_all_games()
