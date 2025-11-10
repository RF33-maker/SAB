#!/usr/bin/env python3
"""
Backfill script to populate play-by-play data for all existing games.
"""

import requests
from app.utils.json_parser import (
    supabase,
    extract_play_by_play,
    get_or_create_team
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
            
            # Debug: show top-level keys in the JSON
            if processed == 0 and skipped == 0:
                print(f"   🔍 JSON structure keys: {list(data.keys())[:20]}")
            
            # Check if there's periods data
            if not data.get("periods"):
                # Try alternative keys that might contain play-by-play
                if not data.get("pbp") and not data.get("Plays") and not data.get("plays"):
                    if processed == 0 and skipped == 0:
                        print(f"   ⚠️  No 'periods', 'pbp', 'Plays', or 'plays' data found")
                    skipped += 1
                    continue
                else:
                    print(f"   ℹ️  Game uses alternative PBP format (not periods/actions)")
                    skipped += 1
                    continue
            
            # Build team map
            team_map = {"A": home_team_id, "B": away_team_id}
            
            # Extract play-by-play
            extract_play_by_play(data, league_id, game_key, team_map)
            
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
