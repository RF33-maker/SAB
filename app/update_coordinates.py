#!/usr/bin/env python3
"""
Lightweight script to update ONLY x_coord and y_coord columns in live_events.
Does NOT delete any data - just patches the coordinate fields.
"""

import requests
from app.utils.json_parser import supabase


def update_coordinates():
    """
    Fetch JSON for each game and update x_coord/y_coord by matching action_number.
    No deletes, no re-inserts - just updates.
    """
    print("🔄 Starting coordinates-only update...")
    print("=" * 70)
    
    print("\n📊 Fetching games with LiveStats URLs...")
    result = supabase.table("game_schedule").select(
        'game_key, "LiveStats URL"'
    ).execute()
    
    games = [g for g in result.data if g.get("LiveStats URL")]
    print(f"   Found {len(games)} games to process\n")
    print("=" * 70)
    
    updated_games = 0
    updated_events = 0
    skipped = 0
    errors = 0
    
    for idx, game in enumerate(games, 1):
        game_key = game["game_key"]
        livestats_url = game["LiveStats URL"]
        
        print(f"\n[{idx}/{len(games)}] 🎯 {game_key}")
        
        try:
            numeric_id = livestats_url.rstrip("/").split("/")[-1]
            data_json_url = f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"
            
            response = requests.get(data_json_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            
            if response.status_code != 200:
                print(f"   ⏭️  Skipped (HTTP {response.status_code})")
                skipped += 1
                continue
            
            data = response.json()
            pbp = data.get("pbp", [])
            
            if not pbp:
                print(f"   ⏭️  No PBP data")
                skipped += 1
                continue
            
            game_updates = 0
            for e in pbp:
                action_number = e.get("actionNumber")
                x = e.get("x")
                y = e.get("y")
                
                if action_number is None:
                    continue
                
                if x is not None or y is not None:
                    try:
                        supabase.table("live_events").update({
                            "x_coord": x,
                            "y_coord": y
                        }).eq("game_key", game_key).eq("action_number", action_number).execute()
                        game_updates += 1
                    except Exception as e:
                        pass
            
            if game_updates > 0:
                print(f"   ✅ Updated {game_updates} events with coordinates")
                updated_games += 1
                updated_events += game_updates
            else:
                print(f"   ⏭️  No coordinates to update")
                skipped += 1
                
        except Exception as ex:
            print(f"   ❌ Error: {ex}")
            errors += 1
    
    print(f"\n{'=' * 70}")
    print(f"✅ COORDINATE UPDATE COMPLETE!")
    print(f"{'=' * 70}")
    print(f"   Games updated: {updated_games}")
    print(f"   Total events patched: {updated_events}")
    print(f"   Skipped: {skipped}")
    print(f"   Errors: {errors}")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    update_coordinates()
